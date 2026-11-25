//! The read-only connections, and which read gets the next free one (issue #109).
//!
//! Reads come from two kinds of caller: the instance itself (ingest, enrichment, the progress
//! snapshot) and the API's calls ([`api_call`], [`api_call_blocking`]). A flood of API reads
//! must not leave ingest's reads waiting behind it, so when both wait, the instance's read
//! gets the reader. The API is not starved either: while it holds no reader, it may take one
//! even if instance reads wait (one at a time), and instance reads leave it the last free one.

use std::cell::Cell;
use std::future::Future;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use rusqlite::Connection;

tokio::task_local! {
    /// Set for the whole of an API call's future ([`api_call`]).
    static API_TASK: ();
}

thread_local! {
    /// Set while a blocking thread runs an API call's store work ([`api_call_blocking`]).
    static API_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// Runs an API call: the store reads it makes, on this task, are the API's and give way to
/// the instance's own reads when both wait for a read-only connection (issue #109).
pub async fn api_call<F: Future>(call: F) -> F::Output {
    API_TASK.scope((), call).await
}

/// [`api_call`] for store work on a blocking thread.
pub fn api_call_blocking<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            API_THREAD.set(self.0);
        }
    }
    let _restore = Restore(API_THREAD.replace(true));
    f()
}

/// Who a read is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Caller {
    /// The instance's own work: ingest, enrichment, the progress snapshot.
    Instance,
    /// An API call.
    Api,
}

impl Caller {
    /// The caller of the current task or thread.
    pub(crate) fn current() -> Self {
        if API_THREAD.get() || API_TASK.try_with(|()| ()).is_ok() {
            Self::Api
        } else {
            Self::Instance
        }
    }

    /// Runs `f` on a blocking thread as this caller.
    pub(crate) fn run<T>(self, f: impl FnOnce() -> T) -> T {
        match self {
            Self::Instance => f(),
            Self::Api => api_call_blocking(f),
        }
    }
}

/// The read-only connections: idle ones wait in the pool, a read takes one and gives it back.
#[derive(Default)]
pub(crate) struct Readers {
    pool: Mutex<Pool>,
    /// Instance reads waiting for a connection.
    instance: Condvar,
    /// API reads waiting for a connection.
    api: Condvar,
    count: usize,
}

#[derive(Default)]
struct Pool {
    idle: Vec<Connection>,
    instance_waiting: usize,
    api_waiting: usize,
    api_held: usize,
}

impl Pool {
    /// An instance read takes any free connection, except the last one while the API holds
    /// none and waits for one.
    fn instance_may_take(&self) -> bool {
        let starved_api = self.api_held == 0 && self.api_waiting > 0;
        self.idle.len() > usize::from(starved_api)
    }

    /// An API read takes a free connection no instance read is waiting for; or any while it
    /// holds none.
    fn api_may_take(&self) -> bool {
        !self.idle.is_empty() && (self.api_held == 0 || self.idle.len() > self.instance_waiting)
    }
}

fn lock(m: &Mutex<Pool>) -> MutexGuard<'_, Pool> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn wait<'a>(cv: &Condvar, pool: MutexGuard<'a, Pool>) -> MutexGuard<'a, Pool> {
    cv.wait(pool).unwrap_or_else(PoisonError::into_inner)
}

impl Readers {
    pub(crate) fn new(idle: Vec<Connection>) -> Self {
        let count = idle.len();
        Self {
            pool: Mutex::new(Pool {
                idle,
                ..Pool::default()
            }),
            instance: Condvar::new(),
            api: Condvar::new(),
            count,
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    /// A free connection for `caller`, waiting for one when none is free to it.
    pub(crate) fn take(&self, caller: Caller) -> Connection {
        let mut pool = lock(&self.pool);
        match caller {
            Caller::Instance => {
                while !pool.instance_may_take() {
                    pool.instance_waiting += 1;
                    pool = wait(&self.instance, pool);
                    pool.instance_waiting -= 1;
                }
            }
            Caller::Api => {
                while !pool.api_may_take() {
                    pool.api_waiting += 1;
                    pool = wait(&self.api, pool);
                    pool.api_waiting -= 1;
                }
                pool.api_held += 1;
            }
        }
        let conn = pool.idle.pop().expect("a free connection");
        // A wakeup may have gone to a read that had been woken already: pass it on.
        self.wake(&pool);
        conn
    }

    /// Returns a connection `caller` took.
    pub(crate) fn give_back(&self, conn: Connection, caller: Caller) {
        let mut pool = lock(&self.pool);
        pool.idle.push(conn);
        if caller == Caller::Api {
            pool.api_held -= 1;
        }
        self.wake(&pool);
    }

    /// Wakes a waiting read of each kind that may take a free connection now.
    fn wake(&self, pool: &Pool) {
        if pool.instance_waiting > 0 && pool.instance_may_take() {
            self.instance.notify_one();
        }
        if pool.api_waiting > 0 && pool.api_may_take() {
            self.api.notify_one();
        }
    }

    /// The idle connections, when nothing holds one any more (closing the store).
    pub(crate) fn into_idle(self) -> Vec<Connection> {
        self.pool
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .idle
    }

    /// Idle connections and the reads waiting: (idle, instance, API).
    #[cfg(test)]
    pub(crate) fn state(&self) -> (usize, usize, usize) {
        let pool = lock(&self.pool);
        (pool.idle.len(), pool.instance_waiting, pool.api_waiting)
    }

    /// Runs `f` on each idle connection.
    #[cfg(test)]
    pub(crate) fn each_idle(&self, f: impl Fn(&Connection) -> bool) -> bool {
        lock(&self.pool).idle.iter().all(f)
    }
}
