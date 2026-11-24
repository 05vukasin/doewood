//! Tests against a real database. They run only when `DOEWOOD_TEST_POSTGRES_URL` is set, and
//! skip with a message otherwise. Each test works in a fresh schema, so they run in parallel
//! on one database.

use std::time::{Duration, Instant};

use doewood_core::{InstanceId, PostgresUrl};
use doewood_model::{ClaimStore, Error, SourceKey};
use uuid::Uuid;

use crate::PgStore;

mod list_records;

const VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";

fn test_url() -> Option<String> {
    match std::env::var(VAR) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => {
            eprintln!("skipping: {VAR} is not set; the postgres CI job sets it");
            None
        }
    }
}

/// A fresh schema; dropped by [`Scratch::drop_schema`] at the end of a passing test.
struct Scratch {
    admin: tokio_postgres::Client,
    config: tokio_postgres::Config,
    schema: String,
}

impl Scratch {
    async fn new(url: &str) -> Self {
        let base: tokio_postgres::Config = url.parse().expect("test URL");
        let (admin, conn) = base
            .connect(crate::tls::connector().unwrap())
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let schema = format!("dw_test_{}", Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        // pgvector in `public`, shared by every test schema. Concurrent tests may race to
        // create it; whoever loses sees it created.
        let _ = admin
            .batch_execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public")
            .await;
        let mut config = base;
        config.options(format!("-c search_path={schema}"));
        Self {
            admin,
            config,
            schema,
        }
    }

    async fn store(&self) -> PgStore {
        PgStore::connect_config(self.config.clone()).await.unwrap()
    }

    /// A plain connection with the schema on its search path.
    async fn client(&self) -> tokio_postgres::Client {
        let (client, conn) = self
            .config
            .connect(crate::tls::connector().unwrap())
            .await
            .unwrap();
        tokio::spawn(conn);
        client
    }

    async fn drop_schema(self) {
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn conformance() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    // Two pools: two instances sharing one database.
    let (a, b) = (db.store().await, db.store().await);
    doewood_model::conformance::run(&a, &b).await;
    db.drop_schema().await;
}

#[tokio::test]
async fn migrations_are_idempotent_and_versioned() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let first = db.store().await;
    let second = db.store().await;
    assert_eq!(first.has_vector(), second.has_vector());
    let versions: Vec<i32> = db
        .admin
        .query(
            &format!(
                "SELECT version FROM {}.doewood_schema ORDER BY 1",
                db.schema
            ),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        versions,
        (1..=i32::try_from(crate::SCHEMA_VERSION).unwrap()).collect::<Vec<_>>()
    );
    let embedding: bool = db
        .admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = 'search_index' \
             AND column_name = 'embedding')",
            &[&db.schema],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(embedding, first.has_vector());
    let available: bool = db
        .admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(first.has_vector(), available, "pgvector used iff available");
    // A tsvector column with a GIN index.
    let gin: bool = db
        .admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 \
             AND indexname = 'search_tsv' AND indexdef LIKE '%USING gin (tsv)%')",
            &[&db.schema],
        )
        .await
        .unwrap()
        .get(0);
    assert!(gin);
    db.drop_schema().await;
}

/// A role that may not create pgvector (it is not a trusted extension) still gets a working
/// store, without the embedding column.
#[tokio::test]
async fn pgvector_is_skipped_when_it_cannot_be_created() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let name = db.schema.replace("dw_test_", "dw_noext_");
    // One statement per call: CREATE DATABASE refuses an (implicit) transaction.
    for sql in [
        format!("CREATE ROLE {name} LOGIN PASSWORD 'noext'"),
        format!("CREATE DATABASE {name} OWNER {name}"),
    ] {
        db.admin.batch_execute(&sql).await.unwrap();
    }
    let mut config: tokio_postgres::Config = url.parse().unwrap();
    config.user(&name).password("noext").dbname(&name);
    let store = PgStore::connect_config(config).await.unwrap();
    assert!(!store.has_vector());
    doewood_model::conformance::records(&store).await;
    drop(store);
    for sql in [
        format!("DROP DATABASE {name} WITH (FORCE)"),
        format!("DROP ROLE {name}"),
    ] {
        db.admin.batch_execute(&sql).await.unwrap();
    }
    db.drop_schema().await;
}

#[tokio::test]
async fn a_newer_schema_is_refused() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    drop(db.store().await);
    db.admin
        .batch_execute(&format!(
            "INSERT INTO {}.doewood_schema (version) VALUES (999)",
            db.schema
        ))
        .await
        .unwrap();
    let err = PgStore::connect_config(db.config.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::SchemaTooNew { found: 999, .. }),
        "{err:?}"
    );
    db.drop_schema().await;
}

#[tokio::test]
async fn concurrent_claims_have_one_winner() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let stores = [db.store().await, db.store().await];
    let src = SourceKey::new("http://minio:9000", "race", "").unwrap();
    let mut tasks = Vec::new();
    for i in 0..16 {
        let store = stores[i % 2].clone();
        let src = src.clone();
        tasks.push(tokio::spawn(async move {
            let me = InstanceId::new(format!("racer-{i}")).unwrap();
            store.claim_source(&src, &me).await.map(|c| c.owner)
        }));
    }
    let mut winners = Vec::new();
    let mut losers = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(owner) => winners.push(owner),
            Err(Error::SourceClaimed { owner, .. }) => {
                losers += 1;
                assert!(owner.as_str().starts_with("racer-"));
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(winners.len(), 1, "{winners:?}");
    assert_eq!(losers, 15);
    let owner = stores[0].source_owner(&src).await.unwrap().unwrap().owner;
    assert_eq!(owner, winners[0]);
    db.drop_schema().await;
}

/// Needs no database: nothing listens on port 1.
#[tokio::test]
async fn an_unreachable_database_is_unavailable_and_fails_fast() {
    let url =
        PostgresUrl::parse("postgres://dw:hunter2@127.0.0.1:1/doewood?connect_timeout=2").unwrap();
    let started = Instant::now();
    let err = PgStore::connect(&url).await.unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(matches!(err, Error::Unavailable(_)), "{err:?}");
    assert!(!err.to_string().contains("hunter2"), "{err}");
}

#[tokio::test]
async fn a_bad_url_is_invalid() {
    let url = PostgresUrl::parse("postgres://dw@db/x?sslmode=sometimes").unwrap();
    assert!(matches!(
        PgStore::connect(&url).await,
        Err(Error::Invalid(_))
    ));
}
