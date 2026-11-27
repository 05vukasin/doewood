//! Schema migrations, embedded in the binary. The version is SQLite's `user_version`.

use rusqlite::Connection;

use crate::StoreError;

/// Which foreign keys a migration checks before it commits (issue #159).
///
/// Foreign keys are off while migrations run (see [`migrate`]), so a migration that moves or
/// copies rows could leave a dangling reference that nothing else would catch. The check
/// over the whole file reads every row of every table with a foreign key: on a 200,000-document
/// store it took 70 to 310 s, against 8 s for the migration it followed. So each migration
/// says what it can break.
#[derive(Clone, Copy, Debug)]
pub(crate) enum FkCheck {
    /// Every table: a migration that rebuilds a table (SQLite's create-copy-drop-rename
    /// procedure: the rebuilt table's rows, and the rows of every table that references it,
    /// must still match), deletes rows, or moves data between tables.
    Whole,
    /// Only these tables' own foreign keys: a migration that writes rows into them and
    /// touches no other table's rows (a new table filled from existing data).
    Tables(&'static [&'static str]),
    /// None: a migration that writes no rows and rebuilds no table (indexes, triggers, new
    /// empty tables).
    Skip,
}

/// One migration: its SQL and what it checks.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Migration {
    pub(crate) sql: &'static str,
    pub(crate) fk_check: FkCheck,
}

const fn whole(sql: &'static str) -> Migration {
    Migration {
        sql,
        fk_check: FkCheck::Whole,
    }
}

/// The migrations, in order. Migration `i` takes the schema from version `i` to `i + 1`.
/// Never edit a released migration: add a new one. Schemas 1 to 10 rebuild tables or move
/// data, and keep the whole check (only a store older than schema 10 runs them).
/// `migrations_declare_what_they_check` holds every migration to its [`FkCheck`].
const MIGRATIONS: &[Migration] = &[
    whole(include_str!("migrations/0001_init.sql")),
    whole(include_str!("migrations/0002_standalone_model.sql")),
    whole(include_str!("migrations/0003_sources.sql")),
    whole(include_str!("migrations/0004_ingest.sql")),
    whole(include_str!("migrations/0005_llm.sql")),
    whole(include_str!("migrations/0006_enrichment.sql")),
    whole(include_str!("migrations/0007_merges.sql")),
    whole(include_str!("migrations/0008_proposals.sql")),
    whole(include_str!("migrations/0009_feeds.sql")),
    whole(include_str!("migrations/0010_status_counts.sql")),
    // A new table filled from `sketch_bands`, and triggers.
    Migration {
        sql: include_str!("migrations/0011_sketch_band_sizes.sql"),
        fk_check: FkCheck::Tables(&["sketch_band_sizes"]),
    },
    // Drops and builds an index.
    Migration {
        sql: include_str!("migrations/0012_graph_pages.sql"),
        fk_check: FkCheck::Skip,
    },
    // A column with a default and a new empty table without foreign keys.
    Migration {
        sql: include_str!("migrations/0013_known_copies.sql"),
        fk_check: FkCheck::Skip,
    },
    // New tables filled from `nodes` and `edges`, indexes and triggers (issue #172).
    Migration {
        sql: include_str!("migrations/0014_company_graph.sql"),
        fk_check: FkCheck::Tables(&["company_stats", "company_graph_log"]),
    },
];

/// The schema version this binary writes.
#[allow(clippy::cast_possible_wrap)]
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// Brings the schema up to [`SCHEMA_VERSION`], one transaction per migration.
///
/// Foreign keys are off while migrations run, so that a migration can rebuild a table that
/// others reference (SQLite's create-copy-drop-rename procedure; the pragma has no effect
/// inside a transaction). Each migration checks what its [`FkCheck`] says before it commits;
/// the caller turns them back on.
pub(crate) fn migrate(conn: &mut Connection) -> Result<i64, StoreError> {
    migrate_to(conn, SCHEMA_VERSION)
}

/// Brings the schema up to `target` (at most [`SCHEMA_VERSION`]): [`migrate`], or a test's
/// older schema to migrate data from.
pub(crate) fn migrate_to(conn: &mut Connection, target: i64) -> Result<i64, StoreError> {
    run(conn, MIGRATIONS, target)
}

/// The foreign key violations `check` looks for, in the open transaction.
pub(crate) fn fk_violations(conn: &Connection, check: FkCheck) -> rusqlite::Result<i64> {
    match check {
        FkCheck::Whole => {
            conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
                r.get(0)
            })
        }
        // An unknown table is an error ("no such table"), not an empty check.
        FkCheck::Tables(tables) => tables
            .iter()
            .map(|t| {
                conn.query_row(
                    "SELECT count(*) FROM pragma_foreign_key_check(?1)",
                    [t],
                    |r| r.get::<_, i64>(0),
                )
            })
            .sum(),
        FkCheck::Skip => Ok(0),
    }
}

/// [`migrate_to`] over `migrations`.
fn run(conn: &mut Connection, migrations: &[Migration], target: i64) -> Result<i64, StoreError> {
    #[allow(clippy::cast_possible_wrap)]
    let latest = migrations.len() as i64;
    conn.pragma_update(None, "foreign_keys", false)?;
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if current > latest {
        return Err(StoreError::SchemaTooNew {
            found: current,
            supported: latest,
        });
    }
    for (version, m) in (1_i64..)
        .zip(migrations)
        .skip(usize::try_from(current).unwrap_or(0))
        .take_while(|(v, _)| *v <= target)
    {
        let tx = conn.transaction()?;
        tx.execute_batch(m.sql)?;
        let violations = fk_violations(&tx, m.fk_check)?;
        if violations > 0 {
            // Dropping `tx` rolls the migration back.
            return Err(StoreError::Migration {
                version,
                reason: format!("{violations} foreign key violation(s)"),
            });
        }
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
    }
    Ok(target.min(latest))
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use rusqlite::Connection;

    use super::{FkCheck, MIGRATIONS, Migration, SCHEMA_VERSION, fk_violations, run};

    /// `sql` without comments and trigger bodies (`CREATE TRIGGER` to `END;`), upper case: the
    /// statements a migration runs itself.
    fn own_statements(sql: &str) -> String {
        let text: String = sql
            .lines()
            .map(|l| l.split_once("--").map_or(l, |(code, _)| code))
            .collect::<Vec<_>>()
            .join("\n")
            .to_uppercase();
        let mut out = String::new();
        let mut rest = text.as_str();
        while let Some(at) = rest.find("CREATE TRIGGER") {
            out.push_str(&rest[..at]);
            let end = rest[at..].find("END;").expect("a trigger ends with END;");
            rest = &rest[at + end + 4..];
        }
        out.push_str(rest);
        out
    }

    /// The table after each `keyword` in `sql`.
    fn targets<'a>(sql: &'a str, keyword: &str) -> Vec<&'a str> {
        sql.match_indices(keyword)
            .filter_map(|(at, _)| sql[at + keyword.len()..].split_whitespace().next())
            .map(|t| t.trim_matches(|c: char| !c.is_alphanumeric() && c != '_'))
            .collect()
    }

    /// Issue #159: a migration that skips the check, or checks only some tables, does only what
    /// that allows: `Skip` writes no rows and rebuilds no table; `Tables` writes rows only into
    /// its tables, and deletes and rebuilds nothing (those need the whole check).
    #[test]
    fn migrations_declare_what_they_check() {
        for (version, m) in (1..).zip(MIGRATIONS) {
            let own = own_statements(m.sql);
            let rebuilds = ["DROP TABLE", "RENAME", "DELETE FROM"]
                .iter()
                .any(|k| own.contains(k));
            let writes: Vec<&str> = ["INSERT INTO", "INSERT OR", "REPLACE INTO", "UPDATE "]
                .iter()
                .flat_map(|k| targets(&own, k))
                .collect();
            match m.fk_check {
                FkCheck::Whole => {}
                FkCheck::Skip => {
                    assert!(
                        !rebuilds && writes.is_empty(),
                        "migration {version}: {writes:?}"
                    );
                }
                FkCheck::Tables(tables) => {
                    assert!(!rebuilds, "migration {version} needs the whole check");
                    for w in writes {
                        assert!(
                            tables.iter().any(|t| t.eq_ignore_ascii_case(w)),
                            "migration {version} writes {w}, which it does not check"
                        );
                    }
                }
            }
        }
        // The rule sees writes and rebuilds where they are.
        let sql = "-- INSERT INTO nope\nCREATE TRIGGER t AFTER UPDATE ON a BEGIN \
                   INSERT INTO b VALUES (1); END;\nINSERT INTO c SELECT 1; DROP TABLE d;";
        let own = own_statements(sql);
        assert_eq!(targets(&own, "INSERT INTO"), ["C"]);
        assert!(own.contains("DROP TABLE") && !own.contains("NOPE"));
    }

    /// A test schema: `parent` and `child` (with a foreign key), and one child row.
    const BASE: Migration = Migration {
        sql: "CREATE TABLE parent (id INTEGER PRIMARY KEY); \
              CREATE TABLE child (id INTEGER PRIMARY KEY, \
                  parent_id INTEGER NOT NULL REFERENCES parent (id)); \
              INSERT INTO parent VALUES (1); INSERT INTO child VALUES (1, 1);",
        fk_check: FkCheck::Whole,
    };

    fn children(conn: &Connection) -> Vec<(i64, i64)> {
        let mut stmt = conn
            .prepare("SELECT id, parent_id FROM child ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    /// Issue #159: a migration that leaves a dangling reference still fails and is rolled
    /// back, whether it checks the whole file or the table it wrote; one that checks nothing
    /// is not checked, which is why it may write nothing.
    #[test]
    fn a_migration_that_breaks_a_foreign_key_fails() {
        // A rebuild of `child` (create, copy, drop, rename) that leaves an orphan.
        let rebuild = Migration {
            sql: "CREATE TABLE child_new (id INTEGER PRIMARY KEY, \
                      parent_id INTEGER NOT NULL REFERENCES parent (id)); \
                  INSERT INTO child_new SELECT id, parent_id FROM child; \
                  INSERT INTO child_new VALUES (2, 99); \
                  DROP TABLE child; ALTER TABLE child_new RENAME TO child;",
            fk_check: FkCheck::Whole,
        };
        let orphan = |fk_check| Migration {
            sql: "INSERT INTO child VALUES (2, 99);",
            fk_check,
        };
        // A parent deleted under its child: only the child's check sees it.
        let delete_parent = Migration {
            sql: "DELETE FROM parent;",
            fk_check: FkCheck::Whole,
        };
        for (name, bad) in [
            ("rebuild", rebuild),
            ("whole", orphan(FkCheck::Whole)),
            ("tables", orphan(FkCheck::Tables(&["child"]))),
            ("delete", delete_parent),
        ] {
            let mut conn = Connection::open_in_memory().unwrap();
            let err = run(&mut conn, &[BASE, bad], 2).unwrap_err();
            assert!(
                matches!(err, crate::StoreError::Migration { version: 2, .. }),
                "{name}: {err:?}"
            );
            let version: i64 = conn
                .pragma_query_value(None, "user_version", |r| r.get(0))
                .unwrap();
            assert_eq!(version, 1, "{name}: rolled back");
            assert_eq!(children(&conn), [(1, 1)], "{name}");
        }
        // A table to check that does not exist is an error, not an empty check.
        let mut conn = Connection::open_in_memory().unwrap();
        assert!(run(&mut conn, &[BASE, orphan(FkCheck::Tables(&["nope"]))], 2).is_err());
        // Skip checks nothing: the orphan goes in (the declaration rule above forbids it).
        let mut conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            run(&mut conn, &[BASE, orphan(FkCheck::Skip)], 2).unwrap(),
            2
        );
        assert_eq!(children(&conn), [(1, 1), (2, 99)]);
        // A good migration of each kind passes.
        let mut conn = Connection::open_in_memory().unwrap();
        let good = [
            BASE,
            Migration {
                sql: "CREATE INDEX child_by_parent ON child (parent_id);",
                fk_check: FkCheck::Skip,
            },
            Migration {
                sql: "INSERT INTO child VALUES (3, 1);",
                fk_check: FkCheck::Tables(&["child"]),
            },
        ];
        assert_eq!(run(&mut conn, &good, 3).unwrap(), 3);
        assert_eq!(children(&conn), [(1, 1), (3, 1)]);
    }

    /// Times each pending migration of the store file `DOEWOOD_MIGRATE_DB` (issue #112: the
    /// `edges_by_dst` rebuild of schema 12 on a big store; issue #159: the check): first its
    /// SQL, its own check and the whole-file check apart, rolled back, then as
    /// [`super::migrate`] runs it. Migrates the file: run it on a copy.
    /// `DOEWOOD_MIGRATE_DB=<file> cargo test --release -p doewood-store --lib migration_timings
    /// -- --ignored --nocapture`. `DOEWOOD_MIGRATE_SPLIT=0` leaves the rolled-back parts out;
    /// `DOEWOOD_MIGRATE_ALL_WHOLE=1` migrates as before #159, every migration with the
    /// whole-file check.
    #[test]
    #[ignore = "a measurement (issues #112, #159); see docs/configuration.md"]
    fn migration_timings() {
        let path = std::env::var("DOEWOOD_MIGRATE_DB").expect("DOEWOOD_MIGRATE_DB");
        let split = std::env::var("DOEWOOD_MIGRATE_SPLIT").map_or(true, |v| v != "0");
        let all_whole = std::env::var("DOEWOOD_MIGRATE_ALL_WHOLE").is_ok_and(|v| v == "1");
        let mut conn = Connection::open(path).unwrap();
        let from: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        let edges: i64 = conn
            .query_row("SELECT count(*) FROM edges", [], |r| r.get(0))
            .unwrap();
        println!("schema {from}, {edges} edges");
        for target in (from + 1..=SCHEMA_VERSION).filter(|_| split) {
            let m = MIGRATIONS[usize::try_from(target - 1).unwrap()];
            let tx = conn.transaction().unwrap();
            let t = Instant::now();
            tx.execute_batch(m.sql).unwrap();
            let sql = t.elapsed().as_secs_f64();
            let t = Instant::now();
            fk_violations(&tx, m.fk_check).unwrap();
            let own = t.elapsed().as_secs_f64();
            println!(
                "migration {target} ({:?}), rolled back: SQL {sql:.2} s, its check {own:.2} s",
                m.fk_check
            );
        }
        let migrations: Vec<Migration> = MIGRATIONS
            .iter()
            .map(|m| Migration {
                fk_check: if all_whole {
                    FkCheck::Whole
                } else {
                    m.fk_check
                },
                ..*m
            })
            .collect();
        let started = Instant::now();
        for target in from + 1..=SCHEMA_VERSION {
            let t = Instant::now();
            run(&mut conn, &migrations, target).unwrap();
            println!(
                "migration {target}{}: {:.2} s",
                if all_whole { " (whole-file check)" } else { "" },
                t.elapsed().as_secs_f64()
            );
        }
        println!(
            "schema {from} to {SCHEMA_VERSION}: {:.2} s",
            started.elapsed().as_secs_f64()
        );
        conn.pragma_update(None, "foreign_keys", true).unwrap();
    }
}
