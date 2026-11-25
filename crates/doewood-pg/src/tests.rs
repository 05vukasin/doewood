//! Tests against a real database. They run only when `DOEWOOD_TEST_POSTGRES_URL` is set, and
//! skip with a message otherwise. Each test works in a fresh schema, so they run in parallel
//! on one database.

use std::time::{Duration, Instant};

use doewood_core::{InstanceId, PostgresUrl};
use doewood_model::{ClaimStore, Error, SourceKey};
use uuid::Uuid;

use crate::PgStore;

mod companies_due;
mod list_records;
mod read_paths;

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
        // pgvector in `public`, shared by every test schema, created under the store's own
        // lock. Left to the store, it would land in the first test schema to connect and go
        // when that schema is dropped, under the feet of the other tests.
        admin
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock({}); \
                 DO $$ BEGIN \
                     IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') \
                     THEN CREATE EXTENSION IF NOT EXISTS vector SCHEMA public; END IF; \
                 END $$; COMMIT",
                crate::MIGRATION_LOCK
            ))
            .await
            .unwrap();
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

/// Issue #51: two instances (two pools, so two connections) patch different keys of one
/// company from two tasks on two threads, many rounds, and both add to one shared list.
/// Every write survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn props_patches_race_on_two_connections() {
    use doewood_model::{GraphStore, NodeKind, PropsPatch};
    use serde_json::json;

    const ROUNDS: usize = 100;
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let (a, b) = (db.store().await, db.store().await);
    let id = a.ensure_node(NodeKind::Company, "raced").await.unwrap();
    let writer = |s: PgStore, who: &'static str| {
        tokio::spawn(async move {
            for i in 0..ROUNDS {
                let patch = PropsPatch::new()
                    .set(&[&format!("{who}{i}")], json!(i))
                    .add_to_set(&["log"], json!(format!("{who}{i}")), 10 * ROUNDS)
                    .set_if_absent(&["name"], json!(who));
                s.update_props(id, &patch).await.unwrap().unwrap();
            }
        })
    };
    let (x, y) = (writer(a.clone(), "a"), writer(b.clone(), "b"));
    x.await.unwrap();
    y.await.unwrap();
    let props = a.node(id).await.unwrap().unwrap().props;
    for who in ["a", "b"] {
        for i in 0..ROUNDS {
            assert_eq!(props[format!("{who}{i}")], json!(i), "{who}{i} kept");
        }
    }
    assert_eq!(props["log"].as_array().unwrap().len(), 2 * ROUNDS);
    assert!(props["name"] == json!("a") || props["name"] == json!("b"));
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
    // Catalog reads by this schema's own relations only (issue #58): a view such as
    // `pg_indexes` or `information_schema.columns` calls functions on every relation's oid
    // before it filters by schema, and those fail on a relation another test drops at the
    // same moment ("could not open relation with OID").
    let embedding: bool = db
        .admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) \
             AND attname = 'embedding' AND NOT attisdropped)",
            &[&format!("{}.search_index", db.schema)],
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
    let def: Option<String> = db
        .admin
        .query_one(
            "SELECT pg_get_indexdef(to_regclass($1))",
            &[&format!("{}.search_tsv", db.schema)],
        )
        .await
        .unwrap()
        .get(0);
    let gin = def
        .as_deref()
        .is_some_and(|d| d.contains("USING gin (tsv)"));
    assert!(gin, "{def:?}");
    // Its pending list is bounded (schema 7, issue #102), so a search scans at most 256 kB of it.
    assert!(
        def.as_deref()
            .is_some_and(|d| d.contains("gin_pending_list_limit='256'")),
        "{def:?}"
    );
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

/// Two instances merging one pair in opposite directions at once: one merge wins, the other
/// sees the husk and is refused, and the log has one entry.
#[tokio::test]
async fn concurrent_merges_of_one_pair_have_one_winner() {
    use doewood_model::{
        GraphStore, MergeActor, MergeActorKind, MergeQuery, MergeRequest, MergeStore, NodeKind,
    };
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let stores = [db.store().await, db.store().await];
    let x = stores[0]
        .upsert_node(NodeKind::Company, "race-x", &serde_json::json!({}))
        .await
        .unwrap();
    let y = stores[0]
        .upsert_node(NodeKind::Company, "race-y", &serde_json::json!({}))
        .await
        .unwrap();
    let req = |survivor, absorbed, who: &str| MergeRequest {
        survivor,
        absorbed,
        actor: MergeActor {
            kind: MergeActorKind::Enrichment,
            instance: InstanceId::new(who).unwrap(),
        },
        evidence: serde_json::json!([]),
        confidence: 1.0,
        probability: 0.99,
    };
    let (one, two) = (req(x, y, "inst-a"), req(y, x, "inst-b"));
    let (r1, r2) = tokio::join!(
        stores[0].merge_companies(&one),
        stores[1].merge_companies(&two),
    );
    assert!(r1.is_ok() != r2.is_ok(), "{r1:?} {r2:?}");
    let lost = if r1.is_ok() { r2 } else { r1 };
    assert!(matches!(lost, Err(Error::Precondition(_))), "{lost:?}");
    let log = stores[1].list_merges(&MergeQuery::default()).await.unwrap();
    assert_eq!(log.len(), 1);
    db.drop_schema().await;
}

/// Issue #48: one instance merges companies while another writes party edges to the
/// absorbed one, which it matched before the merge, both at once, many rounds. No edge is
/// left on a husk: each lands on the survivor, by the write's own resolution or by the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn party_edges_never_land_on_a_husk_under_concurrent_merges() {
    use doewood_model::{
        Edge, EdgeKind, EdgeSource, GraphStore, MergeActor, MergeActorKind, MergeRequest,
        MergeStore, NodeKind,
    };
    use serde_json::json;

    const ROUNDS: usize = 150;
    const DOCS: usize = 4;
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let (merger, writer) = (db.store().await, db.store().await);
    let mut redirected = 0;
    for round in 0..ROUNDS {
        let survivor = merger
            .upsert_node(NodeKind::Company, &format!("s{round}"), &json!({}))
            .await
            .unwrap();
        let absorbed = merger
            .upsert_node(NodeKind::Company, &format!("a{round}"), &json!({}))
            .await
            .unwrap();
        let mut docs = Vec::new();
        for d in 0..DOCS {
            docs.push(
                merger
                    .upsert_node(NodeKind::Document, &format!("ref:d{round}-{d}"), &json!({}))
                    .await
                    .unwrap(),
            );
        }
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let merge = {
            let (s, barrier) = (merger.clone(), std::sync::Arc::clone(&barrier));
            tokio::spawn(async move {
                barrier.wait().await;
                // Staggered, so some rounds write first and some merge first.
                let lag = u64::try_from(round % 6).unwrap() * 400;
                tokio::time::sleep(Duration::from_micros(lag)).await;
                s.merge_companies(&MergeRequest {
                    survivor,
                    absorbed,
                    actor: MergeActor {
                        kind: MergeActorKind::Enrichment,
                        instance: InstanceId::new("inst-a").unwrap(),
                    },
                    evidence: json!([]),
                    confidence: 1.0,
                    probability: 0.99,
                })
                .await
                .unwrap();
            })
        };
        let write = {
            let s = writer.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let mut redirected = 0;
                for doc in docs {
                    // Matched before the merge: the edge is meant for `absorbed`.
                    let e = Edge::new(doc, absorbed, EdgeKind::Party, EdgeSource::Document);
                    let w = s.upsert_live_edge(&e).await.unwrap();
                    assert!(w.dst == absorbed || w.dst == survivor, "{w:?}");
                    redirected += usize::from(w.redirects > 0);
                }
                redirected
            })
        };
        merge.await.unwrap();
        redirected += write.await.unwrap();
    }
    let c = db.client().await;
    let count = |sql: &'static str| {
        let c = &c;
        async move { c.query_one(sql, &[]).await.unwrap().get::<_, i64>(0) }
    };
    let on_husks = count(
        "SELECT count(*) FROM edges e JOIN nodes n ON n.id = e.dst \
         WHERE e.kind = 'party' AND n.props ? 'merged_into'",
    )
    .await;
    let on_survivors = count(
        "SELECT count(*) FROM edges e JOIN nodes n ON n.id = e.dst \
         WHERE e.kind = 'party' AND e.state NOT IN ('retracted', 'superseded') \
         AND n.key LIKE 's%' AND NOT n.props ? 'merged_into'",
    )
    .await;
    eprintln!(
        "{ROUNDS} rounds: {redirected} of {} edges redirected by the write itself, \
         the rest moved by the merge",
        ROUNDS * DOCS
    );
    assert_eq!(on_husks, 0, "party edges left on husks");
    assert_eq!(on_survivors, i64::try_from(ROUNDS * DOCS).unwrap());
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

/// A database of its own, dropped (with its connections) at the end of a passing test.
struct FreshDb {
    admin: tokio_postgres::Client,
    config: tokio_postgres::Config,
    name: String,
}

impl FreshDb {
    async fn new(url: &str) -> Self {
        let base: tokio_postgres::Config = url.parse().expect("test URL");
        let (admin, conn) = base
            .connect(crate::tls::connector().unwrap())
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let name = format!("dw_fresh_{}", Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap();
        let mut config = base;
        config.dbname(&name);
        Self {
            admin,
            config,
            name,
        }
    }

    async fn client(&self) -> tokio_postgres::Client {
        let (client, conn) = self
            .config
            .connect(crate::tls::connector().unwrap())
            .await
            .unwrap();
        tokio::spawn(conn);
        client
    }

    async fn drop_db(self) {
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .await
            .unwrap();
    }
}

/// Instances starting at the same moment against an empty database (a `StatefulSet` with
/// `podManagementPolicy: Parallel`): all connect, pgvector is created once and every
/// migration runs exactly once.
#[tokio::test]
async fn concurrent_first_starts_on_a_fresh_database_migrate_once() {
    const N: usize = 8;
    let Some(url) = test_url() else { return };
    let db = FreshDb::new(&url).await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(N));
    let mut tasks = Vec::new();
    for _ in 0..N {
        let config = db.config.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            PgStore::connect_config(config).await
        }));
    }
    let mut stores = Vec::new();
    for t in tasks {
        stores.push(t.await.unwrap().expect("every concurrent start connects"));
    }
    let client = db.client().await;
    let available: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(stores.iter().all(|s| s.has_vector() == available));
    let rows = client
        .query(
            "SELECT version, count(*) OVER (PARTITION BY version) FROM doewood_schema \
             ORDER BY 1",
            &[],
        )
        .await
        .unwrap();
    let versions: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        versions,
        (1..=i32::try_from(crate::SCHEMA_VERSION).unwrap()).collect::<Vec<_>>(),
        "each migration ran exactly once"
    );
    assert!(rows.iter().all(|r| r.get::<_, i64>(1) == 1));
    let extensions: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_extension WHERE extname = 'vector'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(extensions, i64::from(available));
    drop(stores);
    drop(client);
    db.drop_db().await;
}

/// A start that finds the schema lock held waits for it, then proceeds.
#[tokio::test]
async fn a_start_waits_for_the_schema_lock() {
    let Some(url) = test_url() else { return };
    let db = FreshDb::new(&url).await;
    let mut holder = db.client().await;
    let tx = holder.transaction().await.unwrap();
    tx.execute(
        "SELECT pg_advisory_xact_lock($1)",
        &[&crate::MIGRATION_LOCK],
    )
    .await
    .unwrap();
    let start = tokio::spawn(PgStore::connect_config(db.config.clone()));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !start.is_finished(),
        "the start waits while the lock is held"
    );
    tx.commit().await.unwrap();
    let store = tokio::time::timeout(Duration::from_secs(30), start)
        .await
        .expect("the start proceeds once the lock is free")
        .unwrap()
        .unwrap();
    drop(store);
    drop(holder);
    db.drop_db().await;
}

/// Schema 5 moves the props lists of schema 4 into the proposals table, once: duplicates keep
/// the latest entry, a dismissed pair is closed, malformed entries are left out, and the lists
/// leave the props (the other props stay).
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
#[tokio::test]
async fn schema_5_moves_proposals_out_of_props() {
    use doewood_model::{
        ProposalKind, ProposalQuery, ProposalReason, ProposalRequest, ProposalState, ProposalStore,
    };
    use serde_json::json;

    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let (a, b, c) = (
        Uuid::from_u128(0xa),
        Uuid::from_u128(0xb),
        Uuid::from_u128(0xc),
    );
    {
        let client = db.client().await;
        client
            .batch_execute(
                "CREATE TABLE doewood_schema (version integer PRIMARY KEY, \
                 applied_at timestamptz NOT NULL DEFAULT now())",
            )
            .await
            .unwrap();
        for (version, sql) in (1_i32..).zip(&crate::migrate::MIGRATIONS[..4]) {
            client.batch_execute(sql).await.unwrap();
            client
                .execute(
                    "INSERT INTO doewood_schema (version) VALUES ($1)",
                    &[&version],
                )
                .await
                .unwrap();
        }
        let props_a = json!({
            "name": "Alfa d.o.o.",
            "kept": 1,
            "merge_proposals": [
                {"company": b.to_string(), "probability": 0.7, "reason": "uncertain",
                 "evidence": [{"source": "name_ladder"}], "at": 1_700_000_000},
                {"company": b.to_string(), "probability": 0.8, "reason": "conflict",
                 "evidence": [{"source": "registry_id"}], "at": 1_700_000_100},
                {"company": c.to_string(), "probability": 0.95, "reason": "dismissed",
                 "dismissal": 1, "evidence": [], "at": 1_700_000_200},
                {"company": "not-an-id", "probability": 0.6},
                {"company": a.to_string(), "probability": 0.6},
                {"company": Uuid::from_u128(0xdead).to_string(), "probability": 0.6},
                "junk"
            ],
            "registry_proposals": [
                {"registry": "apr", "record": "companies/12345678", "name": "Alfa DOO",
                 "probability": 0.75, "reason": "uncertain", "at": 1_700_000_300},
                {"registry": "apr"}
            ]
        });
        let props_b = json!({
            "name": "Alfa Plus d.o.o.",
            "merge_proposals": [
                {"company": a.to_string(), "probability": 0.6, "reason": "uncertain",
                 "at": 1_700_000_050}
            ],
            "registry_proposals": []
        });
        for (id, key, props) in [
            (a, "a", props_a),
            (b, "b", props_b),
            (c, "c", json!({"name": "Gama"})),
        ] {
            client
                .execute(
                    "INSERT INTO nodes (id, kind, key, props) VALUES ($1, 'company', $2, $3)",
                    &[&id, &key, &props],
                )
                .await
                .unwrap();
        }
    }
    let s = db.store().await;
    let all = s.list_proposals(&ProposalQuery::default()).await.unwrap();
    assert_eq!(all.len(), 3, "{all:#?}");
    let pair = all
        .iter()
        .find(|p| p.kind == ProposalKind::Merge && p.state == ProposalState::Open)
        .expect("the open pair");
    assert_eq!(
        (pair.company, pair.candidate.as_str()),
        (a, b.to_string().as_str())
    );
    assert!((pair.probability - 0.8).abs() < 1e-6, "the latest entry");
    assert_eq!(pair.reason, ProposalReason::Conflict);
    assert_eq!(pair.evidence, json!([{"source": "registry_id"}]));
    assert_eq!(pair.created_at, 1_700_000_100);
    assert_eq!(pair.detail["from_props"], a.to_string());
    let closed = all
        .iter()
        .find(|p| p.state == ProposalState::Rejected)
        .expect("the dismissed pair");
    assert_eq!(
        (closed.company, closed.candidate.as_str()),
        (a, c.to_string().as_str())
    );
    assert_eq!(closed.reason, ProposalReason::Dismissed);
    let r = closed.resolution.clone().expect("closed");
    assert!(r.dismissed);
    assert_eq!((r.actor, r.at), (None, 1_700_000_200));
    let rec = all
        .iter()
        .find(|p| p.kind == ProposalKind::RegistryRecord)
        .expect("the record");
    assert_eq!(rec.company, a);
    assert_eq!(rec.registry_record(), Some(("apr", "companies/12345678")));
    assert_eq!(rec.detail["name"], "Alfa DOO");
    assert_eq!(
        (rec.reason, rec.state),
        (ProposalReason::ReviewBand, ProposalState::Open)
    );
    let props = |id: Uuid| {
        let s = s.clone();
        async move {
            doewood_model::GraphStore::node(&s, id)
                .await
                .unwrap()
                .unwrap()
                .props
        }
    };
    assert_eq!(props(a).await, json!({"name": "Alfa d.o.o.", "kept": 1}));
    assert_eq!(props(b).await, json!({"name": "Alfa Plus d.o.o."}));
    assert_eq!(props(c).await, json!({"name": "Gama"}));
    let again =
        ProposalRequest::merge(c, a, json!({}), json!([]), 0.95, ProposalReason::ReviewBand);
    assert_eq!(
        s.propose(&again).await.unwrap(),
        None,
        "the dismissed pair stays closed"
    );
    // A second instance starting migrates nothing twice.
    let second = db.store().await;
    assert_eq!(
        second
            .list_proposals(&ProposalQuery::default())
            .await
            .unwrap()
            .len(),
        3
    );
    db.drop_schema().await;
}
