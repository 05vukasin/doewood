//! Issue #172 on generated data, as on SQLite (`doewood-store`, `standalone/explore_scale.rs`):
//! the company graph's counts, folded from the log (also while writers and other folds run),
//! match a count from scratch (and the document total of issue #181, also across the upgrade
//! to schema 13), also after purges delete documents and companies (issue #168); and an
//! ignored test times the explorer's reads.

use std::collections::BTreeMap;
use std::time::Instant;

use doewood_model::explore::LINK_PARTIES_MAX;
use doewood_model::{
    Edge, EdgeKind, EdgeSource, EdgeState, GraphStore, NodeKind, PurgeStore, Purged,
};
use serde_json::json;
use uuid::Uuid;

use super::{Scratch, test_url};

/// `n` documents with three `party` edges each (buyer and supplier skewed to the hubs,
/// signatory uniform) and `companies` named companies `co-1` ..; a corporate edge from each of
/// the first tenth to another. With `churn`, one party edge in 13 is then retracted, one in 29
/// deleted, and one in 31 moved to `co-1`.
fn seed(n: u32, companies: u32, churn: bool) -> String {
    let mut s = format!(
        "SELECT setseed(0.172);
         CREATE TEMP TABLE seed_co AS
           SELECT c, gen_random_uuid() AS id FROM generate_series(1, {companies}) c;
         INSERT INTO nodes (id, kind, key, props)
           SELECT id, 'company', 'co-' || c,
                  jsonb_build_object('name', 'Company ' || lpad((c * 7919 % 100000)::text, 5, '0'))
           FROM seed_co;
         CREATE TEMP TABLE seed_docs AS
           SELECT i, gen_random_uuid() AS id FROM generate_series(1, {n}) i;
         INSERT INTO nodes (id, kind, key) SELECT id, 'document', md5(i::text) || md5((-i)::text)
           FROM seed_docs;
         INSERT INTO edges (id, src, dst, kind, props, confidence, state, source)
           SELECT gen_random_uuid(), d.id, co.id, 'party', jsonb_build_object('role', r.role),
                  0.9, 'active', 'document'
           FROM seed_docs d
           CROSS JOIN LATERAL (VALUES
               (1 + floor({companies} * (random() + 0 * d.i) ^ 2)::int % {companies}, 'buyer'),
               (1 + floor({companies} * (random() + 0 * d.i) ^ 2)::int % {companies}, 'supplier'),
               (1 + floor({companies} * (random() + 0 * d.i))::int % {companies}, 'signatory')) r (c, role)
           JOIN seed_co co ON co.c = r.c
           ON CONFLICT DO NOTHING;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), a.id, b.id, 'parent_of', 0.9, 'active', 'registry'
           FROM seed_co a JOIN seed_co b ON b.c = 1 + (a.c * 104729) % {companies}
           WHERE a.c <= {companies} / 10 AND a.c <> b.c;
         DROP TABLE seed_docs, seed_co;"
    );
    if churn {
        s.push_str(CHURN);
    }
    s.push_str(" ANALYZE nodes, edges;");
    s
}

const CHURN: &str = "
    UPDATE edges SET state = 'retracted'
      WHERE kind = 'party' AND get_byte(uuid_send(id), 15) % 13 = 0;
    DELETE FROM edges WHERE kind = 'party' AND get_byte(uuid_send(id), 14) % 29 = 0;
    UPDATE edges e SET dst = (SELECT id FROM nodes WHERE kind = 'company' AND key = 'co-1')
      WHERE e.kind = 'party' AND get_byte(uuid_send(e.id), 13) % 31 = 0
      AND e.id = (SELECT g.id FROM edges g WHERE g.src = e.src AND g.kind = 'party'
          ORDER BY g.id LIMIT 1)
      AND NOT EXISTS (SELECT 1 FROM edges f WHERE f.src = e.src AND f.kind = 'party'
          AND f.source = e.source AND f.valid_from IS NOT DISTINCT FROM e.valid_from
          AND f.dst = (SELECT id FROM nodes WHERE kind = 'company' AND key = 'co-1'));";

type Counts = (
    BTreeMap<Uuid, (i64, i64)>,
    BTreeMap<(Uuid, Uuid), (i64, i64)>,
);

async fn from_scratch(c: &tokio_postgres::Client) -> Counts {
    let live = "e.state NOT IN ('retracted', 'superseded')";
    let mut pairs: BTreeMap<(Uuid, Uuid), (i64, i64)> = BTreeMap::new();
    for r in c
        .query(
            &format!(
                "WITH p AS (SELECT DISTINCT e.src, e.dst FROM edges e WHERE e.kind = 'party' \
                 AND {live}), n AS (SELECT src, count(*) AS k FROM p GROUP BY src) \
                 SELECT x.dst, y.dst, count(*) FROM p x JOIN p y ON y.src = x.src \
                 AND x.dst < y.dst JOIN n ON n.src = x.src WHERE n.k <= {LINK_PARTIES_MAX} \
                 GROUP BY 1, 2"
            ),
            &[],
        )
        .await
        .unwrap()
    {
        pairs.entry((r.get(0), r.get(1))).or_default().0 = r.get(2);
    }
    for r in c
        .query(
            &format!(
                "SELECT least(e.src, e.dst), greatest(e.src, e.dst), count(*) FROM edges e \
                 WHERE e.kind NOT IN ('party', 'located_at', 'amends', 'cites', 'similar_to') \
                 AND {live} AND e.src <> e.dst GROUP BY 1, 2"
            ),
            &[],
        )
        .await
        .unwrap()
    {
        pairs.entry((r.get(0), r.get(1))).or_default().1 = r.get(2);
    }
    let mut stats: BTreeMap<Uuid, (i64, i64)> = BTreeMap::new();
    for r in c
        .query(
            &format!(
                "SELECT e.dst, count(DISTINCT e.src) FROM edges e WHERE e.kind = 'party' \
                 AND {live} GROUP BY 1"
            ),
            &[],
        )
        .await
        .unwrap()
    {
        stats.entry(r.get(0)).or_default().0 = r.get(1);
    }
    for (a, b) in pairs.keys() {
        stats.entry(*a).or_default().1 += 1;
        stats.entry(*b).or_default().1 += 1;
    }
    (stats, pairs)
}

async fn stored(c: &tokio_postgres::Client) -> Counts {
    let stats = c
        .query(
            "SELECT company_id, documents, degree FROM company_stats \
             WHERE documents > 0 OR degree > 0",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect();
    let pairs = c
        .query("SELECT a, b, docs, events FROM company_links", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| ((r.get(0), r.get(1)), (r.get(2), r.get(3))))
        .collect();
    (stats, pairs)
}

/// Every party edge of one document in 16 retracted, so those leave the document total.
const RETRACT_DOCS: &str = "UPDATE edges SET state = 'retracted' WHERE kind = 'party' \
     AND src IN (SELECT id FROM nodes WHERE kind = 'document' AND key LIKE '%0')";

/// The documents with a live party edge, from scratch; and as folded.
async fn totals(c: &tokio_postgres::Client) -> (i64, i64) {
    let r = c
        .query_one(
            "SELECT (SELECT count(DISTINCT src) FROM edges WHERE kind = 'party' \
             AND state NOT IN ('retracted', 'superseded')), \
             (SELECT documents FROM company_graph_totals)",
            &[],
        )
        .await
        .unwrap();
    (r.get(0), r.get(1))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn folded_counts_match_a_count_from_scratch() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let (s, other) = (db.store().await, db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(3000, 300, false)).await.unwrap();
    // Half folded before the churn, the rest after; then writers on two pools race two
    // folders.
    s.fold_company_graph(2000).await.unwrap();
    c.batch_execute(CHURN).await.unwrap();
    c.batch_execute(RETRACT_DOCS).await.unwrap();
    let companies: Vec<Uuid> = c
        .query(
            "SELECT id FROM nodes WHERE kind = 'company' ORDER BY key LIMIT 40",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let mut writers = Vec::new();
    for (w, store) in [s.clone(), other.clone()].into_iter().enumerate() {
        let companies = companies.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..60u32 {
                let doc = store
                    .upsert_node(
                        NodeKind::Document,
                        &format!(
                            "{:064x}",
                            0x0172_0000_u32 + i * 2 + u32::try_from(w).unwrap()
                        ),
                        &json!({}),
                    )
                    .await
                    .unwrap();
                // Each document's parties, then one of them retracted again: both writers
                // touch the same companies' pairs.
                let at = |k: u32| companies[usize::try_from((i * 7 + k * 3) % 40).unwrap()];
                let edges: Vec<Edge> = (0..4)
                    .map(|k| {
                        Edge::new(doc, at(k), EdgeKind::Party, EdgeSource::Document)
                            .with_props(json!({"role": format!("r{k}")}))
                    })
                    .collect();
                store.upsert_edges(&edges).await.unwrap();
                if i % 3 == 0 {
                    store
                        .upsert_edge(&edges[1].clone().with_state(EdgeState::Retracted))
                        .await
                        .unwrap();
                }
            }
        }));
    }
    let mut folders = Vec::new();
    for store in [s.clone(), other.clone()] {
        folders.push(tokio::spawn(async move {
            for _ in 0..40 {
                store.fold_company_graph(50).await.unwrap();
            }
        }));
    }
    for t in writers.into_iter().chain(folders) {
        t.await.unwrap();
    }
    while s.fold_company_graph(500).await.unwrap().more {}
    let (want_stats, want_pairs) = from_scratch(&c).await;
    let (stats, pairs) = stored(&c).await;
    assert!(want_pairs.len() > 1000, "{}", want_pairs.len());
    assert_eq!(pairs, want_pairs);
    assert_eq!(stats, want_stats);
    let (want, got) = totals(&c).await;
    assert_eq!(got, want);
    let documents: i64 = c
        .query_one("SELECT count(*) FROM nodes WHERE kind = 'document'", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        got < documents - 150,
        "retractions lower the total: {got} of {documents}"
    );
    db.drop_schema().await;
}

/// Records for every seeded document, and a live location for one in ten: the rest are
/// orphans for a purge.
const LOCATE_SOME: &str = "
    INSERT INTO records (blake3, body, versions)
      SELECT decode(key, 'hex'), '{}', '{}' FROM nodes WHERE kind = 'document';
    INSERT INTO nodes (id, kind, key, props)
      SELECT gen_random_uuid(), 'location', jsonb_build_array('http://s', 'b', key)::text, '{}'
      FROM nodes WHERE kind = 'document' AND get_byte(decode(key, 'hex'), 0) % 10 = 0;
    INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, object_key)
      SELECT l.id, d.id, decode(d.key, 'hex'), 'http://s', 'b', d.key
      FROM nodes d JOIN nodes l ON l.kind = 'location'
        AND l.key = jsonb_build_array('http://s', 'b', d.key)::text
      WHERE d.kind = 'document' AND get_byte(decode(d.key, 'hex'), 0) % 10 = 0;";

/// Every batch of an orphan purge of records.
async fn purge_records(s: &crate::PgStore) -> Purged {
    let (mut total, mut after) = (Purged::default(), None::<String>);
    loop {
        let p = s.purge_orphan_records(after.as_deref(), 97).await.unwrap();
        total.add(&p);
        match p.next {
            Some(n) => after = Some(n),
            None => return total,
        }
    }
}

/// Issue #168: purges delete documents (and the companies only they named, with their
/// corporate edges) through the edges' delete triggers, so the folded counts still match a
/// count from scratch; with two purges on two pools and a fold racing them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purged_counts_match_a_count_from_scratch() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let (s, other) = (db.store().await, db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(3000, 300, false)).await.unwrap();
    c.batch_execute(LOCATE_SOME).await.unwrap();
    s.fold_company_graph(2000).await.unwrap();
    c.batch_execute(CHURN).await.unwrap();
    let kept: i64 = c
        .query_one("SELECT count(*) FROM locations", &[])
        .await
        .unwrap()
        .get(0);
    let purgers: Vec<_> = [s.clone(), other.clone()]
        .into_iter()
        .map(|store| tokio::spawn(async move { purge_records(&store).await }))
        .collect();
    let folder = {
        let store = other.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                store.fold_company_graph(50).await.unwrap();
            }
        })
    };
    let mut purged = Purged::default();
    for p in purgers {
        purged.add(&p.await.unwrap());
    }
    folder.await.unwrap();
    // Each record went once, whichever purge got it.
    assert_eq!(i64::try_from(purged.records).unwrap(), 3000 - kept);
    assert!(purged.companies > 0, "{purged:?}");
    let left: i64 = c
        .query_one("SELECT count(*) FROM records", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, kept);
    while s.fold_company_graph(500).await.unwrap().more {}
    let (want_stats, want_pairs) = from_scratch(&c).await;
    let (stats, pairs) = stored(&c).await;
    assert!(!want_pairs.is_empty());
    assert_eq!(pairs, want_pairs);
    assert_eq!(stats, want_stats);
    let (want, got) = totals(&c).await;
    assert_eq!(got, want);
    let stale: i64 = c
        .query_one(
            "SELECT count(*) FROM company_stats s WHERE NOT EXISTS \
             (SELECT 1 FROM nodes n WHERE n.id = s.company_id)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stale, 0);
    db.drop_schema().await;
}

/// Issue #181: the upgrade to schema 13 counts the total as folded so far, so the folds after
/// it keep it exact, though the log still holds rows of documents folded before (and churned
/// since) and of documents never folded.
#[tokio::test]
async fn schema_13_counts_the_total_with_log_rows_pending() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let c = db.client().await;
    {
        let s = db.store().await;
        c.batch_execute(&seed(3000, 300, false)).await.unwrap();
        // Fold a part as schema 12 would (the total's row aside), then go back to schema 12
        // (schema 14's table and schema 15's index go too, so 13 to 15 run again).
        s.fold_company_graph(2000).await.unwrap();
    }
    c.batch_execute(
        // The schemas after 13 go too (issue #166's `document_texts`, #168's index, #169's
        // `similar_keys`, #193's `payments`, #194's `document_facts`, #198's `search_facts`;
        // schema 19 left schema 17's search vector), so 13 runs again.
        "DROP TABLE document_texts; DROP TABLE similar_keys; DROP TABLE payments; \
         DROP TABLE document_facts; DROP TABLE search_facts; \
         ALTER TABLE search_index DROP COLUMN has_facts; \
         DROP TABLE company_graph_totals; \
         DROP INDEX locations_by_document; DELETE FROM doewood_schema WHERE version >= 13;",
    )
    .await
    .unwrap();
    c.batch_execute(CHURN).await.unwrap();
    c.batch_execute(RETRACT_DOCS).await.unwrap();
    let pending: i64 = c
        .query_one("SELECT count(*) FROM company_graph_log", &[])
        .await
        .unwrap()
        .get(0);
    assert!(pending > 1000, "{pending}");
    let s = db.store().await;
    while s.fold_company_graph(500).await.unwrap().more {}
    let (want, got) = totals(&c).await;
    assert_eq!(got, want);
    assert_eq!(
        s.company_documents_total().await.unwrap(),
        u64::try_from(want).unwrap()
    );
    assert_eq!(stored(&c).await, from_scratch(&c).await);
    db.drop_schema().await;
}

/// `DOEWOOD_TEST_POSTGRES_URL=... cargo test -p doewood-pg --lib explorer_timings --
/// --ignored --nocapture`
#[tokio::test]
#[ignore = "a measurement (issue #172); run by hand"]
async fn explorer_timings() {
    let Some(url) = test_url() else { return };
    let var = |name: &str, default: u32| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (n, companies) = (
        var("DOEWOOD_BENCH_DOCS", 200_000),
        var("DOEWOOD_BENCH_COMPANIES", 20_000),
    );
    let db = Scratch::new(&url).await;
    let s = db.store().await;
    let c = db.client().await;
    let t = Instant::now();
    c.batch_execute(&seed(n, companies, true)).await.unwrap();
    println!(
        "Postgres: {n} documents, {companies} companies (seeded in {:.1} s); median of 9 runs",
        t.elapsed().as_secs_f64()
    );
    let id = async |key: String| -> Uuid {
        c.query_one(
            "SELECT id FROM nodes WHERE kind = 'company' AND key = $1",
            &[&key],
        )
        .await
        .unwrap()
        .get(0)
    };
    let hub = id("co-1".to_owned()).await;
    let from = id(format!("co-{}", companies - 1)).await;
    let to = id(format!("co-{}", companies - 2)).await;
    doewood_model::conformance::explorer_timings(&s, hub, from, to, 9).await;
    db.drop_schema().await;
}
