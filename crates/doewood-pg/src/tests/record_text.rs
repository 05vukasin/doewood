//! Records stored as text (schema 8, issue #121): what the API reads is byte for byte what it
//! read when records were jsonb, for new rows, for rows from before, and after their
//! conversion.

use doewood_model::canonical::to_canonical;
use doewood_model::{Blake3, PutOutcome, RecordStore, SourceKey, StepVersions};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{Scratch, test_url};

const ENDPOINT: &str = "http://s3.example:9000";

/// Varied records: every JSON type, numbers at the edges of their kinds, strings that need
/// escapes or are outside ASCII, keys whose orders differ (bytes, jsonb's length-first order),
/// deep nesting and a long text, and a record shaped like quail's.
fn corpus() -> Vec<Value> {
    let mut deep = json!("bottom");
    for i in 0..64 {
        deep = if i % 2 == 0 {
            json!([deep])
        } else {
            json!({ "k": deep })
        };
    }
    vec![
        json!({}),
        json!([]),
        json!(null),
        json!(true),
        json!("just a string"),
        json!(0),
        json!(-1),
        json!(u64::MAX),
        json!(i64::MIN),
        json!(0.1),
        json!(1.0),
        json!(2.5),
        json!(-123.456),
        json!(1e300),
        json!(1.5e-7),
        json!(f64::MIN_POSITIVE),
        json!({"z": 1, "a": {"y": [1, 2, {"b": null, "a": false}]}, "m": [], "o": {}}),
        json!({"é": 1, "e": 2, "E": 3, "": 4, "a b": 5, "aa": 6, "b": 7}),
        json!({"text": "Ćevapi ž 東京 😀 \"quoted\" back\\slash /slash \n\t\r \u{1} \u{1f} \u{7f} \u{2028} \\u0000 as text"}),
        json!({"long": "Lorem ipsum dolor sit amet. ".repeat(4000)}),
        deep,
        json!({
            "title": "INVOICE No. 7/2026",
            "format": "pdf",
            "facts": [
                {"key": "signal.email_domain", "src": {"page": 1}, "conf": 0.9,
                 "value": {"t": "text", "v": "alpha-trade.example"}},
                {"key": "signal.amount", "src": {"page": 1}, "conf": 0.75,
                 "value": {"t": "money", "v": 4200.5, "cur": "RSD"}},
                {"key": "summary.keywords", "value": {"t": "list", "v": ["pallets", "contract"]}}
            ],
            "timings": {"parse_ms": 3, "ocr_ms": 0}
        }),
    ]
}

fn hash(i: usize, form: u8) -> Blake3 {
    let mut b = [0u8; 32];
    b[..8].copy_from_slice(&(i as u64).to_be_bytes());
    b[8] = form;
    Blake3::from_slice(&b).unwrap()
}

/// A document and a location in [`ENDPOINT`]/`b` for `h`, so `list_records` returns it.
async fn locate(c: &tokio_postgres::Client, h: &Blake3, key: &str) {
    let (doc, loc) = (Uuid::now_v7(), Uuid::now_v7());
    c.execute(
        "INSERT INTO nodes (id, kind, key) VALUES ($1, 'document', encode($2, 'hex'))",
        &[&doc, &h.as_bytes().as_slice()],
    )
    .await
    .unwrap();
    c.execute(
        "INSERT INTO nodes (id, kind, key) VALUES ($1, 'location', $2)",
        &[&loc, &key],
    )
    .await
    .unwrap();
    c.execute(
        "INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, object_key) \
         VALUES ($1, $2, $3, $4, 'b', $5)",
        &[&loc, &doc, &h.as_bytes().as_slice(), &ENDPOINT, &key],
    )
    .await
    .unwrap();
}

async fn facts(store: &crate::PgStore, hashes: &[Blake3]) -> Vec<String> {
    store
        .fact_texts(hashes, "signal.email_domain")
        .await
        .unwrap()
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn records_read_as_they_did_as_jsonb() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let store = db.store().await;
    let c = db.client().await;
    let versions = StepVersions::from([("quail".to_owned(), "1".to_owned())]);
    let corpus = corpus();
    let texts: Vec<String> = corpus
        .iter()
        .map(|v| to_canonical(v).unwrap().get().to_owned())
        .collect();
    for (i, text) in texts.iter().enumerate() {
        let body = serde_json::value::RawValue::from_string(text.clone()).unwrap();
        let (new, old) = (hash(i, 0), hash(i, 1));
        assert_eq!(
            store.put_record(&new, &body, &versions).await.unwrap(),
            PutOutcome::Inserted
        );
        // A row as schema 7 wrote it: the same text, parsed into jsonb on the server.
        c.execute(
            "INSERT INTO records (blake3, record, versions) \
             VALUES ($1, $2::text::jsonb, '{\"quail\":\"1\"}')",
            &[&old.as_bytes().as_slice(), &text],
        )
        .await
        .unwrap();
        locate(&c, &new, &format!("new/{i}")).await;
        locate(&c, &old, &format!("old/{i}")).await;
        // What the code before schema 8 returned: the jsonb as a `Value`, written canonically.
        let before: Value = c
            .query_one(
                "SELECT record FROM records WHERE blake3 = $1",
                &[&old.as_bytes().as_slice()],
            )
            .await
            .unwrap()
            .try_get(0)
            .unwrap_or_else(|e| panic!("record {i}: {e:?}"));
        let before = to_canonical(&before).unwrap();
        assert_eq!(before.get(), text, "record {i}");
        for h in [new, old] {
            let got = store.get_record(&h).await.unwrap().unwrap();
            assert_eq!(got.body.get(), text, "record {i}");
        }
        // Unchanged against a row from before too: it is compared as jsonb.
        for h in [new, old] {
            assert_eq!(
                store.put_record(&h, &body, &versions).await.unwrap(),
                PutOutcome::Unchanged,
                "record {i}"
            );
        }
    }
    let source = SourceKey::new(ENDPOINT, "b", "").unwrap();
    let list = async || {
        let page = store
            .list_records(&source, None, 1000, usize::MAX)
            .await
            .unwrap();
        page.into_iter()
            .map(|r| (r.blake3, r.body.get().to_owned(), r.updated_at))
            .collect::<Vec<_>>()
    };
    let listed = list().await;
    assert_eq!(listed.len(), 2 * texts.len());
    for (h, body, _) in &listed {
        let i = usize::try_from(u64::from_be_bytes(h.as_bytes()[..8].try_into().unwrap())).unwrap();
        assert_eq!(body, &texts[i]);
    }
    let quail_like = texts.len() - 1;
    assert_eq!(
        facts(&store, &[hash(quail_like, 0)]).await,
        ["alpha-trade.example"]
    );
    assert_eq!(
        facts(&store, &[hash(quail_like, 1)]).await,
        ["alpha-trade.example"]
    );

    // Where jsonb did not give the text back, the text is now returned as written (as SQLite
    // does): jsonb's numeric has no -0.0 (it read as 0.0), and f64::MAX came back as 309
    // digits that serde_json could not read.
    for (i, v) in [json!(-0.0), json!(f64::MAX), json!({"x": [-0.0, f64::MAX]})]
        .iter()
        .enumerate()
    {
        let body = to_canonical(v).unwrap();
        let h = hash(2000 + i, 0);
        store.put_record(&h, &body, &versions).await.unwrap();
        let got = store.get_record(&h).await.unwrap().unwrap();
        assert_eq!(got.body.get(), body.get());
    }

    // The conversion: every row from before gets its text, `updated_at` stays.
    assert_eq!(
        store.convert_records().await.unwrap(),
        u64::try_from(texts.len()).unwrap()
    );
    let left: i64 = c
        .query_one(
            "SELECT count(*) FROM records WHERE body IS NULL OR record IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0);
    assert_eq!(list().await, listed);
    assert_eq!(store.convert_records().await.unwrap(), 0);
    assert_eq!(
        facts(&store, &[hash(quail_like, 1)]).await,
        ["alpha-trade.example"]
    );

    // A changed record replaces a row from before (jsonb) with its text.
    let changed = hash(1000, 1);
    c.execute(
        "INSERT INTO records (blake3, record) VALUES ($1, '{\"a\": 1}')",
        &[&changed.as_bytes().as_slice()],
    )
    .await
    .unwrap();
    let body = serde_json::value::RawValue::from_string(r#"{"a":2}"#.to_owned()).unwrap();
    assert_eq!(
        store.put_record(&changed, &body, &versions).await.unwrap(),
        PutOutcome::Updated
    );
    let row = c
        .query_one(
            "SELECT body, record IS NULL FROM records WHERE blake3 = $1",
            &[&changed.as_bytes().as_slice()],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), r#"{"a":2}"#);
    assert!(row.get::<_, bool>(1));
    db.drop_schema().await;
}

/// jsonb cannot hold U+0000, so such a record is refused as it was when records were jsonb;
/// the same six characters as text (an escaped backslash) are kept.
#[tokio::test]
async fn a_record_with_u0000_is_refused_as_before() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let store = db.store().await;
    let versions = StepVersions::new();
    let nul = to_canonical(&json!({"t": "a\u{0}b"})).unwrap();
    assert_eq!(nul.get(), r#"{"t":"a\u0000b"}"#);
    let err = store
        .put_record(&hash(1, 0), &nul, &versions)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("22P05"), "{err}");
    for text in [r#"{"t":"a\\u0000b"}"#, r#"{"t":"\\\\u0000"}"#] {
        let body = serde_json::value::RawValue::from_string(text.to_owned()).unwrap();
        store
            .put_record(&hash(2, 0), &body, &versions)
            .await
            .unwrap();
        let back = store.get_record(&hash(2, 0)).await.unwrap().unwrap();
        assert_eq!(back.body.get(), text);
    }
    db.drop_schema().await;
}

/// A database at schema 7 with records keeps them through the migration; a start converts
/// them in the background.
#[tokio::test]
async fn schema_8_converts_records_from_before() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let text = to_canonical(&json!({"b": [1, 2.5, "ž"], "a": null})).unwrap();
    {
        let client = db.client().await;
        client
            .batch_execute(
                "CREATE TABLE doewood_schema (version integer PRIMARY KEY, \
                 applied_at timestamptz NOT NULL DEFAULT now())",
            )
            .await
            .unwrap();
        for (version, sql) in (1_i32..).zip(&crate::migrate::MIGRATIONS[..7]) {
            client.batch_execute(sql).await.unwrap();
            client
                .execute(
                    "INSERT INTO doewood_schema (version) VALUES ($1)",
                    &[&version],
                )
                .await
                .unwrap();
        }
        // As schema 7's `put_record` wrote it: the text, parsed by the server.
        client
            .execute(
                "INSERT INTO records (blake3, record) \
                 SELECT sha256(int4send(i)), $1::text::jsonb FROM generate_series(1, 250) i",
                &[&text.get()],
            )
            .await
            .unwrap();
    }
    let store = db.store().await;
    let c = db.client().await;
    let h = Blake3::from_slice(
        &c.query_one("SELECT sha256(int4send(7))", &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
    )
    .unwrap();
    assert_eq!(
        store.get_record(&h).await.unwrap().unwrap().body.get(),
        text.get()
    );
    // The start's own conversion may have done some or all of it already.
    store.convert_records().await.unwrap();
    let (left, texts): (i64, i64) = {
        let r = c
            .query_one(
                "SELECT count(*) FILTER (WHERE body IS NULL OR record IS NOT NULL), \
                 count(*) FILTER (WHERE body = $1) FROM records",
                &[&text.get()],
            )
            .await
            .unwrap();
        (r.get(0), r.get(1))
    };
    assert_eq!((left, texts), (0, 250));
    assert_eq!(
        store.get_record(&h).await.unwrap().unwrap().body.get(),
        text.get()
    );
    db.drop_schema().await;
}
