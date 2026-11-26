//! `sketch_band_sizes` since schema 10 (issue #141): a row only for a bucket of two or more
//! documents, with the bucket's exact count, also when documents add the same new band at
//! the same moment.

use std::sync::Arc;

use doewood_model::{Blake3, RecordStore, SketchStore, StepVersions};
use serde_json::value::RawValue;

use super::{Scratch, test_url};

fn hash(n: u8) -> Blake3 {
    Blake3([n; 32])
}

/// The size rows, by band.
async fn sizes(c: &tokio_postgres::Client) -> Vec<(i64, i32)> {
    c.query(
        "SELECT band, docs FROM sketch_band_sizes ORDER BY band",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

/// The buckets of two or more documents, counted from `sketch_bands`.
async fn counted(c: &tokio_postgres::Client) -> Vec<(i64, i32)> {
    c.query(
        "SELECT band, count(*)::int FROM sketch_bands GROUP BY band HAVING count(*) > 1 \
         ORDER BY band",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

#[tokio::test]
async fn only_shared_buckets_have_a_size() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let s = Arc::new(db.store().await);
    let c = db.client().await;
    let body = RawValue::from_string("{}".to_owned()).unwrap();
    let versions = StepVersions::default();
    for n in 0..48 {
        s.put_record(&hash(n), &body, &versions).await.unwrap();
    }
    // One document: no size rows.
    s.put_sketch(&hash(0), &[0], &[1, 2, 3]).await.unwrap();
    assert_eq!(sizes(&c).await, []);
    // A second document in band 2: its bucket's row, with both.
    s.put_sketch(&hash(1), &[1], &[2, 4]).await.unwrap();
    assert_eq!(sizes(&c).await, [(2, 2)]);
    s.put_sketch(&hash(2), &[2], &[2, 4, 5]).await.unwrap();
    assert_eq!(sizes(&c).await, [(2, 3), (4, 2)]);
    // A replaced sketch leaves its old buckets; a bucket back to one keeps its row.
    s.put_sketch(&hash(2), &[2], &[5]).await.unwrap();
    assert_eq!(sizes(&c).await, [(2, 2), (4, 1)]);
    // Many documents adding the same new bands at once: each bucket is counted exactly.
    let writes = (10..48).map(|n| {
        let s = Arc::clone(&s);
        tokio::spawn(async move {
            s.put_sketch(
                &hash(n),
                &[1],
                &[100, 200 + u64::from(n % 3), 300 + u64::from(n)],
            )
            .await
            .unwrap();
        })
    });
    for w in futures_util::future::join_all(writes).await {
        w.unwrap();
    }
    let got = sizes(&c).await;
    assert!(got.contains(&(100, 38)), "{got:?}");
    assert_eq!(
        got.iter()
            .filter(|(b, _)| *b >= 100)
            .copied()
            .collect::<Vec<_>>(),
        counted(&c)
            .await
            .into_iter()
            .filter(|(b, _)| *b >= 100)
            .collect::<Vec<_>>()
    );
    // A sketch of more than 64 bands takes the lock of the whole class: the same counts.
    let wide: Vec<u64> = (100..300).collect();
    s.put_sketch(&hash(5), &[5], &wide).await.unwrap();
    assert!(sizes(&c).await.contains(&(100, 39)));
    // Deleting the records cascades to their bands and the sizes follow.
    c.execute(
        "DELETE FROM records WHERE blake3 = ANY($1)",
        &[&(10..30)
            .map(|n| hash(n).as_bytes().to_vec())
            .collect::<Vec<_>>()],
    )
    .await
    .unwrap();
    let got = sizes(&c).await;
    assert!(got.contains(&(100, 19)), "{got:?}");
    for (band, docs) in counted(&c).await {
        assert!(got.contains(&(band, docs)), "{band}: {docs} in {got:?}");
    }
    db.drop_schema().await;
}
