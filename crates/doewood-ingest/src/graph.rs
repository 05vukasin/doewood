//! A record's facts into the shared graph and search (ADR 0001 section 7, ADR 0002
//! section 1).
//!
//! - **Document and Location** through `upsert_location`; the document's props hold its format,
//!   title, kind and own numbers.
//! - **Companies** by the deterministic match ladder: validated identifiers first, then the
//!   normalized name with its legal form, the squashed name, and the legal-form-blind name
//!   while exactly one company owns it. Each key is a node alias, so a lookup is one read and
//!   the first claim wins; an identifier another company owns is refused, never moved. Every
//!   match keeps its step, confidence and evidence on the `party` edge, and only there (issue
//!   #129). Fuzzy steps are not taken here: nothing merges on a fuzzy match.
//! - **Merges** (ADR 0002 section 1.8): ingest never merges two company nodes. When a party's
//!   identifier and its name point at different companies, the identifier's company gets the
//!   party: two conflicting high-precision hits are for review, never an automatic merge.
//!   Merges come from enrichment or a person, through the merge log. A match never lands on a
//!   merged-away company: it follows `merged_into` at match time, and again atomically with
//!   the edge write, so a merge that commits in between sends it to the survivor (issue
//!   #48). A document split off a company with a dismissal is not attached back to it: a
//!   dismissal is permanent.
//! - **References**: a cited number resolves to the document that has it as its own number
//!   (an alias `number:<key>`); when that document is not known yet, the edge goes to a pending
//!   Document node `ref:<key>`, and the edges move to the real document once it arrives.
//! - **`similar_to`**: candidates from the LSH band index and the exact-match key index
//!   (issue #169), each relation confirmed by its threshold ([`Similar`]): identical, near
//!   copy, related version, same template, same origin. One edge per pair, from the smaller
//!   hash to the larger, its props naming the relations that hold.
//! - **Search**: title, the summary card and keywords as the body, kind, date and amount.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use doewood_model::{
    Backend, Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, NeighbourQuery, NodeKind,
    ObjectLocation, PENDING_REF_PREFIX, PropsPatch, Result, SearchDoc, SimilarEdge, SimilarKeyKind,
    SimilarRelation, merged_into,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::debug;
use uuid::Uuid;

use crate::company::{AliasKind, CompanyName, LadderStep};
use crate::facts::{DocFacts, Party, SimilarityFacts};

/// How `similar_to` candidates are found. The thresholds and the largest band bucket are
/// runtime settings ([`Similar`]), passed to [`GraphWriter::write`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimilarityConfig {
    /// Most candidates looked at per document from the band index, and per key from the key
    /// index.
    pub max_candidates: u32,
}

impl Default for SimilarityConfig {
    fn default() -> Self {
        Self {
            max_candidates: 100,
        }
    }
}

/// The runtime `similar_to` settings ([`doewood_store::IngestSettings`]), read for every
/// file and passed to [`GraphWriter::write`]. A bare `f64` is the least Jaccard estimate with
/// the other settings at their defaults.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Similar {
    /// Least Jaccard estimate for an edge: a related version (`similar_min_jaccard`).
    pub min_jaccard: f64,
    /// Least Jaccard estimate of a near copy (`similar_near_copy_min_jaccard`, issue #169).
    pub near_copy_min_jaccard: f64,
    /// Least masked similarity of one template (`similar_template_min_masked`).
    pub template_min_masked: f64,
    /// Shortest identical text, in characters (`similar_identical_min_chars`).
    pub identical_min_chars: u32,
    /// Largest band bucket the candidate query reads (`similar_max_bucket`, issue #148).
    pub max_bucket: u32,
}

impl From<f64> for Similar {
    fn from(min_jaccard: f64) -> Self {
        Self {
            min_jaccard,
            ..Self::from(doewood_store::IngestSettings::default())
        }
    }
}

impl From<doewood_store::IngestSettings> for Similar {
    fn from(s: doewood_store::IngestSettings) -> Self {
        Self {
            min_jaccard: s.similar_min_jaccard,
            near_copy_min_jaccard: s.similar_near_copy_min_jaccard,
            template_min_masked: s.similar_template_min_masked,
            identical_min_chars: s.similar_identical_min_chars,
            max_bucket: s.similar_max_bucket,
        }
    }
}

impl Similar {
    /// The relations between a document and one candidate, as a `similar_to` edge's props
    /// ([`SimilarEdge`]); `None` when none holds. `sig` and `own` are the document's
    /// signature and similarity facts.
    fn relate(&self, sig: Option<&[u8]>, own: &SimilarityFacts, c: &Found) -> Option<SimilarEdge> {
        let raw = match (sig, c.signature.as_deref()) {
            (Some(a), Some(b)) => Some(quail_sketch::jaccard_estimate(a, b)),
            _ => None,
        };
        let masked = match (own.masked, c.masked) {
            (Some(a), Some(b)) => Some(doewood_model::similar::masked_similarity(a, b)),
            _ => None,
        };
        let mut e = SimilarEdge {
            raw_similarity: raw,
            masked_similarity: masked,
            shared_bands: c.shared_bands,
            ..SimilarEdge::default()
        };
        // One text relation, the strongest. The same text key is checked against the
        // signatures when both are there (the key is 64 bits of quail's 128).
        let long = own
            .text_chars
            .is_some_and(|n| n >= u64::from(self.identical_min_chars));
        if c.text && long && raw.is_none_or(|r| r >= 1.0) {
            e.relations.push(SimilarRelation::Identical);
            e.chars = own.text_chars;
        } else if let Some(r) = raw {
            if r >= self.near_copy_min_jaccard {
                e.relations.push(SimilarRelation::NearCopy);
            } else if r >= self.min_jaccard {
                e.relations.push(SimilarRelation::RelatedVersion);
            }
        }
        if let (Some(kind), Some(m)) = (c.template, masked)
            && m >= self.template_min_masked
        {
            e.relations.push(SimilarRelation::SameTemplate);
            e.template = Some(kind);
        }
        if let Some(kind) = c.origin {
            e.relations.push(SimilarRelation::SameOrigin);
            e.origin = Some(kind);
        }
        (!e.relations.is_empty()).then_some(e)
    }
}

/// A candidate of [`GraphWriter::similar`], from the band index, the key index or both.
#[derive(Debug, Default)]
struct Found {
    signature: Option<Vec<u8>>,
    shared_bands: Option<u32>,
    masked: Option<u64>,
    /// The same text key.
    text: bool,
    /// The first shared template key.
    template: Option<SimilarKeyKind>,
    /// The first shared origin id.
    origin: Option<SimilarKeyKind>,
}

/// Most names and identifiers per kind a company's props keep.
const MAX_NAMES: usize = 20;
const MAX_IDS: usize = 10;
/// Most mentions cited in a party edge's evidence.
const MAX_MENTIONS: usize = 5;
/// Most `merged_into` hops followed from a merged-away company.
const MAX_HOPS: usize = 64;

/// What one document added.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// `party` edges.
    pub parties: usize,
    /// Companies created.
    pub new_companies: usize,
    /// `amends` / `cites` edges (pending ones included).
    pub refs: usize,
    /// Pending references created or used.
    pub pending_refs: usize,
    /// `similar_to` edges.
    pub similar: usize,
    /// Band keys of the document skipped because their bucket held more than
    /// `similar_max_bucket` documents (issue #148).
    pub similar_buckets_skipped: usize,
    /// `party` edges that landed on the survivor of a merge that committed after the
    /// company was matched (issue #48).
    pub redirects: usize,
    /// Time spent waiting for the instance's party locks ([`GraphWriter`], issue #78).
    pub company_lock_wait: Duration,
    /// Time the party locks were held (party matching and alias writes).
    pub company_lock_hold: Duration,
}

/// How a party matched a company.
#[derive(Clone, Debug, PartialEq)]
pub struct Match {
    /// The company.
    pub company: Uuid,
    /// The ladder step; `None` for a company created for it.
    pub step: Option<LadderStep>,
    /// The alias that matched.
    pub alias: String,
    /// The step's reliability.
    pub confidence: f32,
    /// Identifiers that point at other companies (to review; never merged).
    pub conflicts: Vec<String>,
    /// The company the ladder found, when a dismissal sent the party elsewhere: the document
    /// was split off that company and the pair dismissed, so it is not attached back.
    pub dismissed: Option<Uuid>,
}

/// Stripes of [`PartyLocks`]. More stripes, fewer unrelated parties that share one.
const PARTY_STRIPES: u64 = 1024;

/// The locks of company resolution within the instance (issue #78). Across instances alias
/// claims and `ensure_node` decide, as before.
///
/// A document's parties take one striped lock per key they can read or write: each party's
/// identifier aliases and ladder keys (the form-blind key included) and the learned aliases
/// it claims, deduplicated and in stripe order (no deadlock), together with `all` shared,
/// for the whole of [`GraphWriter::parties`]' matching, as the one instance-wide lock was
/// held before. Two parties that share a key run one after the other; unrelated ones run
/// at the same time.
///
/// Why that keeps ADR 0002 section 1 (first claim wins, form-blind blocking, dismissals) as
/// it was with the one lock:
///
/// - The keys a party reads (`aliases`, `node_by_key`) and writes (`claim_aliases`,
///   `block_alias`, `ensure_node`, its learned aliases) are all known before its match: they
///   follow from the facts alone, not from what the store holds. The company `create` makes
///   is keyed by one of them. So every alias decision is made under its key's lock, and
///   any two parties whose reads and writes of aliases overlap are serialized.
/// - The other things matching reads are not written by party resolution: `merged_into`
///   (the walk in `live`) and dismissals come from merges and splits, which never took this
///   lock and also come from other instances; `upsert_live_edge` resolves `merged_into`
///   again atomically. The document's own `party` edges (`attached`) are written after the
///   locks are released, as before.
/// - One read is of a company's props that another party may write without sharing a key:
///   the form-blind step reads the owner's `legal_form`, and any party matched to that
///   company (by an identifier, say) sets `legal_form` when it is absent. A `legal_form`
///   that is set never changes here (`set_if_absent`), so a decision on a set one is stable.
///   A decision on an absent one is not, so a party that has a legal form and finds the
///   form-blind key's owner without one gives up its stripes and takes `all` exclusively
///   (no other party of the instance runs), then matches again from the start. Nothing was
///   written for that party yet; the parties before it in the document were written under
///   their keys' locks.
struct PartyLocks {
    /// Shared by every party set, exclusive for the form-blind case above.
    all: RwLock<()>,
    stripes: Vec<Mutex<()>>,
}

/// The locks a document's parties hold.
enum PartyGuard<'a> {
    Keys {
        _all: RwLockReadGuard<'a, ()>,
        _stripes: Vec<MutexGuard<'a, ()>>,
    },
    All {
        _all: RwLockWriteGuard<'a, ()>,
    },
}

impl PartyGuard<'_> {
    fn exclusive(&self) -> bool {
        matches!(self, Self::All { .. })
    }
}

impl PartyLocks {
    fn new() -> Self {
        Self {
            all: RwLock::new(()),
            stripes: (0..PARTY_STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }

    /// The stripe of a key (`DefaultHasher::new` has fixed keys: a key always has one stripe).
    fn stripe(key: &str) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        usize::try_from(h.finish() % PARTY_STRIPES).unwrap_or(0)
    }

    /// `all` shared, then the stripes of `keys` in order, each once.
    async fn keys<'k>(&self, keys: impl IntoIterator<Item = &'k str>) -> PartyGuard<'_> {
        let mut stripes: Vec<usize> = keys.into_iter().map(Self::stripe).collect();
        stripes.sort_unstable();
        stripes.dedup();
        let all = self.all.read().await;
        let mut held = Vec::with_capacity(stripes.len());
        for i in stripes {
            held.push(self.stripes[i].lock().await);
        }
        PartyGuard::Keys {
            _all: all,
            _stripes: held,
        }
    }

    /// `all` exclusively: no other party of the instance runs.
    async fn all(&self) -> PartyGuard<'_> {
        PartyGuard::All {
            _all: self.all.write().await,
        }
    }
}

/// Writes documents into a backend. Company resolution is serialized within the instance
/// per alias key ([`PartyLocks`]).
pub struct GraphWriter {
    backend: Arc<dyn Backend>,
    similarity: SimilarityConfig,
    locks: PartyLocks,
}

impl std::fmt::Debug for GraphWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphWriter")
            .field("backend", &self.backend.name())
            .field("similarity", &self.similarity)
            .finish_non_exhaustive()
    }
}

impl GraphWriter {
    /// Writes to `backend`.
    pub fn new(backend: Arc<dyn Backend>, similarity: SimilarityConfig) -> Self {
        Self {
            backend,
            similarity,
            locks: PartyLocks::new(),
        }
    }

    /// The backend written to.
    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    /// Writes one document found at `location`. Its record must already be stored.
    /// `name` is the file name, the title of last resort. `similar` holds the
    /// `similar_to` thresholds and the largest band bucket read ([`Similar`]).
    pub async fn write(
        &self,
        blake3: &Blake3,
        location: &ObjectLocation,
        facts: &DocFacts,
        name: &str,
        similar_settings: impl Into<Similar>,
    ) -> Result<WriteStats> {
        let similar_settings = similar_settings.into();
        let b = &self.backend;
        let hex = blake3.to_hex();
        let doc = b.upsert_location(blake3, location).await?.document;
        let title = facts.title.clone().unwrap_or_else(|| name.to_owned());
        // Only this document's own keys: whatever else is on the node stays.
        let patch = PropsPatch::new()
            .set(&["format"], json!(facts.format))
            .set(&["title"], json!(title))
            .set(&["kind"], json!(facts.kind))
            .set(&["numbers"], json!(facts.numbers));
        let search = SearchDoc {
            blake3: *blake3,
            title,
            body: facts.body.clone(),
            kind: facts.kind.clone(),
            date: facts.date,
            amount: facts.amount,
        };
        // Independent steps run together (issue #103), so their round trips overlap: the
        // document's props and search row, which nothing else here reads; its references
        // (its own numbers first, then what it cites, as before); its parties; and its
        // similar documents. Each step keeps its own order. All of them finish before the
        // first error, in this order, is returned.
        let (mut stats, mut refs, mut similar) = Default::default();
        let (props, indexed, refs_done, parties, similar_done) = tokio::join!(
            b.update_props(doc, &patch),
            b.index_document(&search),
            async {
                self.numbers(doc, &hex, facts, &mut refs).await?;
                self.cites(doc, &hex, facts, &mut refs).await
            },
            self.parties(doc, &hex, facts, &mut stats),
            self.similar(blake3, doc, facts, similar_settings, &mut similar),
        );
        props?;
        indexed?;
        refs_done?;
        parties?;
        similar_done?;
        let WriteStats {
            refs: cited,
            pending_refs,
            ..
        } = refs;
        stats.refs += cited;
        stats.pending_refs += pending_refs;
        stats.similar += similar.similar;
        stats.similar_buckets_skipped += similar.similar_buckets_skipped;
        Ok(stats)
    }

    /// The document's own numbers become its aliases; pending references to them move here.
    async fn numbers(
        &self,
        doc: Uuid,
        hex: &str,
        facts: &DocFacts,
        stats: &mut WriteStats,
    ) -> Result<()> {
        let b = &self.backend;
        let claims: Vec<(String, Value)> = facts
            .numbers
            .iter()
            .map(|n| (format!("number:{n}"), json!({"document": hex})))
            .collect();
        b.claim_aliases(NodeKind::Document, doc, &claims).await?;
        for n in &facts.numbers {
            let Some(pending) = b
                .node_by_key(NodeKind::Document, &format!("{PENDING_REF_PREFIX}{n}"))
                .await?
            else {
                continue;
            };
            let incoming = b
                .neighbours(
                    pending.id,
                    &NeighbourQuery {
                        direction: Direction::In,
                        ..NeighbourQuery::default()
                    },
                )
                .await?;
            for nb in incoming {
                let old = nb.edge.edge;
                if old.src == doc {
                    continue;
                }
                let mut moved = old.clone();
                moved.dst = doc;
                if let Some(p) = moved.props.as_object_mut() {
                    p.remove("pending");
                }
                b.upsert_edge(&moved).await?;
                b.upsert_edge(&old.with_state(EdgeState::Superseded))
                    .await?;
                stats.pending_refs += 1;
            }
        }
        Ok(())
    }

    async fn cites(
        &self,
        doc: Uuid,
        hex: &str,
        facts: &DocFacts,
        stats: &mut WriteStats,
    ) -> Result<()> {
        let b = &self.backend;
        // Its own numbers, cited in its own header, are left out.
        let cites: Vec<_> = facts
            .cites
            .iter()
            .filter(|c| !facts.numbers.contains(&c.key))
            .collect();
        let keys: Vec<String> = cites.iter().map(|c| format!("number:{}", c.key)).collect();
        let targets = b.aliases(NodeKind::Document, &keys).await?;
        for (c, target) in cites.into_iter().zip(targets) {
            let kind = if c.relation == "amends" {
                EdgeKind::Amends
            } else {
                EdgeKind::Cites
            };
            let mut props =
                json!({"relation": c.relation, "raw": c.raw, "key": c.key, "kind": c.kind});
            let target = match target.and_then(|a| a.node) {
                Some(t) if t != doc => t,
                Some(_) => continue,
                None => {
                    props["pending"] = Value::Bool(true);
                    stats.pending_refs += 1;
                    b.upsert_node(
                        NodeKind::Document,
                        &format!("{PENDING_REF_PREFIX}{}", c.key),
                        &json!({"pending": true, "key": c.key, "raw": c.raw, "kind": c.kind}),
                    )
                    .await?
                }
            };
            let edge = Edge::new(doc, target, kind, EdgeSource::Document)
                .with_props(props)
                .with_evidence(json!([{"document": hex, "raw": c.raw}]))
                .with_confidence(0.8);
            b.upsert_edge(&edge).await?;
            stats.refs += 1;
        }
        Ok(())
    }

    async fn similar(
        &self,
        blake3: &Blake3,
        doc: Uuid,
        facts: &DocFacts,
        settings: Similar,
        stats: &mut WriteStats,
    ) -> Result<()> {
        let keys = facts.similarity.keys();
        let sketch = facts
            .minhash
            .as_ref()
            .map(|(sig, bands)| (sig.as_slice(), bands.as_slice()));
        if sketch.is_none() && keys.keys.is_empty() {
            return Ok(());
        }
        let b = &self.backend;
        // The sketch and the keys (issue #169) are written, then read back, together: on
        // Postgres each pair is one round trip, as the sketch alone was.
        b.put_similarity(blake3, sketch, &keys).await?;
        // Bands whose bucket is larger than the setting are skipped (issue #148): on a
        // template-heavy corpus this changes which edges exist, and bounds the query.
        let bands = sketch.map_or(&[][..], |(_, bands)| bands);
        let candidates = b
            .similarity_candidates(
                bands,
                &keys.keys,
                blake3,
                self.similarity.max_candidates,
                settings.max_bucket,
            )
            .await?;
        stats.similar_buckets_skipped +=
            usize::try_from(candidates.skipped_bands).unwrap_or(usize::MAX);
        // Band candidates first, in their order; then those found only by a key, by hash.
        let mut order: Vec<Blake3> = Vec::new();
        let mut found: BTreeMap<Blake3, Found> = BTreeMap::new();
        for c in candidates.sketches {
            order.push(c.blake3);
            found.insert(
                c.blake3,
                Found {
                    signature: Some(c.signature),
                    shared_bands: Some(c.shared_bands),
                    ..Found::default()
                },
            );
        }
        for m in candidates.keyed {
            let f = found.entry(m.blake3).or_insert_with(|| {
                order.push(m.blake3);
                Found::default()
            });
            if f.signature.is_none() {
                f.signature = m.signature;
            }
            f.masked = f.masked.or(m.masked);
            match m.kind.relation() {
                SimilarRelation::Identical => f.text = true,
                SimilarRelation::SameTemplate => f.template = f.template.or(Some(m.kind)),
                _ => f.origin = f.origin.or(Some(m.kind)),
            }
        }
        let sig = sketch.map(|(sig, _)| sig);
        let related: Vec<(Blake3, SimilarEdge)> = order
            .into_iter()
            .filter_map(|h| {
                let e = settings.relate(sig, &facts.similarity, &found[&h])?;
                Some((h, e))
            })
            .collect();
        if related.is_empty() {
            return Ok(());
        }
        // Issue #126: the candidates' Document nodes in one read (the few missing ones are
        // created one by one), then every edge in one batch, instead of two round trips per
        // candidate.
        // Batches of at most `GRAPH_BATCH_MAX`: one, unless many keys each brought their
        // full 100 matches.
        for batch in related.chunks(doewood_model::GRAPH_BATCH_MAX) {
            let keys: Vec<String> = batch.iter().map(|(h, _)| h.to_hex()).collect();
            let ids = b.node_ids(NodeKind::Document, &keys).await?;
            let mut edges = Vec::with_capacity(batch.len());
            for ((h, e), (key, id)) in batch.iter().zip(keys.iter().zip(ids)) {
                let other = match id {
                    Some(id) => id,
                    None => b.ensure_node(NodeKind::Document, key).await?,
                };
                // One edge per pair, from the smaller hash to the larger.
                let (src, dst) = if blake3 < h {
                    (doc, other)
                } else {
                    (other, doc)
                };
                edges.push(
                    Edge::new(src, dst, EdgeKind::SimilarTo, EdgeSource::Document)
                        .with_props(e.to_props())
                        .with_confidence(e.confidence()),
                );
            }
            b.upsert_edges(&edges).await?;
            stats.similar += edges.len();
        }
        Ok(())
    }

    async fn parties(
        &self,
        doc: Uuid,
        hex: &str,
        facts: &DocFacts,
        stats: &mut WriteStats,
    ) -> Result<()> {
        // The parties with a name, each with the learned aliases it claims.
        let work: Vec<Work<'_>> = facts
            .parties
            .iter()
            .filter_map(|p| {
                let name = CompanyName::parse(&p.surface)?;
                let learned = learned_aliases(&name, facts, hex);
                Some((p, name, learned))
            })
            .collect();
        if work.is_empty() {
            return Ok(()); // nothing to match: no lock, no reads
        }
        let keys = lock_keys(&work);
        // One edge per company: candidates that resolve to the same one merge their roles.
        let mut edges: BTreeMap<Uuid, (Party, Match)> = BTreeMap::new();
        let waited = Instant::now();
        let mut guard = self.locks.keys(keys.iter().map(String::as_str)).await;
        let mut held = Instant::now();
        stats.company_lock_wait += held - waited;
        let attached = self.attached(doc).await?;
        for (p, name, learned) in &work {
            let (m, created) = loop {
                if let Some(r) = self
                    .resolve(name, p, hex, &attached, guard.exclusive())
                    .await?
                {
                    break r;
                }
                // The form-blind case of [`PartyLocks`]: match again with no other party
                // of the instance running.
                stats.company_lock_hold += held.elapsed();
                drop(guard);
                let waited = Instant::now();
                guard = self.locks.all().await;
                held = Instant::now();
                stats.company_lock_wait += held - waited;
            };
            if created {
                stats.new_companies += 1;
            }
            self.learn_aliases(m.company, learned).await?;
            add_edge(&mut edges, p, m);
        }
        drop(guard);
        stats.company_lock_hold += held.elapsed();
        for (company, (p, m)) in edges {
            let mentions: Vec<Value> = p
                .mentions
                .iter()
                .take(MAX_MENTIONS)
                .map(|(page, start, end)| json!({"page": page, "start": start, "end": end}))
                .collect();
            let ids: Vec<String> = p
                .ids
                .iter()
                .map(crate::company::Identifier::alias)
                .collect();
            let edge = Edge::new(doc, company, EdgeKind::Party, EdgeSource::Document)
                .with_props(json!({
                    "role": p.roles.first().map_or("party", String::as_str),
                    "roles": p.roles,
                    "surface": p.surface,
                    "match": {
                        "step": m.step.map_or("created", LadderStep::as_str),
                        "alias": m.alias,
                        "confidence": m.confidence,
                        "conflicts": m.conflicts,
                        "dismissed": m.dismissed.map(|d| d.to_string()),
                    },
                }))
                .with_evidence(json!([{
                    "document": hex,
                    "mentions": mentions,
                    "identifiers": ids,
                    "quail_key": p.quail_key,
                }]))
                .with_confidence(p.score);
            // Resolves `merged_into` in the write itself: a merge committed since the match
            // (on another instance) moves the edge to the survivor, never leaves it on a husk.
            let written = self.backend.upsert_live_edge(&edge).await?;
            if written.redirects > 0 {
                debug!(
                    document = %doc,
                    matched = %company,
                    company = %written.dst,
                    hops = written.redirects,
                    "party edge redirected to the company it was merged into"
                );
                stats.redirects += 1;
            }
            stats.parties += 1;
        }
        Ok(())
    }

    /// The companies the document has a live `party` edge to now.
    async fn attached(&self, doc: Uuid) -> Result<BTreeSet<Uuid>> {
        let query = NeighbourQuery {
            direction: Direction::Out,
            edge_kinds: vec![EdgeKind::Party],
            node_kind: Some(NodeKind::Company),
            ..NeighbourQuery::default()
        };
        Ok(self
            .backend
            .neighbours(doc, &query)
            .await?
            .into_iter()
            .map(|n| n.node.id)
            .collect())
    }

    /// The live company a company node stands for: itself, or the company it was merged
    /// into (ADR 0002 section 1.8), so nothing attaches to a merged-away husk.
    async fn live(&self, mut id: Uuid) -> Result<Uuid> {
        for _ in 0..MAX_HOPS {
            match self.backend.node(id).await?.as_ref().and_then(merged_into) {
                Some(next) => id = next,
                None => break,
            }
        }
        Ok(id)
    }

    /// A dismissal is permanent: when the ladder finds a company the document is not attached
    /// to, while it is attached to a company dismissed from that one (a person split it off),
    /// the party stays with the company it is attached to.
    async fn honour_dismissals(&self, m: &mut Match, attached: &BTreeSet<Uuid>) -> Result<()> {
        if attached.contains(&m.company) {
            return Ok(());
        }
        for other in attached {
            if self.backend.dismissal(m.company, *other).await?.is_some() {
                m.dismissed = Some(m.company);
                m.company = *other;
                return Ok(());
            }
        }
        Ok(())
    }

    /// The match ladder for one party. Returns the match and whether the company is new.
    /// `attached` are the companies the document is a party to now. `None` (nothing
    /// written) when the match depends on a company's absent `legal_form` and `exclusive` is
    /// not set: the caller takes the party locks exclusively and asks again ([`PartyLocks`]).
    async fn resolve(
        &self,
        name: &CompanyName,
        p: &Party,
        hex: &str,
        attached: &BTreeSet<Uuid>,
        exclusive: bool,
    ) -> Result<Option<(Match, bool)>> {
        let b = &self.backend;
        // Every key of the ladder in one read (issue #103): the identifiers, then the names.
        let ids: Vec<String> = p
            .ids
            .iter()
            .map(crate::company::Identifier::alias)
            .collect();
        let names = name.ladder_keys();
        let keys: Vec<String> = ids
            .iter()
            .cloned()
            .chain(names.iter().map(|(_, k)| k.clone()))
            .collect();
        let mut owners = b
            .aliases(NodeKind::Company, &keys)
            .await?
            .into_iter()
            .map(|a| a.and_then(|a| a.node));
        // 1. Identifiers, strongest first. Ones that point elsewhere are conflicts.
        let mut found: Option<Match> = None;
        for (alias, owner) in ids.into_iter().zip(owners.by_ref()) {
            let Some(owner) = owner else {
                continue;
            };
            match &mut found {
                None => {
                    found = Some(Match {
                        company: owner,
                        step: Some(LadderStep::Identifier),
                        alias,
                        confidence: LadderStep::Identifier.confidence(),
                        conflicts: Vec::new(),
                        dismissed: None,
                    });
                }
                Some(m) if m.company != owner => m.conflicts.push(alias),
                Some(_) => {}
            }
        }
        // 2. Names: normalized with the form, squashed, legal-form-blind.
        if found.is_none() {
            for ((step, key), owner) in names.into_iter().zip(owners) {
                let Some(owner) = owner else {
                    continue;
                };
                if step == LadderStep::FormBlind
                    && let Some(form) = name.legal_form
                    && let Some(node) = b.node(owner).await?
                {
                    let theirs = node.props["legal_form"].as_str().filter(|f| !f.is_empty());
                    if theirs.is_some_and(|f| f != form) {
                        continue; // "ACME d.o.o." is not "ACME a.d."
                    }
                    if theirs.is_none() && !exclusive {
                        // Another party matched to the owner may set its form meanwhile.
                        return Ok(None);
                    }
                }
                found = Some(Match {
                    company: owner,
                    step: Some(step),
                    alias: key,
                    confidence: step.confidence(),
                    conflicts: Vec::new(),
                    dismissed: None,
                });
                break;
            }
        }
        let (mut m, created) = match found {
            Some(m) => (m, false),
            None => self.create(name, p).await?,
        };
        if !created {
            m.company = self.live(m.company).await?;
            self.honour_dismissals(&mut m, attached).await?;
        }
        self.claim(&mut m, name, p, hex).await?;
        self.update_company(&m, name, p).await?;
        Ok(Some((m, created)))
    }

    /// A new company, keyed by its strongest identifier, else its name key. A node with that
    /// key already there (another instance, a blocked alias) is used as it is.
    async fn create(&self, name: &CompanyName, p: &Party) -> Result<(Match, bool)> {
        let b = &self.backend;
        let key = p.ids.first().map_or_else(
            || name.ladder_keys()[0].1.clone(),
            crate::company::Identifier::alias,
        );
        if let Some(n) = b.node_by_key(NodeKind::Company, &key).await? {
            return Ok((
                Match {
                    company: n.id,
                    step: Some(LadderStep::Normalized),
                    alias: key,
                    confidence: LadderStep::Normalized.confidence(),
                    conflicts: Vec::new(),
                    dismissed: None,
                },
                false,
            ));
        }
        // Created empty: a company another writer created with the same key at the same
        // moment keeps its props. `update_company` fills in the name.
        let id = b.ensure_node(NodeKind::Company, &key).await?;
        Ok((
            Match {
                company: id,
                step: None,
                alias: key,
                confidence: 1.0,
                conflicts: Vec::new(),
                dismissed: None,
            },
            true,
        ))
    }

    /// Claims the party's identifiers and name keys for the company. An identifier another
    /// company owns is refused and noted; a blind key owned by another becomes blocked.
    async fn claim(&self, m: &mut Match, name: &CompanyName, p: &Party, hex: &str) -> Result<()> {
        let b = &self.backend;
        let me = m.company;
        let evidence = |step: LadderStep| json!({"step": step.as_str(), "confidence": step.confidence(), "document": hex, "surface": name.original});
        let names = name.ladder_keys();
        // Identifiers first, then the name keys, as one batch (issue #103).
        let claims: Vec<(String, Value)> = p
            .ids
            .iter()
            .map(|id| (id.alias(), evidence(LadderStep::Identifier)))
            .chain(
                names
                    .iter()
                    .map(|(step, key)| (key.clone(), evidence(*step))),
            )
            .collect();
        let got = b.claim_aliases(NodeKind::Company, me, &claims).await?;
        let (ids, keys) = got.split_at(p.ids.len());
        for ((alias, _), got) in claims.iter().zip(ids) {
            if got.node.is_some_and(|o| o != me) && !m.conflicts.contains(alias) {
                m.conflicts.push(alias.clone());
            }
        }
        for ((step, key), got) in names.iter().zip(keys) {
            if *step == LadderStep::FormBlind && got.node.is_some_and(|o| o != me) {
                b.block_alias(
                    NodeKind::Company,
                    key,
                    &json!({"blocked": "more than one company owns this name", "document": hex}),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Claims the party's learned aliases ([`learned_aliases`]) for the company.
    async fn learn_aliases(&self, company: Uuid, claims: &[(String, Value)]) -> Result<()> {
        self.backend
            .claim_aliases(NodeKind::Company, company, claims)
            .await?;
        Ok(())
    }

    /// Merges what this party says into the company's props, as one atomic patch of the
    /// keys ingest keeps (issue #51): the enrichment job's keys, and what another document
    /// wrote at the same moment, stay.
    ///
    /// How the party matched is on its `party` edge only: a list of it on the company changed
    /// with every document, so every party rewrote its company's row (issue #129). For a
    /// company already known the patch now usually changes nothing and writes nothing. A
    /// `matches` list an earlier version stored is removed here, the next time the company is
    /// patched.
    async fn update_company(&self, m: &Match, name: &CompanyName, p: &Party) -> Result<()> {
        let mut patch = PropsPatch::new()
            .add_to_set(&["names"], json!(name.original), MAX_NAMES)
            .set_if_absent(&["name"], json!(name.original))
            .set_if_absent(&["key"], json!(name.key));
        if let Some(f) = name.legal_form {
            patch = patch.set_if_absent(&["legal_form"], json!(f));
        }
        if let Some(j) = &p.jurisdiction {
            patch = patch.set(&["jurisdiction"], json!(j));
        }
        patch = patch.add_to_set(&["quail_keys"], json!(p.quail_key), MAX_NAMES);
        for id in &p.ids {
            if m.conflicts.contains(&id.alias()) {
                continue;
            }
            patch = patch.add_to_set(&["ids", id.kind.as_str()], json!(id.value), MAX_IDS);
        }
        patch = patch.remove(&["matches"]);
        self.backend.update_props(m.company, &patch).await?;
        Ok(())
    }
}

/// Former and trading names a document states become learned aliases of the company (ladder
/// step 5): from the name's brackets, and from quail's name statements. The keys and their
/// props; they follow from the facts alone, so they are known before the party is matched.
fn learned_aliases(name: &CompanyName, facts: &DocFacts, hex: &str) -> Vec<(String, Value)> {
    let mut others: Vec<String> = name
        .aliases
        .iter()
        .filter(|a| a.kind != AliasKind::Annotation)
        .map(|a| a.text.clone())
        .collect();
    for s in &facts.statements {
        let current = CompanyName::parse(&s.current);
        if current.as_ref().is_some_and(|c| c.key == name.key) {
            others.push(s.other.clone());
        }
    }
    let step = LadderStep::LearnedAlias;
    others
        .into_iter()
        .filter_map(|other| {
            let alias = CompanyName::parse(&other)?;
            let form = alias.legal_form.or(name.legal_form).unwrap_or("");
            let key = format!("name:{}|{form}", alias.key);
            let props = json!({"step": step.as_str(), "confidence": step.confidence(), "document": hex, "surface": other});
            Some((key, props))
        })
        .collect()
}

/// A party with a name, and the learned aliases it claims.
type Work<'a> = (&'a Party, CompanyName, Vec<(String, Value)>);

/// Every key the parties read or write ([`PartyLocks`]): their identifier aliases, ladder
/// keys (the form-blind one included) and learned aliases. A company `create` makes is
/// keyed by one of the first two.
fn lock_keys(work: &[Work<'_>]) -> Vec<String> {
    let mut keys = Vec::new();
    for (p, name, learned) in work {
        keys.extend(p.ids.iter().map(crate::company::Identifier::alias));
        keys.extend(name.ladder_keys().into_iter().map(|(_, k)| k));
        keys.extend(learned.iter().map(|(k, _)| k.clone()));
    }
    keys
}

/// Adds a party's match to the document's edges: candidates that resolve to the same company
/// merge their roles.
fn add_edge(edges: &mut BTreeMap<Uuid, (Party, Match)>, p: &Party, m: Match) {
    match edges.get_mut(&m.company) {
        Some((q, _)) => {
            for r in &p.roles {
                if !q.roles.contains(r) {
                    q.roles.push(r.clone());
                }
            }
            q.score = q.score.max(p.score);
        }
        None => {
            edges.insert(m.company, (p.clone(), m));
        }
    }
}
