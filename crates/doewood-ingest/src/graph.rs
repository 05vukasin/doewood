//! A record's facts into the shared graph and search (ADR 0001 section 7, ADR 0002
//! section 1).
//!
//! - **Document and Location** through `upsert_location`; the document's props hold its format,
//!   title, kind and own numbers.
//! - **Companies** by the deterministic match ladder: validated identifiers first, then the
//!   normalized name with its legal form, the squashed name, and the legal-form-blind name
//!   while exactly one company owns it. Each key is a node alias, so a lookup is one read and
//!   the first claim wins; an identifier another company owns is refused, never moved. Every
//!   match keeps its step, confidence and evidence, on the `party` edge and the company. Fuzzy
//!   steps are not taken here: nothing merges on a fuzzy match.
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
//! - **`similar_to`**: candidates from the LSH band index, each confirmed by its Jaccard
//!   estimate. One edge per pair, from the smaller hash to the larger.
//! - **Search**: title, the summary card and keywords as the body, kind, date and amount.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use doewood_model::{
    Backend, Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, NeighbourQuery, NodeKind,
    ObjectLocation, PENDING_REF_PREFIX, PropsPatch, Result, SearchDoc, merged_into,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

use crate::company::{AliasKind, CompanyName, LadderStep};
use crate::facts::{DocFacts, Party};

/// How `similar_to` candidates are found. The least Jaccard estimate for an edge is a runtime
/// setting (`IngestSettings::similar_min_jaccard`), passed to [`GraphWriter::write`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimilarityConfig {
    /// Most candidates looked at per document.
    pub max_candidates: u32,
}

impl Default for SimilarityConfig {
    fn default() -> Self {
        Self {
            max_candidates: 100,
        }
    }
}

/// Most names, identifiers per kind and match notes a company's props keep.
const MAX_NAMES: usize = 20;
const MAX_IDS: usize = 10;
const MAX_MATCHES: usize = 20;
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
    /// `party` edges that landed on the survivor of a merge that committed after the
    /// company was matched (issue #48).
    pub redirects: usize,
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

/// Writes documents into a backend. Company resolution is serialized within the instance.
pub struct GraphWriter {
    backend: Arc<dyn Backend>,
    similarity: SimilarityConfig,
    companies: Mutex<()>,
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
            companies: Mutex::new(()),
        }
    }

    /// The backend written to.
    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    /// Writes one document found at `location`. Its record must already be stored.
    /// `name` is the file name, the title of last resort. `min_jaccard` is the least Jaccard
    /// estimate for a `similar_to` edge.
    pub async fn write(
        &self,
        blake3: &Blake3,
        location: &ObjectLocation,
        facts: &DocFacts,
        name: &str,
        min_jaccard: f64,
    ) -> Result<WriteStats> {
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
            self.similar(blake3, doc, facts, min_jaccard, &mut similar),
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
        min_jaccard: f64,
        stats: &mut WriteStats,
    ) -> Result<()> {
        let Some((sig, bands)) = &facts.minhash else {
            return Ok(());
        };
        let b = &self.backend;
        b.put_sketch(blake3, sig, bands).await?;
        let candidates = b
            .sketch_candidates(bands, blake3, self.similarity.max_candidates)
            .await?;
        for c in candidates {
            let score = quail_sketch::jaccard_estimate(sig, &c.signature);
            if score < min_jaccard {
                continue;
            }
            let other = document_node(&**b, &c.blake3).await?;
            // One edge per pair, from the smaller hash to the larger.
            let (src, dst) = if *blake3 < c.blake3 {
                (doc, other)
            } else {
                (other, doc)
            };
            #[allow(clippy::cast_possible_truncation)]
            let edge = Edge::new(src, dst, EdgeKind::SimilarTo, EdgeSource::Document)
                .with_props(json!({"score": score, "method": "minhash_lsh", "shared_bands": c.shared_bands}))
                .with_confidence(score.clamp(0.0, 1.0) as f32);
            b.upsert_edge(&edge).await?;
            stats.similar += 1;
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
        // One edge per company: candidates that resolve to the same one merge their roles.
        let mut edges: BTreeMap<Uuid, (Party, Match)> = BTreeMap::new();
        let serial = self.companies.lock().await;
        let attached = self.attached(doc).await?;
        for p in &facts.parties {
            let Some(name) = CompanyName::parse(&p.surface) else {
                continue;
            };
            let (m, created) = self.resolve(&name, p, hex, &attached).await?;
            if created {
                stats.new_companies += 1;
            }
            self.learn_aliases(m.company, &name, facts, hex).await?;
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
        drop(serial);
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
    /// `attached` are the companies the document is a party to now.
    async fn resolve(
        &self,
        name: &CompanyName,
        p: &Party,
        hex: &str,
        attached: &BTreeSet<Uuid>,
    ) -> Result<(Match, bool)> {
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
                    && node.props["legal_form"]
                        .as_str()
                        .is_some_and(|f| !f.is_empty() && f != form)
                {
                    continue; // "ACME d.o.o." is not "ACME a.d."
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
        self.update_company(&m, name, p, hex).await?;
        Ok((m, created))
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

    /// Former and trading names a document states become learned aliases of the company
    /// (ladder step 5): from the name's brackets, and from quail's name statements.
    async fn learn_aliases(
        &self,
        company: Uuid,
        name: &CompanyName,
        facts: &DocFacts,
        hex: &str,
    ) -> Result<()> {
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
        let claims: Vec<(String, Value)> = others
            .into_iter()
            .filter_map(|other| {
                let alias = CompanyName::parse(&other)?;
                let form = alias.legal_form.or(name.legal_form).unwrap_or("");
                let key = format!("name:{}|{form}", alias.key);
                let props = json!({"step": step.as_str(), "confidence": step.confidence(), "document": hex, "surface": other});
                Some((key, props))
            })
            .collect();
        self.backend
            .claim_aliases(NodeKind::Company, company, &claims)
            .await?;
        Ok(())
    }

    /// Merges what this party says into the company's props, as one atomic patch of the
    /// keys ingest keeps (issue #51): the enrichment job's keys, and what another document
    /// wrote at the same moment, stay.
    async fn update_company(
        &self,
        m: &Match,
        name: &CompanyName,
        p: &Party,
        hex: &str,
    ) -> Result<()> {
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
        let entry = json!({
            "step": m.step.map_or("created", LadderStep::as_str),
            "alias": m.alias,
            "confidence": m.confidence,
            "document": hex,
            "conflicts": m.conflicts,
        });
        patch = patch.push_capped(&["matches"], entry, &["document"], MAX_MATCHES);
        self.backend.update_props(m.company, &patch).await?;
        Ok(())
    }
}

/// The Document node of a stored record, created (without props) if it is missing.
async fn document_node(b: &dyn Backend, blake3: &Blake3) -> Result<Uuid> {
    let hex = blake3.to_hex();
    match b.node_by_key(NodeKind::Document, &hex).await? {
        Some(n) => Ok(n.id),
        None => b.ensure_node(NodeKind::Document, &hex).await,
    }
}
