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
//! - **References**: a cited number resolves to the document that has it as its own number
//!   (an alias `number:<key>`); when that document is not known yet, the edge goes to a pending
//!   Document node `ref:<key>`, and the edges move to the real document once it arrives.
//! - **`similar_to`**: candidates from the LSH band index, each confirmed by its Jaccard
//!   estimate. One edge per pair, from the smaller hash to the larger.
//! - **Search**: title, the summary card and keywords as the body, kind, date and amount.

use std::collections::BTreeMap;
use std::sync::Arc;

use doewood_model::{
    Backend, Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, NeighbourQuery, NodeKind,
    ObjectLocation, PENDING_REF_PREFIX, Result, SearchDoc,
};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;
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
        b.upsert_node(
            NodeKind::Document,
            &hex,
            &json!({
                "format": facts.format,
                "title": title,
                "kind": facts.kind,
                "numbers": facts.numbers,
            }),
        )
        .await?;
        let mut stats = WriteStats::default();
        self.numbers(doc, &hex, facts, &mut stats).await?;
        self.parties(doc, &hex, facts, &mut stats).await?;
        self.cites(doc, &hex, facts, &mut stats).await?;
        self.similar(blake3, doc, facts, min_jaccard, &mut stats)
            .await?;
        b.index_document(&SearchDoc {
            blake3: *blake3,
            title,
            body: facts.body.clone(),
            kind: facts.kind.clone(),
            date: facts.date,
            amount: facts.amount,
        })
        .await?;
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
        for n in &facts.numbers {
            b.claim_alias(
                NodeKind::Document,
                &format!("number:{n}"),
                doc,
                &json!({"document": hex}),
            )
            .await?;
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
        for c in &facts.cites {
            if facts.numbers.contains(&c.key) {
                continue; // its own number, cited in its own header
            }
            let kind = if c.relation == "amends" {
                EdgeKind::Amends
            } else {
                EdgeKind::Cites
            };
            let mut props =
                json!({"relation": c.relation, "raw": c.raw, "key": c.key, "kind": c.kind});
            let target = match b
                .alias(NodeKind::Document, &format!("number:{}", c.key))
                .await?
                .and_then(|a| a.node)
            {
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
        for p in &facts.parties {
            let Some(name) = CompanyName::parse(&p.surface) else {
                continue;
            };
            let (m, created) = self.resolve(&name, p, hex).await?;
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
                    },
                }))
                .with_evidence(json!([{
                    "document": hex,
                    "mentions": mentions,
                    "identifiers": ids,
                    "quail_key": p.quail_key,
                }]))
                .with_confidence(p.score);
            self.backend.upsert_edge(&edge).await?;
            stats.parties += 1;
        }
        Ok(())
    }

    /// The match ladder for one party. Returns the match and whether the company is new.
    async fn resolve(&self, name: &CompanyName, p: &Party, hex: &str) -> Result<(Match, bool)> {
        let b = &self.backend;
        // 1. Identifiers, strongest first. Ones that point elsewhere are conflicts.
        let mut found: Option<Match> = None;
        for id in &p.ids {
            let alias = id.alias();
            let Some(owner) = b
                .alias(NodeKind::Company, &alias)
                .await?
                .and_then(|a| a.node)
            else {
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
                    });
                }
                Some(m) if m.company != owner => m.conflicts.push(alias),
                Some(_) => {}
            }
        }
        // 2. Names: normalized with the form, squashed, legal-form-blind.
        if found.is_none() {
            for (step, key) in name.ladder_keys() {
                let Some(owner) = b.alias(NodeKind::Company, &key).await?.and_then(|a| a.node)
                else {
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
                });
                break;
            }
        }
        let (mut m, created) = match found {
            Some(m) => (m, false),
            None => self.create(name, p).await?,
        };
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
                },
                false,
            ));
        }
        let id = b
            .upsert_node(
                NodeKind::Company,
                &key,
                &json!({"name": name.original, "key": name.key, "legal_form": name.legal_form}),
            )
            .await?;
        Ok((
            Match {
                company: id,
                step: None,
                alias: key,
                confidence: 1.0,
                conflicts: Vec::new(),
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
        for id in &p.ids {
            let alias = id.alias();
            let got = b
                .claim_alias(
                    NodeKind::Company,
                    &alias,
                    me,
                    &evidence(LadderStep::Identifier),
                )
                .await?;
            if got.node.is_some_and(|o| o != me) && !m.conflicts.contains(&alias) {
                m.conflicts.push(alias);
            }
        }
        for (step, key) in name.ladder_keys() {
            let got = b
                .claim_alias(NodeKind::Company, &key, me, &evidence(step))
                .await?;
            if step == LadderStep::FormBlind && got.node.is_some_and(|o| o != me) {
                b.block_alias(
                    NodeKind::Company,
                    &key,
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
        for other in others {
            let Some(alias) = CompanyName::parse(&other) else {
                continue;
            };
            let form = alias.legal_form.or(name.legal_form).unwrap_or("");
            let key = format!("name:{}|{form}", alias.key);
            let step = LadderStep::LearnedAlias;
            self.backend
                .claim_alias(
                    NodeKind::Company,
                    &key,
                    company,
                    &json!({"step": step.as_str(), "confidence": step.confidence(), "document": hex, "surface": other}),
                )
                .await?;
        }
        Ok(())
    }

    /// Merges what this party says into the company's props.
    async fn update_company(
        &self,
        m: &Match,
        name: &CompanyName,
        p: &Party,
        hex: &str,
    ) -> Result<()> {
        let b = &self.backend;
        let Some(node) = b.node(m.company).await? else {
            return Ok(());
        };
        let mut props = match node.props {
            Value::Object(o) => o,
            _ => Map::new(),
        };
        push_unique(
            &mut props,
            "names",
            Value::String(name.original.clone()),
            MAX_NAMES,
        );
        if props.get("name").is_none_or(Value::is_null) {
            props.insert("name".into(), Value::String(name.original.clone()));
        }
        if props.get("legal_form").is_none_or(Value::is_null)
            && let Some(f) = name.legal_form
        {
            props.insert("legal_form".into(), Value::String(f.to_owned()));
        }
        if props.get("key").is_none_or(Value::is_null) {
            props.insert("key".into(), Value::String(name.key.clone()));
        }
        if let Some(j) = &p.jurisdiction {
            props.insert("jurisdiction".into(), Value::String(j.clone()));
        }
        push_unique(
            &mut props,
            "quail_keys",
            Value::String(p.quail_key.clone()),
            MAX_NAMES,
        );
        for id in &p.ids {
            if m.conflicts.contains(&id.alias()) {
                continue;
            }
            let ids = props
                .entry("ids")
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(ids) = ids {
                let list = ids
                    .entry(id.kind.as_str())
                    .or_insert_with(|| Value::Array(Vec::new()));
                if let Value::Array(l) = list
                    && l.len() < MAX_IDS
                    && !l.iter().any(|v| v.as_str() == Some(id.value.as_str()))
                {
                    l.push(Value::String(id.value.clone()));
                }
            }
        }
        let entry = json!({
            "step": m.step.map_or("created", LadderStep::as_str),
            "alias": m.alias,
            "confidence": m.confidence,
            "document": hex,
            "conflicts": m.conflicts,
        });
        let matches = props
            .entry("matches")
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(l) = matches {
            l.retain(|n| n["document"] != entry["document"]);
            l.push(entry);
            if l.len() > MAX_MATCHES {
                l.remove(0);
            }
        }
        b.upsert_node(NodeKind::Company, &node.key, &Value::Object(props))
            .await?;
        Ok(())
    }
}

fn push_unique(props: &mut Map<String, Value>, field: &str, v: Value, max: usize) {
    let list = props
        .entry(field)
        .or_insert_with(|| Value::Array(Vec::new()));
    if let Value::Array(l) = list
        && l.len() < max
        && !l.contains(&v)
    {
        l.push(v);
    }
}

/// The Document node of a stored record, created (without props) if it is missing.
async fn document_node(b: &dyn Backend, blake3: &Blake3) -> Result<Uuid> {
    let hex = blake3.to_hex();
    match b.node_by_key(NodeKind::Document, &hex).await? {
        Some(n) => Ok(n.id),
        None => b.upsert_node(NodeKind::Document, &hex, &json!({})).await,
    }
}
