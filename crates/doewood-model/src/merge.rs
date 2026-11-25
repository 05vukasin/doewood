//! Reversible company merges (ADR 0002 section 1.8, ADR 0001 section 7).
//!
//! - **Merge:** a surviving company absorbs another. The absorbed company's aliases and edges
//!   move to the survivor, its props are folded into the survivor's, and the absorbed node
//!   stays as a husk (`{"merged_into": <survivor>, "merge": <entry>}`), so its id keeps
//!   resolving. An edge whose moved identity already exists at the survivor is not moved but
//!   superseded. Edges between the two companies stay where they are.
//! - **Split:** a new company takes a partition of a company's aliases, documents (their
//!   `party` edges) and other edges.
//! - **The log:** each merge and split is one [`MergeEntry`], written in the same transaction,
//!   with its actor, evidence, confidence, fused probability and an undo payload that lists
//!   exactly what moved.
//! - **Undo:** restores what the payload lists. An entry that later live entries depend on
//!   (they involve one of its companies, or a company of such an entry) is refused with
//!   [`Error::Precondition`] naming them, unless the undo cascades: then they are undone
//!   first, newest first.
//! - **Dismissal:** a permanent "not the same" pair, in both directions. A merge between the
//!   groups of a dismissed pair (the companies and everything merged into them) is refused,
//!   whatever the evidence.
//!
//! The rules run once, here, over a [`MergeTx`]: each backend implements those primitives
//! inside one of its transactions.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use doewood_core::InstanceId;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{
    Edge, EdgeKind, EdgeState, Error, MergeActorKind, MergeOp, NODE_KEY_MAX_LEN, Node, NodeAlias,
    NodeKind, Result, StoredEdge, validate_node,
};

/// Largest page of [`crate::MergeStore::list_merges`] and
/// [`crate::MergeStore::list_dismissals`].
pub const MERGES_MAX_LIMIT: u32 = 1000;

/// Most aliases, documents and edges one split names, each.
pub const SPLIT_MAX_ITEMS: usize = 1000;

/// Longest dismissal or undo reason, in bytes.
pub const REASON_MAX_LEN: usize = 2000;

/// Largest evidence list of one entry, as JSON bytes.
pub const EVIDENCE_MAX_BYTES: usize = 64 * 1024;

/// Most `merged_into` hops followed, and most merges folded into one group.
const MAX_CHAIN: usize = 64;
const MAX_GROUP: usize = 10_000;

/// Props keys the merge log owns on company nodes.
const RESERVED: &[&str] = &["merged_into", "merge", "merged_from", "split_from", "split"];

/// The page size the listings use for `limit`.
pub fn merges_limit(limit: u32) -> u32 {
    match limit {
        0 => 50,
        n => n.min(MERGES_MAX_LIMIT),
    }
}

/// The company a husk was merged into, from its props.
pub fn merged_into(node: &Node) -> Option<Uuid> {
    node.props
        .get("merged_into")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// Who did it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeActor {
    /// Ingest, enrichment or a person.
    pub kind: MergeActorKind,
    /// The instance that did it.
    pub instance: InstanceId,
}

/// A merge: `absorbed` into `survivor`.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeRequest {
    /// The company that stays.
    pub survivor: Uuid,
    /// The company that is absorbed.
    pub absorbed: Uuid,
    /// Who merges.
    pub actor: MergeActor,
    /// Why: a JSON array of evidence objects (source, reliability, alias, document, ...).
    pub evidence: Value,
    /// The actor's own confidence in the decision, from 0 to 1.
    pub confidence: f32,
    /// The fused (noisy-OR) probability of the evidence, from 0 to 1 (ADR 0002 section 1.6).
    pub probability: f32,
}

impl MergeRequest {
    /// Checks the request before it reaches a backend.
    pub fn validate(&self) -> Result<()> {
        if self.survivor == self.absorbed {
            return Err(Error::Invalid(
                "a company cannot be merged into itself".to_owned(),
            ));
        }
        validate_evidence(&self.evidence)?;
        validate_unit("confidence", self.confidence)?;
        validate_unit("probability", self.probability)
    }
}

/// A split: a new company takes the named aliases, documents and edges of `company`.
#[derive(Clone, Debug, PartialEq)]
pub struct SplitRequest {
    /// The company split.
    pub company: Uuid,
    /// The new company's node key; `None` gives `split:<entry id>`.
    pub key: Option<String>,
    /// The new company's props (a JSON object): its name, for one.
    pub props: Value,
    /// Company aliases (identifiers, name keys) that move to the new company. Each must be
    /// owned by `company`.
    pub aliases: Vec<String>,
    /// Documents whose `party` edges to `company` move to the new company.
    pub documents: Vec<Uuid>,
    /// Other edges at `company` (company to company) that move.
    pub edges: Vec<Uuid>,
    /// Who splits.
    pub actor: MergeActor,
    /// Why: a JSON array.
    pub evidence: Value,
    /// Also record the two as a permanent "not the same" pair.
    pub dismiss: bool,
}

impl SplitRequest {
    /// Checks the request before it reaches a backend.
    pub fn validate(&self) -> Result<()> {
        if self.aliases.is_empty() && self.documents.is_empty() && self.edges.is_empty() {
            return Err(Error::Invalid(
                "a split moves at least one alias, document or edge".to_owned(),
            ));
        }
        for (what, n) in [
            ("aliases", self.aliases.len()),
            ("documents", self.documents.len()),
            ("edges", self.edges.len()),
        ] {
            if n > SPLIT_MAX_ITEMS {
                return Err(Error::Invalid(format!(
                    "a split names at most {SPLIT_MAX_ITEMS} {what}"
                )));
            }
        }
        if let Some(key) = &self.key {
            validate_node(NodeKind::Company, key, &self.props)?;
        } else if !self.props.is_object() {
            return Err(Error::Invalid("props must be a JSON object".to_owned()));
        }
        validate_evidence(&self.evidence)
    }
}

/// An undo of a merge or a split.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndoRequest {
    /// The entry.
    pub merge: Uuid,
    /// Who undoes it.
    pub actor: MergeActor,
    /// Undo the later entries that depend on it first, instead of refusing.
    pub cascade: bool,
    /// Record the two companies of a merge as a permanent "not the same" pair.
    pub dismiss: bool,
    /// Why (kept on the dismissal).
    pub reason: String,
}

/// What an undo did.
#[derive(Clone, Debug, PartialEq)]
pub struct UndoOutcome {
    /// The entries undone, in the order they were undone (dependents first, the asked-for
    /// entry last).
    pub undone: Vec<Uuid>,
    /// Parts of a payload that could not be restored because something else changed them
    /// since (an alias blocked or claimed by another company, an edge identity taken).
    pub skipped: Vec<String>,
    /// The dismissal recorded, when asked for.
    pub dismissal: Option<Dismissal>,
}

/// When and by whom an entry was undone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Undone {
    /// Unix seconds.
    pub at: i64,
    /// Who.
    pub actor: MergeActor,
}

/// One merge log entry.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeEntry {
    /// Its id.
    pub id: Uuid,
    /// Its place in the log: later entries have larger numbers.
    pub seq: i64,
    /// Merge or split.
    pub op: MergeOp,
    /// When, in Unix seconds.
    pub at: i64,
    /// Who.
    pub actor: MergeActor,
    /// A merge's surviving company; a split's company that was split.
    pub survivor: Uuid,
    /// A merge's absorbed company; a split's new company.
    pub absorbed: Uuid,
    /// Why: a JSON array.
    pub evidence: Value,
    /// The actor's confidence.
    pub confidence: f32,
    /// The fused probability.
    pub probability: f32,
    /// What moved, so an undo restores exactly that (a JSON object, see the module docs).
    pub undo: Value,
    /// Set once undone.
    pub undone: Option<Undone>,
}

impl MergeEntry {
    fn touches(&self, nodes: &BTreeSet<Uuid>) -> bool {
        nodes.contains(&self.survivor) || nodes.contains(&self.absorbed)
    }
}

/// A page of the merge log, newest first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeQuery {
    /// Only entries where this company is the survivor or the absorbed one.
    pub company: Option<Uuid>,
    /// Only entries with a `seq` below this (the previous page's last).
    pub before: Option<i64>,
    /// Page size; 0 means 50, at most [`MERGES_MAX_LIMIT`].
    pub limit: u32,
}

/// A permanent "not the same" pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DismissRequest {
    /// One company.
    pub a: Uuid,
    /// The other.
    pub b: Uuid,
    /// Who.
    pub actor: MergeActor,
    /// Why.
    pub reason: String,
}

/// A stored dismissal. It holds in both directions; `a` is the smaller id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dismissal {
    /// Its place in the list: later dismissals have larger numbers.
    pub seq: i64,
    /// The smaller company id.
    pub a: Uuid,
    /// The larger company id.
    pub b: Uuid,
    /// When, in Unix seconds.
    pub at: i64,
    /// Who.
    pub actor: MergeActor,
    /// Why.
    pub reason: String,
    /// The merge whose undo recorded it, if any.
    pub merge: Option<Uuid>,
}

/// A page of dismissals, newest first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DismissalQuery {
    /// Only dismissals of this company.
    pub company: Option<Uuid>,
    /// Only dismissals with a `seq` below this.
    pub before: Option<i64>,
    /// Page size; 0 means 50, at most [`MERGES_MAX_LIMIT`].
    pub limit: u32,
}

/// An alias as listed for its node.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnedAlias {
    /// The alias's node kind.
    pub kind: NodeKind,
    /// The alias.
    pub alias: String,
    /// Its props (evidence, confidence).
    pub props: Value,
}

pub(crate) fn validate_evidence(evidence: &Value) -> Result<()> {
    if !evidence.is_array() {
        return Err(Error::Invalid("evidence must be a JSON array".to_owned()));
    }
    if evidence.to_string().len() > EVIDENCE_MAX_BYTES {
        return Err(Error::Invalid(format!(
            "evidence is at most {EVIDENCE_MAX_BYTES} bytes of JSON"
        )));
    }
    Ok(())
}

pub(crate) fn validate_unit(what: &str, v: f32) -> Result<()> {
    if (0.0..=1.0).contains(&v) {
        Ok(())
    } else {
        Err(Error::Invalid(format!("{what} {v} is not between 0 and 1")))
    }
}

pub(crate) fn validate_reason(reason: &str) -> Result<()> {
    if reason.len() > REASON_MAX_LEN || reason.contains('\0') {
        return Err(Error::Invalid(format!(
            "a reason is at most {REASON_MAX_LEN} bytes, without NUL"
        )));
    }
    Ok(())
}

/// The primitives the merge rules need, inside one backend transaction. Edges and aliases are
/// listed in a stable order (by id, by kind and alias).
#[async_trait]
pub trait MergeTx: Send {
    /// Locks the nodes against concurrent writes (and against new edges and aliases that
    /// refer to them) until the transaction ends.
    async fn lock_nodes(&mut self, ids: &[Uuid]) -> Result<()>;
    /// A node.
    async fn node(&mut self, id: Uuid) -> Result<Option<Node>>;
    /// Inserts a node with this id. `false` when a node of this kind and key exists.
    async fn insert_node(
        &mut self,
        id: Uuid,
        kind: NodeKind,
        key: &str,
        props: &Value,
    ) -> Result<bool>;
    /// Replaces a node's props.
    async fn set_props(&mut self, id: Uuid, props: &Value) -> Result<()>;
    /// Deletes a node (with what cascades from it).
    async fn delete_node(&mut self, id: Uuid) -> Result<()>;
    /// Every edge at the node, in any state, by id.
    async fn edges_at(&mut self, node: Uuid) -> Result<Vec<StoredEdge>>;
    /// An edge by id.
    async fn edge(&mut self, id: Uuid) -> Result<Option<StoredEdge>>;
    /// The id of the edge with this identity (`src, dst, kind, source, valid_from`), if any.
    async fn edge_with_identity(&mut self, edge: &Edge) -> Result<Option<Uuid>>;
    /// Moves an edge's ends, keeping everything else (its id too).
    async fn repoint_edge(&mut self, id: Uuid, src: Uuid, dst: Uuid) -> Result<()>;
    /// Sets an edge's state.
    async fn set_edge_state(&mut self, id: Uuid, state: EdgeState) -> Result<()>;
    /// The aliases a node owns.
    async fn aliases_of(&mut self, node: Uuid) -> Result<Vec<OwnedAlias>>;
    /// An alias, if claimed or blocked.
    async fn alias(&mut self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>>;
    /// Gives an existing alias to another node, keeping its props.
    async fn set_alias_owner(&mut self, kind: NodeKind, alias: &str, node: Uuid) -> Result<()>;
    /// Appends an entry; returns its `seq`.
    async fn insert_entry(&mut self, entry: &MergeEntry) -> Result<i64>;
    /// An entry.
    async fn entry(&mut self, id: Uuid) -> Result<Option<MergeEntry>>;
    /// A page of entries.
    async fn entries(&mut self, query: &MergeQuery) -> Result<Vec<MergeEntry>>;
    /// The entries not undone with a `seq` above `seq` that involve one of `nodes`, oldest
    /// first.
    async fn live_entries_after(&mut self, seq: i64, nodes: &[Uuid]) -> Result<Vec<MergeEntry>>;
    /// The companies absorbed by live merges into one of `survivors`.
    async fn absorbed_into(&mut self, survivors: &[Uuid]) -> Result<Vec<Uuid>>;
    /// Marks an entry undone.
    async fn mark_undone(&mut self, id: Uuid, undone: &Undone) -> Result<()>;
    /// A dismissal between a company of `a` and a company of `b`, if any.
    async fn dismissal_among(&mut self, a: &[Uuid], b: &[Uuid]) -> Result<Option<Dismissal>>;
    /// Inserts the dismissal unless the pair has one; returns the stored one.
    async fn insert_dismissal(&mut self, d: &Dismissal) -> Result<Dismissal>;
    /// A page of dismissals.
    async fn dismissals(&mut self, query: &DismissalQuery) -> Result<Vec<Dismissal>>;
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// A live (not merged-away) company node, for update.
async fn live_company<T: MergeTx + ?Sized>(tx: &mut T, id: Uuid) -> Result<Node> {
    let node = tx
        .node(id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("company {id} does not exist")))?;
    if node.kind != NodeKind::Company {
        return Err(Error::Invalid(format!(
            "node {id} is a {}, not a company",
            node.kind
        )));
    }
    if let Some(into) = merged_into(&node) {
        return Err(Error::Precondition(format!(
            "company {id} was merged into {into}; use that company"
        )));
    }
    Ok(node)
}

/// The live company a company node resolves to: itself, or where its merges led.
async fn root<T: MergeTx + ?Sized>(tx: &mut T, id: Uuid) -> Result<Uuid> {
    let mut at = id;
    for _ in 0..MAX_CHAIN {
        let node = tx
            .node(at)
            .await?
            .ok_or_else(|| Error::NotFound(format!("company {at} does not exist")))?;
        if node.kind != NodeKind::Company {
            return Err(Error::Invalid(format!(
                "node {at} is a {}, not a company",
                node.kind
            )));
        }
        match merged_into(&node) {
            Some(next) => at = next,
            None => return Ok(at),
        }
    }
    Err(Error::Precondition(format!(
        "company {id} has more than {MAX_CHAIN} merges in a chain"
    )))
}

/// A live company and every company merged into it, transitively.
async fn group<T: MergeTx + ?Sized>(tx: &mut T, root: Uuid) -> Result<Vec<Uuid>> {
    let mut all = BTreeSet::from([root]);
    let mut frontier = vec![root];
    while !frontier.is_empty() && all.len() < MAX_GROUP {
        let next = tx.absorbed_into(&frontier).await?;
        frontier = next.into_iter().filter(|n| all.insert(*n)).collect();
    }
    Ok(all.into_iter().collect())
}

/// The dismissal that keeps `a` and `b` apart: between any company of `a`'s group and any of
/// `b`'s. `None` when there is none, or when they are one company now.
pub async fn find_dismissal<T: MergeTx + ?Sized>(
    tx: &mut T,
    a: Uuid,
    b: Uuid,
) -> Result<Option<Dismissal>> {
    let (ra, rb) = (root(tx, a).await?, root(tx, b).await?);
    if ra == rb {
        return Ok(None);
    }
    let ga = group(tx, ra).await?;
    let gb = group(tx, rb).await?;
    tx.dismissal_among(&ga, &gb).await
}

fn dismissed_error(d: &Dismissal) -> Error {
    Error::Precondition(format!(
        "companies {} and {} are dismissed as not the same (by {} on instance {}); a \
         dismissal is permanent",
        d.a,
        d.b,
        d.actor.kind,
        d.actor.instance.as_str()
    ))
}

/// Merges `absorbed` into `survivor` and logs it (see the module docs).
pub async fn merge<T: MergeTx + ?Sized>(tx: &mut T, req: &MergeRequest) -> Result<MergeEntry> {
    req.validate()?;
    let (s, a) = (req.survivor, req.absorbed);
    tx.lock_nodes(&[s, a]).await?;
    let survivor = live_company(tx, s).await?;
    let absorbed = live_company(tx, a).await?;
    if let Some(d) = find_dismissal(tx, s, a).await? {
        return Err(dismissed_error(&d));
    }
    let id = Uuid::now_v7();
    // Edges: moved in place (same id), or superseded where the survivor has that identity.
    let mut moved = Vec::new();
    let mut superseded = Vec::new();
    for e in tx.edges_at(a).await? {
        let (src, dst) = (e.edge.src, e.edge.dst);
        if src == s || dst == s {
            continue; // between the two: stays as it is
        }
        let end = match (src == a, dst == a) {
            (true, true) => "both",
            (true, false) => "src",
            _ => "dst",
        };
        let mut target = e.edge.clone();
        if src == a {
            target.src = s;
        }
        if dst == a {
            target.dst = s;
        }
        if tx.edge_with_identity(&target).await?.is_some() {
            if e.edge.state.is_live() {
                tx.set_edge_state(e.id, EdgeState::Superseded).await?;
                superseded.push(json!({"id": e.id.to_string(), "state": e.edge.state.as_str()}));
            }
        } else {
            tx.repoint_edge(e.id, target.src, target.dst).await?;
            moved.push(json!({"id": e.id.to_string(), "end": end}));
        }
    }
    let mut aliases = Vec::new();
    for al in tx.aliases_of(a).await? {
        tx.set_alias_owner(al.kind, &al.alias, s).await?;
        aliases.push(json!({"kind": al.kind.as_str(), "alias": al.alias}));
    }
    let (mut props, added) = fold(&object(&survivor.props), &object(&absorbed.props));
    push(
        &mut props,
        "merged_from",
        json!({"company": a.to_string(), "merge": id.to_string()}),
    );
    tx.set_props(s, &Value::Object(props)).await?;
    tx.set_props(a, &husk(&absorbed, s, id)).await?;
    let mut entry = MergeEntry {
        id,
        seq: 0,
        op: MergeOp::Merge,
        at: now(),
        actor: req.actor.clone(),
        survivor: s,
        absorbed: a,
        evidence: req.evidence.clone(),
        confidence: req.confidence,
        probability: req.probability,
        undo: json!({
            "v": 1,
            "survivor_props": survivor.props,
            "absorbed_props": absorbed.props,
            "props_added": added,
            "aliases": aliases,
            "edges": moved,
            "superseded": superseded,
        }),
        undone: None,
    };
    entry.seq = tx.insert_entry(&entry).await?;
    Ok(entry)
}

/// Splits a company in two and logs it (see the module docs).
pub async fn split<T: MergeTx + ?Sized>(tx: &mut T, req: &SplitRequest) -> Result<MergeEntry> {
    req.validate()?;
    let c = req.company;
    tx.lock_nodes(&[c]).await?;
    live_company(tx, c).await?;
    let id = Uuid::now_v7();
    let key = req.key.clone().unwrap_or_else(|| format!("split:{id}"));
    if key.len() > NODE_KEY_MAX_LEN {
        return Err(Error::Invalid(format!(
            "a node key is 1 to {NODE_KEY_MAX_LEN} bytes"
        )));
    }
    let mut props = object(&req.props);
    for k in RESERVED {
        props.remove(*k);
    }
    props.insert("split_from".into(), Value::String(c.to_string()));
    props.insert("split".into(), Value::String(id.to_string()));
    let n = Uuid::now_v7();
    if !tx
        .insert_node(n, NodeKind::Company, &key, &Value::Object(props))
        .await?
    {
        return Err(Error::Precondition(format!(
            "a company with the key {key:?} exists"
        )));
    }
    let mut aliases = Vec::new();
    for alias in uniq(&req.aliases) {
        let owner = tx
            .alias(NodeKind::Company, alias)
            .await?
            .and_then(|a| a.node);
        if owner != Some(c) {
            return Err(Error::Invalid(format!(
                "alias {alias:?} is not owned by company {c}"
            )));
        }
        tx.set_alias_owner(NodeKind::Company, alias, n).await?;
        aliases.push(json!({"kind": "company", "alias": alias}));
    }
    let at_c = tx.edges_at(c).await?;
    let mut chosen: BTreeSet<Uuid> = BTreeSet::new();
    for d in uniq(&req.documents) {
        let party: Vec<&StoredEdge> = at_c
            .iter()
            .filter(|e| e.edge.kind == EdgeKind::Party && e.edge.src == *d && e.edge.dst == c)
            .collect();
        if party.is_empty() {
            return Err(Error::Invalid(format!(
                "document {d} has no party edge to company {c}"
            )));
        }
        chosen.extend(party.iter().map(|e| e.id));
    }
    for e in uniq(&req.edges) {
        let Some(edge) = at_c.iter().find(|x| x.id == *e) else {
            return Err(Error::Invalid(format!("edge {e} is not at company {c}")));
        };
        if edge.edge.src == edge.edge.dst {
            return Err(Error::Invalid(format!(
                "edge {e} is a loop on company {c}; it cannot be split off"
            )));
        }
        chosen.insert(*e);
    }
    let mut moved = Vec::new();
    for e in at_c.iter().filter(|e| chosen.contains(&e.id)) {
        let (mut src, mut dst) = (e.edge.src, e.edge.dst);
        let end = if src == c {
            src = n;
            "src"
        } else {
            dst = n;
            "dst"
        };
        tx.repoint_edge(e.id, src, dst).await?;
        moved.push(json!({"id": e.id.to_string(), "end": end}));
    }
    let mut entry = MergeEntry {
        id,
        seq: 0,
        op: MergeOp::Split,
        at: now(),
        actor: req.actor.clone(),
        survivor: c,
        absorbed: n,
        evidence: req.evidence.clone(),
        confidence: 1.0,
        probability: 1.0,
        undo: json!({"v": 1, "key": key, "aliases": aliases, "edges": moved}),
        undone: None,
    };
    entry.seq = tx.insert_entry(&entry).await?;
    if req.dismiss {
        dismiss_checked(tx, c, n, &req.actor, "split", None).await?;
    }
    Ok(entry)
}

/// Undoes an entry, and with `cascade` the later ones that depend on it (see the module docs).
pub async fn undo<T: MergeTx + ?Sized>(tx: &mut T, req: &UndoRequest) -> Result<UndoOutcome> {
    validate_reason(&req.reason)?;
    let entry = tx
        .entry(req.merge)
        .await?
        .ok_or_else(|| Error::NotFound(format!("merge {} does not exist", req.merge)))?;
    if let Some(u) = &entry.undone {
        return Err(Error::Precondition(format!(
            "merge {} was already undone (at {} by {})",
            entry.id, u.at, u.actor.kind
        )));
    }
    if req.dismiss && entry.op == MergeOp::Split {
        return Err(Error::Invalid(
            "dismiss applies to undoing a merge; a split can record one itself".to_owned(),
        ));
    }
    // The later live entries that depend on this one, transitively.
    let mut nodes = BTreeSet::from([entry.survivor, entry.absorbed]);
    let mut later: Vec<MergeEntry> = Vec::new();
    loop {
        let list: Vec<Uuid> = nodes.iter().copied().collect();
        let found = tx.live_entries_after(entry.seq, &list).await?;
        let mut grew = false;
        for f in found {
            if later.iter().any(|l| l.id == f.id) || !f.touches(&nodes) {
                continue;
            }
            grew |= nodes.insert(f.survivor);
            grew |= nodes.insert(f.absorbed);
            later.push(f);
        }
        if !grew {
            break;
        }
    }
    later.sort_by_key(|e| e.seq);
    if !later.is_empty() && !req.cascade {
        let ids: Vec<String> = later.iter().map(|e| e.id.to_string()).collect();
        return Err(Error::Precondition(format!(
            "later merges or splits involve these companies: {}; undo them first, or undo \
             with cascade",
            ids.join(", ")
        )));
    }
    let all: Vec<Uuid> = nodes.iter().copied().collect();
    tx.lock_nodes(&all).await?;
    let mut out = UndoOutcome {
        undone: Vec::new(),
        skipped: Vec::new(),
        dismissal: None,
    };
    let undone = Undone {
        at: now(),
        actor: req.actor.clone(),
    };
    for e in later.iter().rev().chain(std::iter::once(&entry)) {
        match e.op {
            MergeOp::Merge => undo_merge(tx, e, &mut out.skipped).await?,
            MergeOp::Split => undo_split(tx, e, &mut out.skipped).await?,
        }
        tx.mark_undone(e.id, &undone).await?;
        out.undone.push(e.id);
    }
    if req.dismiss {
        out.dismissal = Some(
            dismiss_checked(
                tx,
                entry.survivor,
                entry.absorbed,
                &req.actor,
                &req.reason,
                Some(entry.id),
            )
            .await?,
        );
    }
    Ok(out)
}

fn payload_list<'a>(undo: &'a Value, field: &str) -> &'a [Value] {
    undo.get(field)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn payload_id(v: &Value) -> Result<Uuid> {
    v.get("id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| Error::Backend {
            backend: "merge log",
            message: "an undo payload entry has no valid id".to_owned(),
        })
}

async fn undo_merge<T: MergeTx + ?Sized>(
    tx: &mut T,
    e: &MergeEntry,
    skipped: &mut Vec<String>,
) -> Result<()> {
    let (s, a) = (e.survivor, e.absorbed);
    let p = &e.undo;
    for al in payload_list(p, "aliases") {
        let (Some(kind), Some(alias)) = (
            al.get("kind")
                .and_then(Value::as_str)
                .and_then(|k| k.parse::<NodeKind>().ok()),
            al.get("alias").and_then(Value::as_str),
        ) else {
            continue;
        };
        if tx.alias(kind, alias).await?.and_then(|x| x.node) == Some(s) {
            tx.set_alias_owner(kind, alias, a).await?;
        } else {
            skipped.push(format!("alias {alias:?} is no longer owned by company {s}"));
        }
    }
    for m in payload_list(p, "edges") {
        let id = payload_id(m)?;
        let end = m.get("end").and_then(Value::as_str).unwrap_or("");
        let Some(edge) = tx.edge(id).await? else {
            skipped.push(format!("edge {id} no longer exists"));
            continue;
        };
        let mut back = edge.edge.clone();
        let ok = match end {
            "src" if back.src == s => {
                back.src = a;
                true
            }
            "dst" if back.dst == s => {
                back.dst = a;
                true
            }
            "both" if back.src == s && back.dst == s => {
                back.src = a;
                back.dst = a;
                true
            }
            _ => false,
        };
        if !ok {
            skipped.push(format!("edge {id} no longer ends at company {s}"));
            continue;
        }
        if tx
            .edge_with_identity(&back)
            .await?
            .is_some_and(|other| other != id)
        {
            skipped.push(format!("edge {id}: company {a} has that edge again"));
            continue;
        }
        tx.repoint_edge(id, back.src, back.dst).await?;
    }
    for sup in payload_list(p, "superseded") {
        let id = payload_id(sup)?;
        let state = sup
            .get("state")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<EdgeState>().ok())
            .unwrap_or(EdgeState::Active);
        match tx.edge(id).await? {
            Some(edge) if edge.edge.state == EdgeState::Superseded => {
                tx.set_edge_state(id, state).await?;
            }
            Some(_) => skipped.push(format!("edge {id} changed state since the merge")),
            None => skipped.push(format!("edge {id} no longer exists")),
        }
    }
    if let Some(survivor) = tx.node(s).await? {
        let mut props = object(&survivor.props);
        unfold(&mut props, payload_list(p, "props_added"));
        if let Some(Value::Array(list)) = props.get_mut("merged_from") {
            let me = e.id.to_string();
            list.retain(|x| x.get("merge").and_then(Value::as_str) != Some(me.as_str()));
            if list.is_empty() {
                props.remove("merged_from");
            }
        }
        tx.set_props(s, &Value::Object(props)).await?;
    }
    let before = p
        .get("absorbed_props")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    tx.set_props(a, &before).await?;
    Ok(())
}

async fn undo_split<T: MergeTx + ?Sized>(
    tx: &mut T,
    e: &MergeEntry,
    skipped: &mut Vec<String>,
) -> Result<()> {
    let (c, n) = (e.survivor, e.absorbed);
    if let Some(d) = tx.dismissal_among(&[c], &[n]).await? {
        return Err(dismissed_error(&d));
    }
    let Some(new) = tx.node(n).await? else {
        skipped.push(format!("company {n} no longer exists"));
        return Ok(());
    };
    // Everything at the new company goes back, including what attached to it since.
    let mut left = 0usize;
    for edge in tx.edges_at(n).await? {
        let (src, dst) = (edge.edge.src, edge.edge.dst);
        if src == c || dst == c {
            left += 1;
            continue;
        }
        let mut back = edge.edge.clone();
        if src == n {
            back.src = c;
        }
        if dst == n {
            back.dst = c;
        }
        if tx.edge_with_identity(&back).await?.is_some() {
            if edge.edge.state.is_live() {
                tx.set_edge_state(edge.id, EdgeState::Superseded).await?;
            }
            skipped.push(format!(
                "edge {}: company {c} has that edge; superseded on {n}",
                edge.id
            ));
            left += 1;
        } else {
            tx.repoint_edge(edge.id, back.src, back.dst).await?;
        }
    }
    for al in tx.aliases_of(n).await? {
        tx.set_alias_owner(al.kind, &al.alias, c).await?;
    }
    if left == 0 {
        tx.delete_node(n).await?;
    } else {
        tx.set_props(n, &husk(&new, c, e.id)).await?;
    }
    Ok(())
}

/// Records a dismissal (see [`crate::MergeStore::dismiss_pair`]).
pub async fn dismiss<T: MergeTx + ?Sized>(tx: &mut T, req: &DismissRequest) -> Result<Dismissal> {
    dismiss_checked(tx, req.a, req.b, &req.actor, &req.reason, None).await
}

async fn dismiss_checked<T: MergeTx + ?Sized>(
    tx: &mut T,
    a: Uuid,
    b: Uuid,
    actor: &MergeActor,
    reason: &str,
    merge: Option<Uuid>,
) -> Result<Dismissal> {
    validate_reason(reason)?;
    if a == b {
        return Err(Error::Invalid(
            "a company cannot be dismissed from itself".to_owned(),
        ));
    }
    let (ra, rb) = (root(tx, a).await?, root(tx, b).await?);
    if ra == rb {
        return Err(Error::Precondition(format!(
            "companies {a} and {b} are merged now (into {ra}); undo that merge with dismiss \
             instead"
        )));
    }
    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
    tx.insert_dismissal(&Dismissal {
        seq: 0,
        a: lo,
        b: hi,
        at: now(),
        actor: actor.clone(),
        reason: reason.to_owned(),
        merge,
    })
    .await
}

fn uniq<T: Ord>(items: &[T]) -> Vec<&T> {
    let mut seen = BTreeSet::new();
    items.iter().filter(|x| seen.insert(*x)).collect()
}

fn object(v: &Value) -> Map<String, Value> {
    match v {
        Value::Object(o) => o.clone(),
        _ => Map::new(),
    }
}

fn husk(node: &Node, into: Uuid, entry: Uuid) -> Value {
    let mut props = Map::new();
    props.insert("merged_into".into(), Value::String(into.to_string()));
    props.insert("merge".into(), Value::String(entry.to_string()));
    if let Some(name) = node.props.get("name") {
        props.insert("name".into(), name.clone());
    }
    Value::Object(props)
}

fn push(props: &mut Map<String, Value>, key: &str, v: Value) {
    match props.get_mut(key) {
        Some(Value::Array(list)) => list.push(v),
        _ => {
            props.insert(key.to_owned(), Value::Array(vec![v]));
        }
    }
}

/// Folds the absorbed company's props into the survivor's: missing keys are added, arrays
/// get the elements they lack, and one level of objects (`ids: {kind: [..]}`) the same way.
/// The survivor's scalars win. Returns the props and what was added, for [`unfold`].
fn fold(
    survivor: &Map<String, Value>,
    absorbed: &Map<String, Value>,
) -> (Map<String, Value>, Vec<Value>) {
    let mut out = survivor.clone();
    let mut added = Vec::new();
    for (k, v) in absorbed {
        if RESERVED.contains(&k.as_str()) {
            continue;
        }
        fold_at(&mut out, &[k.as_str()], v, &mut added);
    }
    (out, added)
}

fn fold_at(map: &mut Map<String, Value>, path: &[&str], v: &Value, added: &mut Vec<Value>) {
    let Some(key) = path.last() else {
        return;
    };
    match map.get_mut(*key) {
        None | Some(Value::Null) => {
            let was_null = map.contains_key(*key);
            map.insert((*key).to_owned(), v.clone());
            added.push(json!({"path": path, "value": v, "key": true, "was_null": was_null}));
        }
        Some(Value::Array(list)) => {
            if let Value::Array(more) = v {
                for x in more {
                    if !list.contains(x) {
                        list.push(x.clone());
                        added.push(json!({"path": path, "value": x}));
                    }
                }
            }
        }
        Some(Value::Object(inner)) if path.len() == 1 => {
            if let Value::Object(more) = v {
                for (k2, v2) in more {
                    fold_at(inner, &[path[0], k2.as_str()], v2, added);
                }
            }
        }
        Some(_) => {}
    }
}

/// Takes out of `props` what [`fold`] added, newest first. A value changed since is left.
fn unfold(props: &mut Map<String, Value>, added: &[Value]) {
    for a in added.iter().rev() {
        let path: Vec<&str> = a
            .get("path")
            .and_then(Value::as_array)
            .map(|p| p.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let Some(value) = a.get("value") else {
            continue;
        };
        let (map, key) = match path.as_slice() {
            [k] => (&mut *props, *k),
            [k, k2] => match props.get_mut(*k) {
                Some(Value::Object(inner)) => (inner, *k2),
                _ => continue,
            },
            _ => continue,
        };
        let whole = a.get("key").and_then(Value::as_bool).unwrap_or(false);
        if whole {
            let was_null = a.get("was_null").and_then(Value::as_bool).unwrap_or(false);
            let restore = |map: &mut Map<String, Value>| {
                if was_null {
                    map.insert(key.to_owned(), Value::Null);
                } else {
                    map.remove(key);
                }
            };
            match map.get_mut(key) {
                Some(cur) if cur == value => restore(map),
                Some(Value::Array(list)) => {
                    if let Value::Array(vals) = value {
                        for x in vals {
                            if let Some(i) = list.iter().position(|y| y == x) {
                                list.remove(i);
                            }
                        }
                        if list.is_empty() {
                            restore(map);
                        }
                    }
                }
                _ => {}
            }
        } else if let Some(Value::Array(list)) = map.get_mut(key)
            && let Some(i) = list.iter().position(|y| y == value)
        {
            list.remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_and_unfold_round_trip() {
        let s = json!({"name": "ACME", "names": ["ACME"], "ids": {"rs_pib": ["1"]}, "legal_form": null});
        let a = json!({"name": "Acme Group", "names": ["Acme Group", "ACME"],
                       "ids": {"rs_pib": ["2"], "lei": ["L"]}, "legal_form": "doo",
                       "jurisdiction": "RS", "merged_into": "x"});
        let (folded, added) = fold(&object(&s), &object(&a));
        assert_eq!(
            Value::Object(folded.clone()),
            json!({"name": "ACME", "names": ["ACME", "Acme Group"],
                   "ids": {"rs_pib": ["1", "2"], "lei": ["L"]}, "legal_form": "doo",
                   "jurisdiction": "RS"})
        );
        let mut back = folded;
        unfold(&mut back, &added);
        assert_eq!(Value::Object(back), s);
    }

    #[test]
    fn unfold_keeps_what_changed_since() {
        let s = json!({"names": ["A"]});
        let a = json!({"names": ["B"], "tags": ["t"]});
        let (mut folded, added) = fold(&object(&s), &object(&a));
        // Later writes: another name, another tag.
        folded["names"].as_array_mut().unwrap().push(json!("C"));
        folded["tags"].as_array_mut().unwrap().push(json!("u"));
        unfold(&mut folded, &added);
        assert_eq!(
            Value::Object(folded),
            json!({"names": ["A", "C"], "tags": ["u"]})
        );
    }

    #[test]
    fn requests_are_checked() {
        let actor = MergeActor {
            kind: MergeActorKind::Manual,
            instance: InstanceId::new("a").unwrap(),
        };
        let (x, y) = (Uuid::now_v7(), Uuid::now_v7());
        let ok = MergeRequest {
            survivor: x,
            absorbed: y,
            actor: actor.clone(),
            evidence: json!([]),
            confidence: 1.0,
            probability: 0.95,
        };
        assert!(ok.validate().is_ok());
        assert!(
            MergeRequest {
                absorbed: x,
                ..ok.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            MergeRequest {
                probability: 1.5,
                ..ok.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            MergeRequest {
                evidence: json!({}),
                ..ok
            }
            .validate()
            .is_err()
        );
        let split = SplitRequest {
            company: x,
            key: None,
            props: json!({}),
            aliases: vec![],
            documents: vec![],
            edges: vec![],
            actor,
            evidence: json!([]),
            dismiss: false,
        };
        assert!(split.validate().is_err());
        assert_eq!(merges_limit(0), 50);
        assert_eq!(merges_limit(5000), MERGES_MAX_LIMIT);
    }
}
