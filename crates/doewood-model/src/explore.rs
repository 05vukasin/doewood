//! Graph reads for a visual explorer (issue #172): companies with their counts, the
//! company-to-company graph, subgraphs around seeds and shortest paths. Every one is bounded:
//! a page, or caps on depth, nodes and edges that end it with `truncated`.
//!
//! **The company graph's counts** (a company's documents and degree, and the weight of each
//! pair of companies) are kept in `company_stats` and `company_links`, so a read never
//! aggregates the `party` edges. Writers do not touch them: a trigger on `edges` appends
//! each change to a live `party` or corporate-event edge to `company_graph_log` (one row per
//! change, never updated, so writers never wait for each other there), and
//! [`crate::GraphStore::fold_company_graph`] folds the log into the counts, one transaction a
//! batch, under a lock only folds take. The explorer's reads fold first, and every instance
//! folds in the background, so the counts trail the edges by at most a few seconds.
//!
//! A fold reads, in one snapshot, every log row of a batch's nodes and their live edges now.
//! What was counted for a node before is then its edges now minus the logged changes, and
//! the counts move by the difference ([`fold_deltas`]).
//!
//! **The document total** (issue #181) is kept the same way: the number of documents with at
//! least one live `party` edge, each once however many companies it names. A fold moves it
//! by the batch's documents that gained their first party or lost their last.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use serde_json::{Map, Value};
use time::Date;
use uuid::Uuid;

use crate::{EdgeKind, Error, Neighbour, NeighbourQuery, Node, NodeKind, Result, StoredEdge};

/// The corporate-event edge kinds (ADR 0002 section 3): company to company.
pub const CORPORATE_KINDS: &[EdgeKind] = &[
    EdgeKind::ParentOf,
    EdgeKind::SubsidiaryOf,
    EdgeKind::Acquired,
    EdgeKind::MergedInto,
    EdgeKind::RenamedTo,
    EdgeKind::BranchOf,
    EdgeKind::TradingNameOf,
    EdgeKind::FormerParentOf,
    EdgeKind::Liquidated,
];

/// A document with more distinct parties than this links none of them to each other (a
/// list or register, not a deal between them); its parties' document counts still count it.
/// It also bounds a fold's work per document (at most 496 pairs).
pub const LINK_PARTIES_MAX: usize = 32;
/// Longest role kept from a `party` edge's `role` (in characters).
pub const ROLE_MAX_CHARS: usize = 64;
/// Most distinct `(side, role)` counts one pair keeps; later new roles are not counted.
pub const LINK_ROLES_MAX: usize = 16;
/// Log rows a fold reads per batch (and all other log rows of those rows' nodes).
pub const FOLD_BATCH: u32 = 1000;
/// Batches a read folds before it answers: past this the counts are served as they are and
/// the answer says `stale`.
pub const FOLD_READ_BATCHES: u32 = 20;

/// Longest name prefix a filter takes, in bytes.
pub const NAME_PREFIX_MAX_LEN: usize = 200;
/// Longest jurisdiction a filter takes, in bytes.
pub const JURISDICTION_MAX_LEN: usize = 16;

/// `CompanyGraph`: companies by default and at most.
pub const COMPANY_GRAPH_NODES_DEFAULT: u32 = 500;
/// `CompanyGraph`: most companies.
pub const COMPANY_GRAPH_NODES_MAX: u32 = 2000;
/// `CompanyGraph`: links by default.
pub const COMPANY_GRAPH_LINKS_DEFAULT: u32 = 5000;
/// `CompanyGraph`: most links.
pub const COMPANY_GRAPH_LINKS_MAX: u32 = 20_000;
/// `CompanyGraph`: most corporate-event edges read per company.
pub const COMPANY_GRAPH_EVENTS_PER_COMPANY: u32 = 50;

/// `Subgraph`: the deepest walk.
pub const SUBGRAPH_DEPTH_MAX: u32 = 3;
/// `Subgraph`: most seeds.
pub const SUBGRAPH_SEEDS_MAX: usize = 100;
/// `Subgraph`: nodes by default.
pub const SUBGRAPH_NODES_DEFAULT: u32 = 200;
/// `Subgraph`: most nodes.
pub const SUBGRAPH_NODES_MAX: u32 = 2000;
/// `Subgraph`: edges by default.
pub const SUBGRAPH_EDGES_DEFAULT: u32 = 1000;
/// `Subgraph`: most edges.
pub const SUBGRAPH_EDGES_MAX: u32 = 10_000;

/// `Path`: the default and the deepest search, in edges.
pub const PATH_DEPTH_DEFAULT: u32 = 4;
/// `Path`: the deepest search.
pub const PATH_DEPTH_MAX: u32 = 6;
/// `Path`: most paths returned.
pub const PATH_PATHS_MAX: u32 = 10;
/// `Path`: most nodes a search visits, both sides together.
pub const PATH_VISITED_MAX: usize = 5000;
/// `Path`: most edges read per level of one side.
pub const PATH_LEVEL_EDGES: u32 = 20_000;

/// Most neighbours read for one node in one step of a walk.
pub const STEP_PER_NODE_MAX: u32 = 999;

/// The bytes of props and evidence a walk (`Subgraph`, `Path`, a `CompanyGraph`'s events)
/// reads at most; reaching it ends the walk as truncated.
pub const WALK_BYTES: usize = 8 << 20;

/// `value` when it is in `1..=max`, `default` for 0, else `max`.
pub fn cap(value: u32, default: u32, max: u32) -> u32 {
    match value {
        0 => default,
        n => n.min(max),
    }
}

/// Which companies a listing or the company graph takes. Merged-away companies (husks) are
/// never listed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompanyFilter {
    /// The name starts with this, ignoring case (lowercased; on SQLite only ASCII letters).
    pub name_prefix: Option<String>,
    /// The `jurisdiction` prop is this, ignoring ASCII case, or one of its subdivisions: `US`
    /// finds `US` and `US-CA`.
    pub jurisdiction: Option<String>,
    /// It owns an identifier alias (`id:<kind>:<value>`), or owns none.
    pub has_identifier: Option<bool>,
    /// The enrichment job enriched it at least once, or never.
    pub enriched: Option<bool>,
}

impl CompanyFilter {
    /// Checks the lengths.
    pub fn validate(&self) -> Result<()> {
        if self
            .name_prefix
            .as_ref()
            .is_some_and(|p| p.len() > NAME_PREFIX_MAX_LEN || p.contains('\0'))
        {
            return Err(Error::Invalid(format!(
                "a name prefix is at most {NAME_PREFIX_MAX_LEN} bytes, without NUL"
            )));
        }
        if self
            .jurisdiction
            .as_ref()
            .is_some_and(|j| j.len() > JURISDICTION_MAX_LEN || j.contains('\0'))
        {
            return Err(Error::Invalid(format!(
                "a jurisdiction is at most {JURISDICTION_MAX_LEN} bytes, without NUL"
            )));
        }
        Ok(())
    }

    /// The jurisdiction as compared: uppercased.
    pub fn jurisdiction_key(&self) -> Option<String> {
        self.jurisdiction
            .as_deref()
            .filter(|j| !j.is_empty())
            .map(str::to_ascii_uppercase)
    }
}

/// The order of a company listing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CompanyOrder {
    /// Most documents first, then by id, descending.
    #[default]
    Documents,
    /// Highest degree first, then by id, descending.
    Degree,
    /// By lowercased name (byte order; no name first), then by id.
    Name,
}

impl CompanyOrder {
    fn tag(self) -> &'static str {
        match self {
            Self::Documents => "d",
            Self::Degree => "g",
            Self::Name => "n",
        }
    }
}

/// A place in a company listing: the last company's sort key and id. As text (a page token)
/// it is `<d|g|n>.<id>.<documents, degree or name key>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompanyCursor {
    /// The listing's order.
    pub order: CompanyOrder,
    /// The company's id.
    pub id: Uuid,
    /// Its documents or degree (`Documents`, `Degree`).
    pub count: u64,
    /// Its name key (`Name`).
    pub name: String,
}

impl CompanyCursor {
    /// The place of `row` in `order`.
    pub fn of(order: CompanyOrder, row: &CompanyRow) -> Self {
        Self {
            order,
            id: row.node.id,
            count: match order {
                CompanyOrder::Documents => row.documents,
                CompanyOrder::Degree => row.degree,
                CompanyOrder::Name => 0,
            },
            name: if order == CompanyOrder::Name {
                row.name_key.clone()
            } else {
                String::new()
            },
        }
    }
}

impl fmt::Display for CompanyCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.", self.order.tag(), self.id.as_hyphenated())?;
        match self.order {
            CompanyOrder::Name => f.write_str(&self.name),
            _ => write!(f, "{}", self.count),
        }
    }
}

impl FromStr for CompanyCursor {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let bad = || Error::Invalid("not a company listing position".to_owned());
        let mut parts = s.splitn(3, '.');
        let (Some(tag), Some(id), Some(key)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(bad());
        };
        let order = match tag {
            "d" => CompanyOrder::Documents,
            "g" => CompanyOrder::Degree,
            "n" => CompanyOrder::Name,
            _ => return Err(bad()),
        };
        let id = Uuid::try_parse(id).map_err(|_| bad())?;
        let (count, name) = if order == CompanyOrder::Name {
            if key.len() > NAME_KEY_MAX_LEN {
                return Err(bad());
            }
            (0, key.to_owned())
        } else {
            (key.parse().map_err(|_| bad())?, String::new())
        };
        Ok(Self {
            order,
            id,
            count,
            name,
        })
    }
}

/// Longest name key a cursor carries, in bytes.
const NAME_KEY_MAX_LEN: usize = 4096;

/// A company with its counts, as listed.
#[derive(Clone, Debug, PartialEq)]
pub struct CompanyRow {
    /// The company node.
    pub node: Node,
    /// Documents with a live `party` edge to it.
    pub documents: u64,
    /// Companies it is linked to: by a shared document or a live corporate-event edge.
    pub degree: u64,
    /// Enriched at least once.
    pub enriched: bool,
    /// Its identifier aliases (`id:<kind>:<value>`), in byte order; at most
    /// [`COMPANY_IDENTIFIERS_MAX`].
    pub identifiers: Vec<String>,
    /// Its name key as the backend's name order compares it: the `name` prop lowercased
    /// (on SQLite only ASCII letters), empty when it has none.
    pub name_key: String,
}

/// Most identifiers listed per company.
pub const COMPANY_IDENTIFIERS_MAX: usize = 32;

/// What a company row counts against a page's byte budget.
pub fn company_row_bytes(row: &CompanyRow) -> usize {
    crate::node_page_bytes(&row.node) + row.identifiers.iter().map(String::len).sum::<usize>()
}

/// One side's role in a pair's shared documents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkRole {
    /// The company.
    pub company: Uuid,
    /// Its role (`buyer`, ...); empty when the edge has none.
    pub role: String,
    /// Shared documents where it has that role.
    pub documents: u64,
}

/// A pair of companies of the company graph.
#[derive(Clone, Debug, PartialEq)]
pub struct CompanyLink {
    /// The smaller id.
    pub a: Uuid,
    /// The larger id.
    pub b: Uuid,
    /// Documents both are parties of (the weight).
    pub documents: u64,
    /// Live corporate-event edges between them, either way.
    pub events: u64,
    /// Their roles in the shared documents.
    pub roles: Vec<LinkRole>,
}

/// What a fold did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Folded {
    /// Log rows folded.
    pub rows: u64,
    /// Rows are left: the call stopped at its budget.
    pub more: bool,
}

/// A logged change to a live `party` or corporate edge: `delta` is +1 when the edge became
/// live (or was written live), -1 when it stopped being live (or was deleted).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// The edge's source: a document (party) or a company (event).
    pub node: Uuid,
    /// Its target.
    pub other: Uuid,
    /// A `party` edge; else a corporate event.
    pub party: bool,
    /// The role (party: its `role` prop as text, at most [`ROLE_MAX_CHARS`] characters, empty
    /// when none) or the edge kind (event).
    pub tag: String,
    /// +1 or -1.
    pub delta: i32,
}

/// A live `party` or corporate edge from a folded node now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    /// The edge's source.
    pub node: Uuid,
    /// Its target.
    pub other: Uuid,
    /// A `party` edge.
    pub party: bool,
    /// As [`LogEntry::tag`].
    pub tag: String,
}

/// A pair's counts, as stored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LinkState {
    /// Shared documents.
    pub documents: i64,
    /// Corporate events.
    pub events: i64,
    /// `a:<role>` / `b:<role>` to documents.
    pub roles: BTreeMap<String, i64>,
}

impl LinkState {
    /// The roles as stored: a JSON object.
    pub fn roles_json(&self) -> String {
        let m: Map<String, Value> = self
            .roles
            .iter()
            .map(|(k, v)| (k.clone(), Value::from(*v)))
            .collect();
        Value::Object(m).to_string()
    }

    /// Reads the stored roles.
    pub fn parse_roles(text: &str) -> BTreeMap<String, i64> {
        match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(m)) => m
                .into_iter()
                .filter_map(|(k, v)| v.as_i64().map(|n| (k, n)))
                .collect(),
            _ => BTreeMap::new(),
        }
    }

    /// The pair as returned, `a < b`.
    pub fn link(&self, a: Uuid, b: Uuid) -> CompanyLink {
        let roles = self
            .roles
            .iter()
            .filter(|(_, n)| **n > 0)
            .filter_map(|(k, n)| {
                let (side, role) = k.split_once(':')?;
                let company = if side == "a" { a } else { b };
                Some(LinkRole {
                    company,
                    role: role.to_owned(),
                    documents: u64::try_from(*n).unwrap_or(0),
                })
            })
            .collect();
        CompanyLink {
            a,
            b,
            documents: u64::try_from(self.documents).unwrap_or(0),
            events: u64::try_from(self.events).unwrap_or(0),
            roles,
        }
    }
}

/// How a fold moves the counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FoldDelta {
    /// The change in documents with a live `party` edge, each once (the document total).
    pub documents_total: i64,
    /// Company to its change in documents.
    pub documents: BTreeMap<Uuid, i64>,
    /// Pair `(a, b)`, `a < b`, to its change.
    pub links: BTreeMap<(Uuid, Uuid), LinkState>,
}

/// What a fold writes: each touched pair's new state (`None`: delete it), and each company's
/// change in documents and degree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FoldPlan {
    /// The pairs to write.
    pub links: Vec<((Uuid, Uuid), Option<LinkState>)>,
    /// Company to `(documents, degree)` changes; only non-zero ones.
    pub stats: BTreeMap<Uuid, (i64, i64)>,
}

/// A node's live edges as counted: party targets with their roles, event targets with
/// their number of edges.
#[derive(Default)]
struct Counted {
    party: BTreeMap<Uuid, BTreeMap<String, i64>>,
    events: BTreeMap<Uuid, i64>,
}

impl Counted {
    fn add(&mut self, other: Uuid, party: bool, tag: &str, n: i64) {
        if party {
            *self
                .party
                .entry(other)
                .or_default()
                .entry(tag.to_owned())
                .or_default() += n;
        } else {
            *self.events.entry(other).or_default() += n;
        }
    }

    /// Drops what is not positive (a count can only go below zero if the log and the edges
    /// disagree, which the fold's snapshot rules out).
    fn settle(&mut self) {
        for roles in self.party.values_mut() {
            roles.retain(|_, n| *n > 0);
        }
        self.party.retain(|_, roles| !roles.is_empty());
        self.events.retain(|_, n| *n > 0);
    }
}

fn pair(x: Uuid, y: Uuid) -> ((Uuid, Uuid), bool) {
    if x < y {
        ((x, y), false)
    } else {
        ((y, x), true)
    }
}

/// Adds `sign` times what `node`'s counted edges contribute.
fn contribute(delta: &mut FoldDelta, node: Uuid, c: &Counted, sign: i64) {
    if !c.party.is_empty() {
        delta.documents_total += sign;
    }
    for &company in c.party.keys() {
        *delta.documents.entry(company).or_default() += sign;
    }
    let parties: Vec<(&Uuid, &BTreeMap<String, i64>)> =
        c.party.iter().filter(|(co, _)| **co != node).collect();
    if parties.len() <= LINK_PARTIES_MAX {
        for (i, &(x, x_roles)) in parties.iter().enumerate() {
            for &(y, y_roles) in &parties[i + 1..] {
                let (key, swapped) = pair(*x, *y);
                let l = delta.links.entry(key).or_default();
                l.documents += sign;
                let (a_roles, b_roles) = if swapped {
                    (y_roles, x_roles)
                } else {
                    (x_roles, y_roles)
                };
                for r in a_roles.keys() {
                    *l.roles.entry(format!("a:{r}")).or_default() += sign;
                }
                for r in b_roles.keys() {
                    *l.roles.entry(format!("b:{r}")).or_default() += sign;
                }
            }
        }
    }
    for (&other, &n) in &c.events {
        if other == node {
            continue;
        }
        let (key, _) = pair(node, other);
        delta.links.entry(key).or_default().events += sign * n;
    }
}

/// How the counts move for the nodes in `log`: `current` holds their live edges now, read in
/// the same snapshot as `log` (every log row of those nodes). Before, they had `current`
/// minus the logged changes.
pub fn fold_deltas(log: &[LogEntry], current: &[Member]) -> FoldDelta {
    let mut now: BTreeMap<Uuid, Counted> = BTreeMap::new();
    let mut before: BTreeMap<Uuid, Counted> = BTreeMap::new();
    for e in log {
        now.entry(e.node).or_default();
        before
            .entry(e.node)
            .or_default()
            .add(e.other, e.party, &e.tag, -i64::from(e.delta));
    }
    for m in current {
        if !now.contains_key(&m.node) {
            continue;
        }
        now.entry(m.node)
            .or_default()
            .add(m.other, m.party, &m.tag, 1);
        before
            .entry(m.node)
            .or_default()
            .add(m.other, m.party, &m.tag, 1);
    }
    let mut delta = FoldDelta::default();
    for (node, mut c) in now {
        c.settle();
        contribute(&mut delta, node, &c, 1);
    }
    for (node, mut c) in before {
        c.settle();
        contribute(&mut delta, node, &c, -1);
    }
    delta.documents.retain(|_, n| *n != 0);
    for l in delta.links.values_mut() {
        l.roles.retain(|_, n| *n != 0);
    }
    delta
        .links
        .retain(|_, l| l.documents != 0 || l.events != 0 || !l.roles.is_empty());
    delta
}

/// The writes of a fold: `existing` holds the stored state of every pair in `delta`.
pub fn plan_fold(delta: &FoldDelta, existing: &BTreeMap<(Uuid, Uuid), LinkState>) -> FoldPlan {
    let mut plan = FoldPlan::default();
    for (&company, &n) in &delta.documents {
        plan.stats.entry(company).or_default().0 += n;
    }
    for (&key, d) in &delta.links {
        let old = existing.get(&key);
        let mut new = old.cloned().unwrap_or_default();
        new.documents = (new.documents + d.documents).max(0);
        new.events = (new.events + d.events).max(0);
        for (role, &n) in &d.roles {
            if let Some(v) = new.roles.get_mut(role) {
                *v += n;
            } else if n > 0 && new.roles.len() < LINK_ROLES_MAX {
                new.roles.insert(role.clone(), n);
            }
        }
        new.roles.retain(|_, n| *n > 0);
        let was = old.is_some_and(|o| o.documents > 0 || o.events > 0);
        let is = new.documents > 0 || new.events > 0;
        if was != is {
            let step = if is { 1 } else { -1 };
            plan.stats.entry(key.0).or_default().1 += step;
            plan.stats.entry(key.1).or_default().1 += step;
        }
        if is {
            if old != Some(&new) {
                plan.links.push((key, Some(new)));
            }
        } else if old.is_some() {
            plan.links.push((key, None));
        }
    }
    plan.stats.retain(|_, (d, g)| *d != 0 || *g != 0);
    plan
}

/// A `Subgraph` request, checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubgraphQuery {
    /// Where to start (each is included).
    pub seeds: Vec<Uuid>,
    /// How many edges away, 0 to [`SUBGRAPH_DEPTH_MAX`].
    pub depth: u32,
    /// Only nodes of these kinds besides the seeds; empty for all.
    pub node_kinds: Vec<NodeKind>,
    /// Only edges of these kinds; empty for all.
    pub edge_kinds: Vec<EdgeKind>,
    /// Most nodes.
    pub max_nodes: u32,
    /// Most edges.
    pub max_edges: u32,
    /// Only edges that hold on this day.
    pub as_of: Option<Date>,
}

/// A node of a subgraph and its distance from the nearest seed.
#[derive(Clone, Debug, PartialEq)]
pub struct Reached {
    /// The node.
    pub node: Node,
    /// Edges from the nearest seed.
    pub depth: u32,
}

/// A subgraph.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Subgraph {
    /// The nodes, seeds first, then by depth in the order reached.
    pub nodes: Vec<Reached>,
    /// The edges between them, each once.
    pub edges: Vec<StoredEdge>,
    /// A cap (nodes, edges, bytes, or a node's neighbours in one step) ended the walk before
    /// it reached `depth` everywhere.
    pub truncated: bool,
}

/// One step of a walk: the live neighbours of `nodes`, at most `per_node` of each, for each
/// kind in `kinds` (one query a kind; empty for every kind). Returns them with the node they
/// are next to, and whether a node had more.
async fn step<S: crate::GraphStore + ?Sized>(
    store: &S,
    nodes: &[Uuid],
    q: &NeighbourQuery,
    kinds: &[NodeKind],
    per_node: u32,
    max_bytes: usize,
) -> Result<(Vec<(Uuid, Neighbour)>, bool)> {
    let mut out = Vec::new();
    let mut more = false;
    let all = [None];
    let kinds: Vec<Option<NodeKind>> = if kinds.is_empty() || kinds.len() == NodeKind::ALL.len() {
        all.to_vec()
    } else {
        kinds.iter().copied().map(Some).collect()
    };
    let mut bytes = 0usize;
    for kind in kinds {
        let q = NeighbourQuery {
            node_kind: kind,
            ..q.clone()
        };
        let found = store
            .neighbours_of_many(nodes, &q, per_node + 1, max_bytes.saturating_sub(bytes))
            .await?;
        let mut per: BTreeMap<Uuid, u32> = BTreeMap::new();
        for (from, n) in found {
            bytes += crate::neighbour_page_bytes(&n);
            let c = per.entry(from).or_default();
            *c += 1;
            if *c > per_node {
                more = true;
                continue;
            }
            out.push((from, n));
        }
        if bytes >= max_bytes {
            more = true;
            break;
        }
    }
    Ok((out, more))
}

/// The other end of `e` from `from`.
fn other_end(e: &StoredEdge, from: Uuid) -> Uuid {
    if e.edge.src == from {
        e.edge.dst
    } else {
        e.edge.src
    }
}

/// Walks out from the seeds, level by level, over live edges either way (issue #172). Each
/// level reads its nodes' neighbours in one store call per node kind asked for, at most a
/// fair share of the edge budget per node. Fails with [`Error::NotFound`] when a seed does
/// not exist.
pub async fn subgraph<S: crate::GraphStore + ?Sized>(
    store: &S,
    q: &SubgraphQuery,
) -> Result<Subgraph> {
    let mut out = Subgraph::default();
    let mut seen: BTreeSet<Uuid> = BTreeSet::new();
    let mut edges: BTreeSet<Uuid> = BTreeSet::new();
    let mut bytes = 0usize;
    let mut frontier = Vec::new();
    for &id in &q.seeds {
        if !seen.insert(id) {
            continue;
        }
        let node = store
            .node(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("node {id} does not exist")))?;
        bytes += crate::node_page_bytes(&node);
        if out.nodes.len() >= q.max_nodes as usize {
            out.truncated = true;
            continue;
        }
        out.nodes.push(Reached { node, depth: 0 });
        frontier.push(id);
    }
    let nq = NeighbourQuery {
        direction: crate::Direction::Both,
        edge_kinds: q.edge_kinds.clone(),
        node_kind: None,
        at: q.as_of,
        include_inactive: false,
    };
    for depth in 1..=q.depth {
        if frontier.is_empty() || out.truncated {
            break;
        }
        let left = q
            .max_edges
            .saturating_sub(u32::try_from(edges.len()).unwrap_or(u32::MAX));
        let share = left / u32::try_from(frontier.len()).unwrap_or(u32::MAX);
        let per_node = share.clamp(1, STEP_PER_NODE_MAX);
        let (found, more) = step(
            store,
            &frontier,
            &nq,
            &q.node_kinds,
            per_node,
            WALK_BYTES.saturating_sub(bytes),
        )
        .await?;
        out.truncated |= more;
        let mut next = Vec::new();
        for (from, n) in found {
            let other = other_end(&n.edge, from);
            if edges.contains(&n.edge.id) {
                continue;
            }
            if !seen.contains(&other) {
                if out.nodes.len() >= q.max_nodes as usize {
                    out.truncated = true;
                    continue;
                }
                seen.insert(other);
                bytes += crate::node_page_bytes(&n.node);
                out.nodes.push(Reached {
                    node: n.node,
                    depth,
                });
                next.push(other);
            }
            if edges.len() >= q.max_edges as usize {
                out.truncated = true;
                continue;
            }
            bytes += crate::json_len(&n.edge.edge.props) + crate::json_len(&n.edge.edge.evidence);
            edges.insert(n.edge.id);
            out.edges.push(n.edge);
        }
        if bytes >= WALK_BYTES {
            out.truncated = true;
        }
        frontier = next;
    }
    Ok(out)
}

/// A `Path` request, checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathQuery {
    /// One end.
    pub from: Uuid,
    /// The other end.
    pub to: Uuid,
    /// Longest path, in edges: 1 to [`PATH_DEPTH_MAX`].
    pub max_depth: u32,
    /// Only edges of these kinds; empty for all.
    pub edge_kinds: Vec<EdgeKind>,
    /// Only edges that hold on this day.
    pub as_of: Option<Date>,
    /// Most paths: 1 to [`PATH_PATHS_MAX`].
    pub max_paths: u32,
}

/// A path: `nodes[i]` and `nodes[i + 1]` are the ends of `edges[i]`.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPath {
    /// From `from` to `to`.
    pub nodes: Vec<Node>,
    /// The edges, in order.
    pub edges: Vec<StoredEdge>,
}

/// What a path search found.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Paths {
    /// The shortest paths, all of the same length; empty when none was found.
    pub paths: Vec<GraphPath>,
    /// A cap (visited nodes, edges per level, bytes, a node's neighbours) cut the search, so
    /// a shorter path, or a path at all when none was found, may exist.
    pub truncated: bool,
}

/// One side of a bidirectional search: each visited node's depth and its parents (the
/// node one step nearer this side's end, and the edge).
struct Side {
    depth: BTreeMap<Uuid, u32>,
    parents: BTreeMap<Uuid, Vec<(Uuid, StoredEdge)>>,
    nodes: BTreeMap<Uuid, Node>,
    frontier: Vec<Uuid>,
    level: u32,
}

impl Side {
    fn new(start: Node) -> Self {
        let id = start.id;
        Self {
            depth: BTreeMap::from([(id, 0)]),
            parents: BTreeMap::new(),
            nodes: BTreeMap::from([(id, start)]),
            frontier: vec![id],
            level: 0,
        }
    }

    /// The paths from this side's end to `node`, as (nodes, edges) from the end; at most
    /// `max`.
    fn chains(&self, node: Uuid, max: usize) -> Vec<(Vec<Uuid>, Vec<StoredEdge>)> {
        let Some(ps) = self.parents.get(&node) else {
            return vec![(vec![node], Vec::new())];
        };
        let mut out = Vec::new();
        for (p, e) in ps {
            for (mut ns, mut es) in self.chains(*p, max - out.len()) {
                ns.push(node);
                es.push(e.clone());
                out.push((ns, es));
                if out.len() >= max {
                    return out;
                }
            }
        }
        out
    }
}

/// The shortest paths between two nodes over live edges either way (issue #172): a
/// bidirectional breadth-first search that expands the smaller frontier a level at a time,
/// within [`PATH_VISITED_MAX`] nodes and [`PATH_LEVEL_EDGES`] edges a level. Fails with
/// [`Error::NotFound`] when an end does not exist.
#[allow(clippy::too_many_lines)]
pub async fn shortest_paths<S: crate::GraphStore + ?Sized>(
    store: &S,
    q: &PathQuery,
) -> Result<Paths> {
    let get = async |id: Uuid| {
        store
            .node(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("node {id} does not exist")))
    };
    let from = get(q.from).await?;
    let to = get(q.to).await?;
    if q.from == q.to {
        return Ok(Paths {
            paths: vec![GraphPath {
                nodes: vec![from],
                edges: Vec::new(),
            }],
            truncated: false,
        });
    }
    let mut sides = [Side::new(from), Side::new(to)];
    let nq = NeighbourQuery {
        direction: crate::Direction::Both,
        edge_kinds: q.edge_kinds.clone(),
        node_kind: None,
        at: q.as_of,
        include_inactive: false,
    };
    let mut truncated = false;
    let mut bytes = 0usize;
    let mut meets: Vec<Uuid> = Vec::new();
    while sides[0].level + sides[1].level < q.max_depth {
        // The smaller frontier grows; an empty one means no path.
        let s = usize::from(sides[1].frontier.len() < sides[0].frontier.len());
        if sides[s].frontier.is_empty() {
            break;
        }
        let visited = sides[0].depth.len() + sides[1].depth.len();
        if visited >= PATH_VISITED_MAX || bytes >= WALK_BYTES {
            truncated = true;
            break;
        }
        let frontier = std::mem::take(&mut sides[s].frontier);
        let share = PATH_LEVEL_EDGES / u32::try_from(frontier.len()).unwrap_or(u32::MAX);
        let per_node = share.clamp(1, STEP_PER_NODE_MAX);
        let (found, more) = step(
            store,
            &frontier,
            &nq,
            &[],
            per_node,
            WALK_BYTES.saturating_sub(bytes),
        )
        .await?;
        truncated |= more;
        let level = sides[s].level + 1;
        sides[s].level = level;
        let mut next = Vec::new();
        for (parent, n) in found {
            let other = other_end(&n.edge, parent);
            if other == parent {
                continue;
            }
            let side = &mut sides[s];
            match side.depth.get(&other) {
                Some(&d) if d < level => continue,
                Some(_) => {}
                None => {
                    if side.depth.len() + sides[1 - s].depth.len() >= PATH_VISITED_MAX {
                        truncated = true;
                        continue;
                    }
                    let side = &mut sides[s];
                    side.depth.insert(other, level);
                    bytes += crate::node_page_bytes(&n.node);
                    side.nodes.insert(other, n.node.clone());
                    next.push(other);
                }
            }
            bytes += crate::json_len(&n.edge.edge.props) + crate::json_len(&n.edge.edge.evidence);
            let side = &mut sides[s];
            let ps = side.parents.entry(other).or_default();
            if ps.len() < PATH_PATHS_MAX as usize {
                ps.push((parent, n.edge));
            }
        }
        sides[s].frontier = next;
        // Met: the nodes this level reached that the other side has, at the least total.
        let best = sides[s]
            .frontier
            .iter()
            .filter_map(|id| sides[1 - s].depth.get(id).map(|d| (d + level, *id)))
            .min_by_key(|(t, _)| *t)
            .map(|(t, _)| t);
        if let Some(best) = best {
            meets = sides[s]
                .frontier
                .iter()
                .filter(|id| sides[1 - s].depth.get(id).map(|d| d + level) == Some(best))
                .copied()
                .collect();
            break;
        }
    }
    let max = q.max_paths as usize;
    let mut paths = Vec::new();
    'meets: for m in meets {
        for (fwd_nodes, fwd_edges) in sides[0].chains(m, max) {
            for (back_nodes, back_edges) in sides[1].chains(m, max) {
                let mut ids = fwd_nodes.clone();
                ids.extend(back_nodes.iter().rev().skip(1));
                let mut es = fwd_edges.clone();
                es.extend(back_edges.iter().rev().cloned());
                let nodes = ids
                    .iter()
                    .map(|id| {
                        sides[0]
                            .nodes
                            .get(id)
                            .or_else(|| sides[1].nodes.get(id))
                            .cloned()
                            .ok_or_else(|| Error::Invalid(format!("path node {id} lost")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                paths.push(GraphPath { nodes, edges: es });
                if paths.len() >= max {
                    break 'meets;
                }
            }
        }
    }
    Ok(Paths { paths, truncated })
}

/// The company graph: the first companies in document order (as listed), the links between
/// them, strongest first, and the corporate-event edges of those links.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompanyGraph {
    /// The companies.
    pub companies: Vec<CompanyRow>,
    /// The links: `documents >= min_weight`, or with events.
    pub links: Vec<CompanyLink>,
    /// The live corporate-event edges between the companies, by the link they belong to.
    pub events: BTreeMap<(Uuid, Uuid), Vec<StoredEdge>>,
    /// Documents with a live `party` edge in the whole graph, each once, as last folded (not
    /// narrowed by the filter or the caps).
    pub documents_total: u64,
    /// More companies matched than the limit, more links than their cap, or a company had
    /// more events than read.
    pub truncated: bool,
    /// The counts trail the edges: changes are still waiting to be folded.
    pub stale: bool,
}

/// Folds what a read may fold first ([`FOLD_READ_BATCHES`]); returns whether the counts are
/// still behind.
pub async fn fold_for_read<S: crate::GraphStore + ?Sized>(store: &S) -> Result<bool> {
    for _ in 0..FOLD_READ_BATCHES {
        let f = store.fold_company_graph(FOLD_BATCH).await?;
        if !f.more {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The company graph (issue #172), after a fold: at most `max_nodes` companies matching
/// `filter`, most documents first, and at most `max_links` links among them.
pub async fn company_graph<S: crate::GraphStore + ?Sized>(
    store: &S,
    filter: &CompanyFilter,
    min_weight: u32,
    max_nodes: u32,
    max_links: u32,
) -> Result<CompanyGraph> {
    let stale = fold_for_read(store).await?;
    let mut companies = store
        .list_companies(
            filter,
            CompanyOrder::Documents,
            None,
            max_nodes + 1,
            WALK_BYTES,
        )
        .await?;
    let mut truncated = companies.len() > max_nodes as usize;
    companies.truncate(max_nodes as usize);
    let ids: Vec<Uuid> = companies.iter().map(|c| c.node.id).collect();
    let mut links = store
        .company_links(&ids, min_weight.max(1), max_links + 1)
        .await?;
    truncated |= links.len() > max_links as usize;
    links.truncate(max_links as usize);
    let with_events: Vec<Uuid> = {
        let mut s = BTreeSet::new();
        for l in links.iter().filter(|l| l.events > 0) {
            s.insert(l.a);
            s.insert(l.b);
        }
        s.into_iter().collect()
    };
    let mut events: BTreeMap<(Uuid, Uuid), Vec<StoredEdge>> = BTreeMap::new();
    if !with_events.is_empty() {
        let keys: BTreeSet<(Uuid, Uuid)> = links
            .iter()
            .filter(|l| l.events > 0)
            .map(|l| (l.a, l.b))
            .collect();
        let q = NeighbourQuery {
            direction: crate::Direction::Out,
            edge_kinds: CORPORATE_KINDS.to_vec(),
            node_kind: Some(NodeKind::Company),
            at: None,
            include_inactive: false,
        };
        let (found, more) = step(
            store,
            &with_events,
            &q,
            &[NodeKind::Company],
            COMPANY_GRAPH_EVENTS_PER_COMPANY,
            WALK_BYTES,
        )
        .await?;
        truncated |= more;
        for (_, n) in found {
            let (key, _) = pair(n.edge.edge.src, n.edge.edge.dst);
            if keys.contains(&key) {
                events.entry(key).or_default().push(n.edge);
            }
        }
    }
    let documents_total = store.company_documents_total().await?;
    Ok(CompanyGraph {
        companies,
        links,
        events,
        documents_total,
        truncated,
        stale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn log(node: u128, other: u128, tag: &str, delta: i32) -> LogEntry {
        LogEntry {
            node: id(node),
            other: id(other),
            party: true,
            tag: tag.to_owned(),
            delta,
        }
    }

    fn member(node: u128, other: u128, tag: &str) -> Member {
        Member {
            node: id(node),
            other: id(other),
            party: true,
            tag: tag.to_owned(),
        }
    }

    #[test]
    fn a_new_document_links_its_parties() {
        let d = fold_deltas(
            &[log(1, 10, "buyer", 1), log(1, 11, "supplier", 1)],
            &[member(1, 10, "buyer"), member(1, 11, "supplier")],
        );
        assert_eq!(d.documents, BTreeMap::from([(id(10), 1), (id(11), 1)]));
        let l = &d.links[&(id(10), id(11))];
        assert_eq!((l.documents, l.events), (1, 0));
        assert_eq!(
            l.roles,
            BTreeMap::from([("a:buyer".to_owned(), 1), ("b:supplier".to_owned(), 1)])
        );
        let plan = plan_fold(&d, &BTreeMap::new());
        assert_eq!(
            plan.stats,
            BTreeMap::from([(id(10), (1, 1)), (id(11), (1, 1))])
        );
        assert_eq!(plan.links.len(), 1);
    }

    #[test]
    fn a_second_edge_to_the_same_company_counts_once() {
        // Before: 1 -> 10 (buyer). Now a second edge 1 -> 10 (signatory) was added.
        let d = fold_deltas(
            &[log(1, 10, "signatory", 1)],
            &[member(1, 10, "buyer"), member(1, 10, "signatory")],
        );
        assert!(d.documents.is_empty(), "{d:?}");
        assert!(d.links.is_empty());
    }

    #[test]
    fn a_moved_edge_moves_the_link() {
        // A merge moved 1 -> 10 to 1 -> 12; 1 -> 11 stays.
        let d = fold_deltas(
            &[log(1, 10, "buyer", -1), log(1, 12, "buyer", 1)],
            &[member(1, 12, "buyer"), member(1, 11, "supplier")],
        );
        assert_eq!(d.documents, BTreeMap::from([(id(10), -1), (id(12), 1)]));
        assert_eq!(d.links[&(id(10), id(11))].documents, -1);
        assert_eq!(d.links[&(id(11), id(12))].documents, 1);
        let existing = BTreeMap::from([(
            (id(10), id(11)),
            LinkState {
                documents: 1,
                events: 0,
                roles: BTreeMap::from([("a:buyer".to_owned(), 1), ("b:supplier".to_owned(), 1)]),
            },
        )]);
        let plan = plan_fold(&d, &existing);
        assert!(plan.links.contains(&((id(10), id(11)), None)));
        assert_eq!(plan.stats[&id(10)], (-1, -1));
        // 11 lost the link to 10 and gained the one to 12.
        assert!(!plan.stats.contains_key(&id(11)));
        assert_eq!(plan.stats[&id(12)], (1, 1));
    }

    /// Issue #181: a document counts once in the total, however many companies it names, and
    /// only when it gains its first party edge or loses its last.
    #[test]
    fn the_document_total_counts_a_document_once() {
        let new = fold_deltas(
            &[
                log(1, 10, "buyer", 1),
                log(1, 11, "supplier", 1),
                log(1, 12, "signatory", 1),
                log(2, 10, "buyer", 1),
            ],
            &[
                member(1, 10, "buyer"),
                member(1, 11, "supplier"),
                member(1, 12, "signatory"),
                member(2, 10, "buyer"),
            ],
        );
        assert_eq!(new.documents_total, 2);
        // One of three parties retracted, and an edge moved: still counted.
        let d = fold_deltas(
            &[
                log(1, 12, "signatory", -1),
                log(2, 10, "buyer", -1),
                log(2, 11, "buyer", 1),
            ],
            &[
                member(1, 10, "buyer"),
                member(1, 11, "supplier"),
                member(2, 11, "buyer"),
            ],
        );
        assert_eq!(d.documents_total, 0);
        // The last party edge retracted: the document leaves the total.
        let d = fold_deltas(&[log(2, 11, "buyer", -1)], &[]);
        assert_eq!(d.documents_total, -1);
        // A corporate event is no document.
        let d = fold_deltas(
            &[LogEntry {
                party: false,
                ..log(20, 10, "parent_of", 1)
            }],
            &[Member {
                party: false,
                ..member(20, 10, "parent_of")
            }],
        );
        assert_eq!(d.documents_total, 0);
    }

    #[test]
    fn events_count_both_ways_and_skip_self_loops() {
        let ev = |node, other, delta| LogEntry {
            node: id(node),
            other: id(other),
            party: false,
            tag: "parent_of".to_owned(),
            delta,
        };
        let m = |node, other| Member {
            node: id(node),
            other: id(other),
            party: false,
            tag: "parent_of".to_owned(),
        };
        let d = fold_deltas(&[ev(20, 10, 1), ev(10, 10, 1)], &[m(20, 10), m(10, 10)]);
        assert_eq!(d.links.len(), 1);
        assert_eq!(d.links[&(id(10), id(20))].events, 1);
        assert!(d.documents.is_empty());
    }

    #[test]
    fn a_crowded_document_links_nobody() {
        let n = u128::try_from(LINK_PARTIES_MAX).unwrap() + 1;
        let log: Vec<_> = (0..n).map(|c| log(1, 100 + c, "", 1)).collect();
        let cur: Vec<_> = (0..n).map(|c| member(1, 100 + c, "")).collect();
        let d = fold_deltas(&log, &cur);
        assert_eq!(d.documents.len(), LINK_PARTIES_MAX + 1);
        assert!(d.links.is_empty());
    }

    #[test]
    fn cursors_read_back() {
        for c in [
            CompanyCursor {
                order: CompanyOrder::Documents,
                id: id(7),
                count: 42,
                name: String::new(),
            },
            CompanyCursor {
                order: CompanyOrder::Name,
                id: id(8),
                count: 0,
                name: "acme. d.o.o.".to_owned(),
            },
        ] {
            assert_eq!(c.to_string().parse::<CompanyCursor>().unwrap(), c);
        }
        assert!("x.1.2".parse::<CompanyCursor>().is_err());
        assert!("d.not-a-uuid.2".parse::<CompanyCursor>().is_err());
    }
}
