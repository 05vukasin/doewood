//! What the registries said, into the graph: identifiers, props, dated edges, and the
//! "later dated parent is current, the other becomes the former parent" rule of ADR 0002
//! section 3.
//!
//! - **Identifiers:** each identifier a registry gave becomes an alias of the company
//!   (`id:<kind>:<value>`, `reg:companies_house:<n>`, `reg:edgar:<cik>`,
//!   `reg:wikidata:<Q-id>`). The first claim wins, so one another company owns stays its own
//!   (the gate has merged the two or recorded a proposal).
//! - **Former names** become learned aliases, and a dated rename a `renamed_to` edge on the
//!   company itself, from the old name to the new.
//! - **Edges** go to the other company: by LEI or Companies House number (created, keyed by
//!   that identifier, when no company has it), by Wikidata item or by name (only when a company
//!   already has it). An edge to a company that cannot be found is dropped.
//! - **Props:** `registry.<name>` (each registry's facts and fetch date), `ids`,
//!   `registry_ids`, `legal_name`, `former_names` and `enriched_at`. The props lists of
//!   proposals before schema 5 (`merge_proposals`, `registry_proposals`) are removed.
//! - **Proposals** go to the proposals table (`ProposalStore::propose`, issue #52).
//!
//! The order: edges first (onto live companies, so a merge committed since the company was
//! resolved moves them to the survivor, issue #60), then the company's own aliases and props,
//! with the live variants (`claim_live_alias`, `update_live_props`, issue #62): when a person
//! merged the company away while the job ran, they fail with `Error::MergedAway` and the job
//! stops for it ([`super::company::Job::run`]); nothing lands on the husk.

// `b` (the backend), `s` (the company) and `f` (the findings) are used throughout.
#![allow(clippy::many_single_char_names)]

use std::collections::BTreeSet;

use doewood_enrich::{CompanyName, LadderStep};
use doewood_model::{
    Backend, Direction, Edge, EdgeKind, EdgeSource, EdgeState, NeighbourQuery, NodeKind,
    PropsPatch, Result, merged_into,
};
use serde_json::{Value, json};
use time::Date;
use tracing::debug;
use uuid::Uuid;

use super::Outcome;
use super::company::{Findings, Job, Other, Planned};
use super::gate::Gated;
use super::subject::{Subject, ch_alias, qid_alias};

/// Most entries each props list keeps.
const MAX_LIST: usize = 20;
/// Most `merged_into` hops followed.
const MAX_HOPS: usize = 64;

/// The live company `id` stands for.
pub(crate) async fn live_id(b: &dyn Backend, mut id: Uuid) -> Result<Uuid> {
    for _ in 0..MAX_HOPS {
        match b.node(id).await?.as_ref().and_then(merged_into) {
            Some(next) => id = next,
            None => break,
        }
    }
    Ok(id)
}

/// Writes `edge` onto live companies ([`doewood_model::GraphStore::upsert_live_edge`], issue
/// #60): a merge committed since its ends were resolved (by a person, or on another instance)
/// moves it to the survivor, never leaves it on a husk. Counts a redirect in `redirected`.
async fn put(b: &dyn Backend, edge: &Edge, redirected: &mut u64) -> Result<()> {
    let live = b.upsert_live_edge(edge).await?;
    if live.redirects > 0 {
        debug!(
            kind = %edge.kind,
            src = %live.src,
            dst = %live.dst,
            hops = live.redirects,
            "registry edge redirected to the company it was merged into"
        );
        *redirected += 1;
    }
    Ok(())
}

/// Writes the findings for `s`, counting into `out` (`edges`, `redirected`, `proposed`) as it
/// goes. The edges come first: they follow a merge onto the survivor (issue #60). The
/// company's own aliases and props come after, onto the live company only (issue #62): once
/// it was merged away they fail with [`doewood_model::Error::MergedAway`] and the rest is not
/// written.
pub(crate) async fn write(
    job: &Job<'_>,
    s: &Subject,
    f: &Findings,
    g: &Gated,
    out: &mut Outcome,
) -> Result<()> {
    let b = job.backend.as_ref();
    let me = s.id();
    // 1. Edges, with the dated renames from the former names.
    let mut edges: Vec<Planned> = f.edges.clone();
    let current = f.names.first().cloned();
    for former in f.former.iter().filter(|n| n.until.is_some()) {
        edges.push(Planned {
            kind: EdgeKind::RenamedTo,
            outgoing: true,
            other: Other::Itself,
            valid_from: former.until,
            valid_to: None,
            state: EdgeState::Active,
            confidence: super::company::OFFICIAL,
            evidence: former.evidence.clone(),
            props: json!({"from": former.name, "to": current}),
        });
    }
    for p in &edges {
        let Some(other) = resolve(b, s, &p.other).await? else {
            debug!(company = %me, other = ?p.other, kind = %p.kind, "the other company is unknown; edge dropped");
            continue;
        };
        if other == me && !matches!(p.other, Other::Itself) {
            continue;
        }
        let (src, dst) = if p.outgoing { (me, other) } else { (other, me) };
        let to = match (p.valid_from, p.valid_to) {
            (Some(a), Some(z)) if z < a => None,
            (_, z) => z,
        };
        let edge = Edge::new(src, dst, p.kind, EdgeSource::Registry)
            .with_props(p.props.clone())
            .with_evidence(json!([p.evidence]))
            .with_confidence(p.confidence)
            .with_state(p.state)
            .valid(p.valid_from, to);
        put(b, &edge, &mut out.redirected).await?;
        out.edges += 1;
    }
    // 2. Two current parents: the later dated one is current.
    out.edges += former_parents(b, me, &mut out.redirected).await?;
    // 3. Identifiers.
    let mut owned = BTreeSet::new();
    for found in &f.found {
        let props = json!({
            "step": "registry",
            "confidence": found.by_name.unwrap_or(0.95),
            "registry": found.evidence,
            "by_name": found.by_name.is_some(),
        });
        let got = b
            .claim_live_alias(NodeKind::Company, &found.alias, me, &props)
            .await?;
        if got.node == Some(me) {
            owned.insert(found.alias.clone());
        } else {
            debug!(company = %me, alias = %found.alias, "identifier owned by another company; not claimed");
        }
    }
    // 4. Former names: learned aliases.
    for former in &f.former {
        let Some(old) = CompanyName::parse(&former.name) else {
            continue;
        };
        let form = old
            .legal_form
            .or_else(|| s.name.as_ref().and_then(|n| n.legal_form))
            .unwrap_or("");
        let step = LadderStep::LearnedAlias;
        b.claim_live_alias(
            NodeKind::Company,
            &format!("name:{}|{form}", old.key),
            me,
            &json!({"step": step.as_str(), "confidence": step.confidence(), "registry": former.evidence, "surface": former.name}),
        )
        .await?;
    }
    // 5. Props.
    update_props(b, me, f, &owned).await?;
    // 6. Proposals.
    out.proposed += proposals(b, me, f, g).await?;
    Ok(())
}

/// The gate's proposals, into the proposals table: a new open one, or the open one updated.
/// A dismissed one is never opened again. Returns how many were opened or updated.
async fn proposals(b: &dyn Backend, me: Uuid, f: &Findings, g: &Gated) -> Result<u64> {
    let mut n = 0;
    let records = f.proposals.iter().map(|p| {
        // The company as it is now (a merge may have made another the survivor).
        let mut p = p.clone();
        p.company = me;
        p
    });
    for p in records.chain(g.proposals.iter().cloned()) {
        match b.propose(&p).await? {
            Some(stored) => {
                debug!(company = %me, proposal = %stored.id, kind = %stored.kind, candidate = %stored.candidate, "proposal recorded");
                n += 1;
            }
            None => {
                debug!(company = %me, kind = %p.kind, candidate = %p.candidate, "proposal dismissed before; not opened");
            }
        }
    }
    Ok(n)
}

/// The company `other` names, created for an LEI or a Companies House number.
async fn resolve(b: &dyn Backend, s: &Subject, other: &Other) -> Result<Option<Uuid>> {
    let by_alias = |alias: String| async move {
        match b
            .alias(NodeKind::Company, &alias)
            .await?
            .and_then(|a| a.node)
        {
            Some(id) => Ok::<_, doewood_model::Error>(Some(live_id(b, id).await?)),
            None => Ok(None),
        }
    };
    match other {
        Other::Itself => Ok(Some(s.id())),
        Other::Lei { lei, name } => {
            create_or_find(b, lei.alias(), name.as_deref(), json!({"lei": [lei.value]})).await
        }
        Other::Ch { number, name } => {
            create_or_find(b, ch_alias(number), name.as_deref(), json!({})).await
        }
        Other::Qid { qid } => by_alias(qid_alias(*qid)).await,
        Other::Name { name } => {
            let Some(parsed) = CompanyName::parse(name) else {
                return Ok(None);
            };
            for (step, key) in parsed.ladder_keys() {
                if step == LadderStep::FormBlind {
                    continue;
                }
                if let Some(id) = by_alias(key).await? {
                    return Ok(Some(id));
                }
            }
            Ok(None)
        }
    }
}

/// The company that owns `alias` (or is keyed by it), else a new one keyed by it, with its
/// name keys claimed (first claim wins).
async fn create_or_find(
    b: &dyn Backend,
    alias: String,
    name: Option<&str>,
    ids: Value,
) -> Result<Option<Uuid>> {
    if let Some(id) = b
        .alias(NodeKind::Company, &alias)
        .await?
        .and_then(|a| a.node)
    {
        return Ok(Some(live_id(b, id).await?));
    }
    if let Some(n) = b.node_by_key(NodeKind::Company, &alias).await? {
        return Ok(Some(live_id(b, n.id).await?));
    }
    let parsed = name.and_then(CompanyName::parse);
    // Created empty, then only missing keys filled: a company ingest created with this key
    // at the same moment keeps what it wrote.
    let id = b.ensure_node(NodeKind::Company, &alias).await?;
    let mut patch = PropsPatch::new()
        .set_if_absent(&["origin"], json!("registry"))
        .set_if_absent(&["ids"], json!({}));
    for (kind, values) in ids.as_object().into_iter().flatten() {
        for v in values.as_array().into_iter().flatten() {
            patch = patch.add_to_set(&["ids", kind], v.clone(), MAX_LIST);
        }
    }
    if let Some(n) = name {
        patch = patch
            .set_if_absent(&["name"], json!(n))
            .set_if_absent(&["legal_name"], json!(n));
    }
    if let Some(p) = &parsed {
        patch = patch
            .set_if_absent(&["key"], json!(p.key))
            .set_if_absent(&["legal_form"], json!(p.legal_form));
    }
    b.update_props(id, &patch).await?;
    let evidence = json!({"step": "registry", "confidence": 0.95});
    b.claim_alias(NodeKind::Company, &alias, id, &evidence)
        .await?;
    if let Some(p) = &parsed {
        for (step, key) in p.ladder_keys() {
            if step != LadderStep::FormBlind {
                b.claim_alias(NodeKind::Company, &key, id, &json!({"step": step.as_str(), "confidence": step.confidence(), "origin": "registry"}))
                    .await?;
            }
        }
    }
    Ok(Some(id))
}

/// ADR 0002 section 3: when two parents of `child` are current (active, undated end, not
/// an ultimate parent), the later dated one stays current; each earlier one ends on that day
/// (superseded, with its `subsidiary_of` twin) and becomes a `former_parent_of` edge. Returns
/// the edges written; redirects onto a survivor are counted in `redirected`.
async fn former_parents(b: &dyn Backend, child: Uuid, redirected: &mut u64) -> Result<u64> {
    let query = NeighbourQuery {
        direction: Direction::In,
        edge_kinds: vec![EdgeKind::ParentOf],
        node_kind: Some(NodeKind::Company),
        ..NeighbourQuery::default()
    };
    let current: Vec<_> = b
        .neighbours(child, &query)
        .await?
        .into_iter()
        .filter(|n| {
            let e = &n.edge.edge;
            e.state == EdgeState::Active
                && e.valid_to.is_none()
                && e.props.get("level").and_then(Value::as_str) != Some("ultimate")
                && n.node.id != child
        })
        .collect();
    let Some(latest) = current
        .iter()
        .filter(|n| n.edge.edge.valid_from.is_some())
        .max_by_key(|n| n.edge.edge.valid_from)
    else {
        return Ok(0);
    };
    let Some(until) = latest.edge.edge.valid_from else {
        return Ok(0);
    };
    let twins = NeighbourQuery {
        direction: Direction::Out,
        edge_kinds: vec![EdgeKind::SubsidiaryOf],
        node_kind: Some(NodeKind::Company),
        ..NeighbourQuery::default()
    };
    let mut written = 0;
    for old in &current {
        let e = &old.edge.edge;
        if old.node.id == latest.node.id || e.valid_from.is_some_and(|d| d >= until) {
            continue;
        }
        put(b, &supersede(e, until), redirected).await?;
        for twin in b.neighbours(child, &twins).await? {
            let t = &twin.edge.edge;
            if twin.node.id == old.node.id && t.source == e.source && t.valid_from == e.valid_from {
                put(b, &supersede(t, until), redirected).await?;
            }
        }
        let mut evidence = e.evidence.as_array().cloned().unwrap_or_default();
        evidence.extend(
            latest
                .edge
                .edge
                .evidence
                .as_array()
                .cloned()
                .unwrap_or_default(),
        );
        let former = Edge::new(old.node.id, child, EdgeKind::FormerParentOf, e.source)
            .with_props(
                json!({"replaced_by": latest.node.id.to_string(), "level": e.props.get("level")}),
            )
            .with_evidence(Value::Array(evidence))
            .with_confidence(e.confidence)
            .with_state(EdgeState::Active)
            .valid(e.valid_from, Some(until));
        put(b, &former, redirected).await?;
        written += 1;
    }
    Ok(written)
}

fn supersede(e: &Edge, until: Date) -> Edge {
    let mut e = e.clone();
    e.state = EdgeState::Superseded;
    e.valid_to = Some(until);
    e
}

/// The company's props from the findings, as one atomic patch of the keys the job keeps
/// (issue #51): ingest's keys, and what it writes at the same moment, stay.
async fn update_props(
    b: &dyn Backend,
    me: Uuid,
    f: &Findings,
    owned: &BTreeSet<String>,
) -> Result<()> {
    // Every list and map the job keeps is there, empty if need be.
    let mut patch = PropsPatch::new();
    for key in ["registry", "ids", "registry_ids"] {
        patch = patch.set_if_absent(&[key], json!({}));
    }
    patch = patch.set_if_absent(&["former_names"], json!([]));
    // Proposals live in their own table since schema 5; an undo of an older merge can bring
    // the old lists back, so they are removed here too.
    for key in ["merge_proposals", "registry_proposals"] {
        patch = patch.remove(&[key]);
    }
    for (name, facts) in &f.facts {
        patch = patch.set(&["registry", name], facts.clone());
    }
    for alias in owned {
        if let Some(rest) = alias.strip_prefix("id:")
            && let Some((kind, value)) = rest.split_once(':')
        {
            patch = patch.add_to_set(&["ids", kind], json!(value), MAX_LIST);
        } else if let Some(rest) = alias.strip_prefix("reg:")
            && let Some((kind, value)) = rest.split_once(':')
        {
            patch = patch.set(&["registry_ids", kind], json!(value));
        }
    }
    if let Some(name) = f.names.first() {
        patch = patch
            .set(&["legal_name"], json!(name))
            .set_if_absent(&["name"], json!(name));
    }
    for n in &f.former {
        let v = json!({"name": n.name, "until": n.until.map(|d| d.to_string()), "evidence": n.evidence});
        patch = patch.push_capped(&["former_names"], v, &["name"], MAX_LIST);
    }
    let at = super::unix_now();
    patch = patch.set(&["enriched_at"], json!(at));
    b.update_live_props(me, &patch).await?;
    Ok(())
}
