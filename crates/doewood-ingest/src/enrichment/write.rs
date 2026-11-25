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
//!   `registry_ids`, `legal_name`, `former_names`, `merge_proposals`, `registry_proposals`
//!   and `enriched_at`.

// `b` (the backend), `s` (the company) and `f` (the findings) are used throughout.
#![allow(clippy::many_single_char_names)]

use std::collections::BTreeSet;

use doewood_enrich::{CompanyName, LadderStep};
use doewood_model::{
    Backend, Direction, Edge, EdgeKind, EdgeSource, EdgeState, NeighbourQuery, NodeKind, Result,
    merged_into,
};
use serde_json::{Map, Value, json};
use time::Date;
use tracing::debug;
use uuid::Uuid;

use super::company::{Findings, Job, Other, Planned, props_map};
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

/// Writes the findings for `s`. Returns how many edges were written.
pub(crate) async fn write(job: &Job<'_>, s: &Subject, f: &Findings, g: &Gated) -> Result<u64> {
    let b = job.backend.as_ref();
    let me = s.id();
    // 1. Identifiers.
    let mut owned = BTreeSet::new();
    for found in &f.found {
        let props = json!({
            "step": "registry",
            "confidence": found.by_name.unwrap_or(0.95),
            "registry": found.evidence,
            "by_name": found.by_name.is_some(),
        });
        let got = b
            .claim_alias(NodeKind::Company, &found.alias, me, &props)
            .await?;
        if got.node == Some(me) {
            owned.insert(found.alias.clone());
        } else {
            debug!(company = %me, alias = %found.alias, "identifier owned by another company; not claimed");
        }
    }
    // 2. Former names: learned aliases, and dated renames.
    let mut edges: Vec<Planned> = f.edges.clone();
    let current = f.names.first().cloned();
    for former in &f.former {
        let Some(old) = CompanyName::parse(&former.name) else {
            continue;
        };
        let form = old
            .legal_form
            .or_else(|| s.name.as_ref().and_then(|n| n.legal_form))
            .unwrap_or("");
        let step = LadderStep::LearnedAlias;
        b.claim_alias(
            NodeKind::Company,
            &format!("name:{}|{form}", old.key),
            me,
            &json!({"step": step.as_str(), "confidence": step.confidence(), "registry": former.evidence, "surface": former.name}),
        )
        .await?;
        if former.until.is_some() {
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
    }
    // 3. Edges.
    let mut written = 0;
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
        b.upsert_edge(&edge).await?;
        written += 1;
    }
    // 4. Two current parents: the later dated one is current.
    written += former_parents(b, me).await?;
    // 5. Props.
    update_props(b, me, f, g, &owned).await?;
    Ok(written)
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
    let mut props = json!({"origin": "registry", "ids": ids});
    if let Some(n) = name {
        props["name"] = json!(n);
        props["legal_name"] = json!(n);
    }
    if let Some(p) = &parsed {
        props["key"] = json!(p.key);
        props["legal_form"] = json!(p.legal_form);
    }
    let id = b.upsert_node(NodeKind::Company, &alias, &props).await?;
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
/// the edges written.
async fn former_parents(b: &dyn Backend, child: Uuid) -> Result<u64> {
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
        b.upsert_edge(&supersede(e, until)).await?;
        for twin in b.neighbours(child, &twins).await? {
            let t = &twin.edge.edge;
            if twin.node.id == old.node.id && t.source == e.source && t.valid_from == e.valid_from {
                b.upsert_edge(&supersede(t, until)).await?;
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
        b.upsert_edge(&former).await?;
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

fn push_bounded(list: &mut Vec<Value>, v: Value, same: impl Fn(&Value) -> bool) {
    list.retain(|x| !same(x));
    list.push(v);
    while list.len() > MAX_LIST {
        list.remove(0);
    }
}

async fn update_props(
    b: &dyn Backend,
    me: Uuid,
    f: &Findings,
    g: &Gated,
    owned: &BTreeSet<String>,
) -> Result<()> {
    let Some(node) = b.node(me).await? else {
        return Ok(());
    };
    let mut props = props_map(&node.props);
    let mut registry = props.get("registry").map(props_map).unwrap_or_default();
    for (name, facts) in &f.facts {
        registry.insert((*name).to_owned(), facts.clone());
    }
    props.insert("registry".into(), Value::Object(registry));
    let mut ids = props.get("ids").map(props_map).unwrap_or_default();
    let mut registry_ids = props.get("registry_ids").map(props_map).unwrap_or_default();
    for alias in owned {
        if let Some(rest) = alias.strip_prefix("id:")
            && let Some((kind, value)) = rest.split_once(':')
        {
            let list = ids
                .entry(kind.to_owned())
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Value::Array(l) = list
                && l.len() < MAX_LIST
                && !l.iter().any(|v| v.as_str() == Some(value))
            {
                l.push(Value::String(value.to_owned()));
            }
        } else if let Some(rest) = alias.strip_prefix("reg:")
            && let Some((kind, value)) = rest.split_once(':')
        {
            registry_ids.insert(kind.to_owned(), Value::String(value.to_owned()));
        }
    }
    props.insert("ids".into(), Value::Object(ids));
    props.insert("registry_ids".into(), Value::Object(registry_ids));
    if let Some(name) = f.names.first() {
        props.insert("legal_name".into(), Value::String(name.clone()));
        if props.get("name").is_none_or(Value::is_null) {
            props.insert("name".into(), Value::String(name.clone()));
        }
    }
    let mut former = list_of(&props, "former_names");
    for n in &f.former {
        let v = json!({"name": n.name, "until": n.until.map(|d| d.to_string()), "evidence": n.evidence});
        push_bounded(&mut former, v, |x| x["name"] == json!(n.name));
    }
    props.insert("former_names".into(), Value::Array(former));
    let at = super::unix_now();
    let mut merges = list_of(&props, "merge_proposals");
    for p in &g.proposals {
        let mut p = p.clone();
        p["at"] = json!(at);
        let company = p["company"].clone();
        push_bounded(&mut merges, p, |x| x["company"] == company);
    }
    props.insert("merge_proposals".into(), Value::Array(merges));
    let mut records = list_of(&props, "registry_proposals");
    for p in &f.proposals {
        let mut p = p.clone();
        p["at"] = json!(at);
        let (r, rec) = (p["registry"].clone(), p["record"].clone());
        push_bounded(&mut records, p, |x| {
            x["registry"] == r && x["record"] == rec
        });
    }
    props.insert("registry_proposals".into(), Value::Array(records));
    props.insert("enriched_at".into(), json!(at));
    b.upsert_node(NodeKind::Company, &node.key, &Value::Object(props))
        .await?;
    Ok(())
}

fn list_of(props: &Map<String, Value>, key: &str) -> Vec<Value> {
    props
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}
