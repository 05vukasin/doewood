//! Conversions between the store and model types and the protobuf messages. Nothing here ever
//! reads a secret: sources and providers say only whether one is set.

use doewood_core::InstanceId;
use doewood_model::{
    Direction, EdgeKind, EdgeSource, EdgeState, Node, NodeKind, Record, SearchHit, StoredEdge,
};
use doewood_s3::inlet::InletStatsSnapshot;
use doewood_store::{AiProvider, LlmMode, LlmPolicy, Source, SourceState, SqsInlet, UrlStyle};

use crate::pb;
use crate::validate::format_date;

pub(crate) fn source_state(s: SourceState) -> pb::SourceState {
    match s {
        SourceState::Active => pb::SourceState::Active,
        SourceState::Paused => pb::SourceState::Paused,
        SourceState::Error => pb::SourceState::Error,
        SourceState::Removed => pb::SourceState::Removed,
    }
}

pub(crate) fn url_style(s: UrlStyle) -> pb::UrlStyle {
    match s {
        UrlStyle::Path => pb::UrlStyle::Path,
        UrlStyle::VirtualHost => pb::UrlStyle::VirtualHost,
    }
}

pub(crate) fn llm_policy(p: &LlmPolicy) -> pb::LlmPolicy {
    let (mode, kinds) = match &p.mode {
        LlmMode::Off => (pb::LlmMode::Off, Vec::new()),
        LlmMode::All => (pb::LlmMode::All, Vec::new()),
        LlmMode::Kinds(k) => (pb::LlmMode::Kinds, k.clone()),
        LlmMode::OnDemand => (pb::LlmMode::OnDemand, Vec::new()),
    };
    pb::LlmPolicy {
        mode: mode as i32,
        kinds,
        concurrency: p.concurrency,
        daily_token_budget: p.daily_token_budget.unwrap_or(0),
    }
}

/// What discovery says about a source's tasks.
#[derive(Debug, Default)]
pub(crate) struct Running {
    pub scanner: bool,
    pub inlet: bool,
    pub inlet_stats: Option<InletStatsSnapshot>,
}

/// A source's SQS inlet: its settings, whether credentials of its own are set (never the
/// credentials), and what it did since it started.
pub(crate) fn sqs_inlet(
    i: &SqsInlet,
    running: bool,
    stats: Option<InletStatsSnapshot>,
) -> pb::SqsInlet {
    let stats = stats.unwrap_or_default();
    let (last_error_at, last_error) = stats.last_error.unwrap_or_default();
    pb::SqsInlet {
        queue_url: i.queue_url.clone(),
        region: i.region().to_owned(),
        enabled: i.enabled,
        credentials_set: i.credentials_set,
        running,
        messages_received: stats.received,
        messages_deleted: stats.deleted,
        events_emitted: stats.emitted,
        records_ignored: stats.ignored,
        test_events: stats.test_events,
        malformed_messages: stats.malformed,
        errors: stats.errors,
        last_error,
        last_error_at,
    }
}

pub(crate) fn source(s: &Source, owner: &InstanceId, running: Running) -> pb::Source {
    pb::Source {
        id: s.id.to_string(),
        endpoint: s.endpoint.clone(),
        bucket: s.bucket.clone(),
        prefix: s.prefix.clone(),
        region: s.region().to_owned(),
        url_style: url_style(s.url_style) as i32,
        credentials_set: s.credentials_secret.is_some(),
        scan_interval_seconds: s.scan_interval.as_secs(),
        max_object_bytes: s.max_object_bytes(),
        llm_policy: Some(llm_policy(&s.llm_policy)),
        state: source_state(s.state) as i32,
        state_reason: s.state_reason.clone().unwrap_or_default(),
        owner_instance: owner.to_string(),
        scanner_running: running.scanner,
        created_at: s.created_at,
        updated_at: s.updated_at,
        sqs_inlet: s
            .sqs
            .as_ref()
            .map(|i| sqs_inlet(i, running.inlet, running.inlet_stats)),
    }
}

pub(crate) fn ai_provider(p: &AiProvider) -> pb::AiProvider {
    pb::AiProvider {
        name: p.name.clone(),
        preset: p.preset.clone(),
        model: p.model.clone(),
        base_url: p.base_url.clone().unwrap_or_default(),
        api_key_set: p.api_key_set,
        is_default: p.is_default,
        updated_at: p.updated_at,
    }
}

pub(crate) fn ingest_settings(s: doewood_store::IngestSettings) -> pb::IngestSettings {
    pb::IngestSettings {
        similar_min_jaccard: s.similar_min_jaccard,
    }
}

pub(crate) fn api_settings(s: doewood_store::ApiSettings) -> pb::ApiSettings {
    pb::ApiSettings {
        max_calls_in_flight: s.max_calls_in_flight,
        deadline_seconds: s.deadline_seconds,
    }
}

pub(crate) fn record(r: &Record) -> pb::Record {
    pb::Record {
        blake3: r.blake3.to_hex(),
        body_json: r.body.to_string(),
        versions: r
            .versions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        updated_at: r.updated_at,
    }
}

pub(crate) fn node_kind(k: NodeKind) -> pb::NodeKind {
    match k {
        NodeKind::Document => pb::NodeKind::Document,
        NodeKind::Location => pb::NodeKind::Location,
        NodeKind::Company => pb::NodeKind::Company,
    }
}

/// `None` for UNSPECIFIED (and unknown values, which the caller reports).
pub(crate) fn node_kind_from(k: pb::NodeKind) -> Option<NodeKind> {
    match k {
        pb::NodeKind::Unspecified => None,
        pb::NodeKind::Document => Some(NodeKind::Document),
        pb::NodeKind::Location => Some(NodeKind::Location),
        pb::NodeKind::Company => Some(NodeKind::Company),
    }
}

pub(crate) fn edge_kind(k: EdgeKind) -> pb::EdgeKind {
    match k {
        EdgeKind::LocatedAt => pb::EdgeKind::LocatedAt,
        EdgeKind::Party => pb::EdgeKind::Party,
        EdgeKind::Amends => pb::EdgeKind::Amends,
        EdgeKind::Cites => pb::EdgeKind::Cites,
        EdgeKind::SimilarTo => pb::EdgeKind::SimilarTo,
        EdgeKind::ParentOf => pb::EdgeKind::ParentOf,
        EdgeKind::SubsidiaryOf => pb::EdgeKind::SubsidiaryOf,
        EdgeKind::Acquired => pb::EdgeKind::Acquired,
        EdgeKind::MergedInto => pb::EdgeKind::MergedInto,
        EdgeKind::RenamedTo => pb::EdgeKind::RenamedTo,
        EdgeKind::BranchOf => pb::EdgeKind::BranchOf,
        EdgeKind::TradingNameOf => pb::EdgeKind::TradingNameOf,
        EdgeKind::FormerParentOf => pb::EdgeKind::FormerParentOf,
        EdgeKind::Liquidated => pb::EdgeKind::Liquidated,
    }
}

pub(crate) fn edge_kind_from(k: pb::EdgeKind) -> Option<EdgeKind> {
    Some(match k {
        pb::EdgeKind::Unspecified => return None,
        pb::EdgeKind::LocatedAt => EdgeKind::LocatedAt,
        pb::EdgeKind::Party => EdgeKind::Party,
        pb::EdgeKind::Amends => EdgeKind::Amends,
        pb::EdgeKind::Cites => EdgeKind::Cites,
        pb::EdgeKind::SimilarTo => EdgeKind::SimilarTo,
        pb::EdgeKind::ParentOf => EdgeKind::ParentOf,
        pb::EdgeKind::SubsidiaryOf => EdgeKind::SubsidiaryOf,
        pb::EdgeKind::Acquired => EdgeKind::Acquired,
        pb::EdgeKind::MergedInto => EdgeKind::MergedInto,
        pb::EdgeKind::RenamedTo => EdgeKind::RenamedTo,
        pb::EdgeKind::BranchOf => EdgeKind::BranchOf,
        pb::EdgeKind::TradingNameOf => EdgeKind::TradingNameOf,
        pb::EdgeKind::FormerParentOf => EdgeKind::FormerParentOf,
        pb::EdgeKind::Liquidated => EdgeKind::Liquidated,
    })
}

fn edge_state(s: EdgeState) -> pb::EdgeState {
    match s {
        EdgeState::Proposed => pb::EdgeState::Proposed,
        EdgeState::Active => pb::EdgeState::Active,
        EdgeState::Contested => pb::EdgeState::Contested,
        EdgeState::Retracted => pb::EdgeState::Retracted,
        EdgeState::Superseded => pb::EdgeState::Superseded,
    }
}

fn edge_source(s: EdgeSource) -> pb::EdgeSource {
    match s {
        EdgeSource::Registry => pb::EdgeSource::Registry,
        EdgeSource::Document => pb::EdgeSource::Document,
        EdgeSource::Web => pb::EdgeSource::Web,
        EdgeSource::Manual => pb::EdgeSource::Manual,
        EdgeSource::Storage => pb::EdgeSource::Storage,
    }
}

pub(crate) fn direction_from(d: pb::Direction) -> Direction {
    match d {
        pb::Direction::Out => Direction::Out,
        pb::Direction::In => Direction::In,
        pb::Direction::Unspecified | pb::Direction::Both => Direction::Both,
    }
}

pub(crate) fn node(n: &Node) -> pb::Node {
    pb::Node {
        id: n.id.to_string(),
        kind: node_kind(n.kind) as i32,
        key: n.key.clone(),
        props_json: n.props.to_string(),
    }
}

pub(crate) fn edge(e: &StoredEdge) -> pb::Edge {
    let edge = &e.edge;
    pb::Edge {
        id: e.id.to_string(),
        src: edge.src.to_string(),
        dst: edge.dst.to_string(),
        kind: edge_kind(edge.kind) as i32,
        props_json: edge.props.to_string(),
        evidence_json: edge.evidence.to_string(),
        confidence: edge.confidence,
        state: edge_state(edge.state) as i32,
        valid_from: edge.valid_from.map(format_date).unwrap_or_default(),
        valid_to: edge.valid_to.map(format_date).unwrap_or_default(),
        source: edge_source(edge.source) as i32,
    }
}

pub(crate) fn search_hit(h: &SearchHit) -> pb::SearchHit {
    pb::SearchHit {
        blake3: h.blake3.to_hex(),
        title: h.title.clone(),
        kind: h.kind.clone().unwrap_or_default(),
        date: h.date.map(format_date).unwrap_or_default(),
        amount: h.amount,
        score: h.score,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_edge_kind_round_trips() {
        for k in EdgeKind::ALL {
            assert_eq!(edge_kind_from(edge_kind(*k)), Some(*k));
        }
        for k in NodeKind::ALL {
            assert_eq!(node_kind_from(node_kind(*k)), Some(*k));
        }
        assert_eq!(edge_kind_from(pb::EdgeKind::Unspecified), None);
    }
}
