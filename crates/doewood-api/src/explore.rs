//! The explorer half of `GraphService` (issue #172): `ListCompanies`, `CompanyGraph`,
//! `Subgraph` and `Path`. The walks and the caps are `doewood_model::explore`'s; this checks
//! requests and shapes answers.

use doewood_model::explore::{
    self, COMPANY_GRAPH_LINKS_DEFAULT, COMPANY_GRAPH_NODES_DEFAULT, PATH_DEPTH_DEFAULT,
    PATH_DEPTH_MAX, PATH_PATHS_MAX, PathQuery, SUBGRAPH_DEPTH_MAX, SUBGRAPH_EDGES_DEFAULT,
    SUBGRAPH_EDGES_MAX, SUBGRAPH_NODES_DEFAULT, SUBGRAPH_NODES_MAX, SUBGRAPH_SEEDS_MAX,
    SubgraphQuery, cap,
};
use doewood_model::{
    COMPANY_GRAPH_LINKS_MAX, COMPANY_GRAPH_NODES_MAX, CompanyCursor, CompanyFilter, CompanyOrder,
    CompanyRow, EdgeKind, NodeKind, RECORDS_MAX_LIMIT, RECORDS_PAGE_BYTES, company_row_bytes,
    page_full, records_limit,
};
use serde_json::Value;
use tonic::Status;

use crate::pb;
use crate::validate::{self, Violations};
use crate::{Api, convert, error};

/// Most edge kinds one request lists.
const MAX_EDGE_KINDS: usize = 32;
/// Most aliases a summary lists.
const SUMMARY_ALIASES_MAX: usize = 32;
/// Longest page token taken.
const PAGE_TOKEN_MAX_LEN: usize = 8192;

fn filter(v: &mut Violations, f: Option<&pb::CompanyFilter>) -> CompanyFilter {
    let Some(f) = f else {
        return CompanyFilter::default();
    };
    let out = CompanyFilter {
        name_prefix: Some(f.name_prefix.clone()).filter(|p| !p.is_empty()),
        jurisdiction: Some(f.jurisdiction.clone()).filter(|j| !j.is_empty()),
        has_identifier: f.has_identifier,
        enriched: f.enriched,
    };
    if f.name_prefix.len() > explore::NAME_PREFIX_MAX_LEN || f.name_prefix.contains('\0') {
        v.add(
            "filter.name_prefix",
            format!("at most {} bytes, no NUL", explore::NAME_PREFIX_MAX_LEN),
        );
    }
    if f.jurisdiction.len() > explore::JURISDICTION_MAX_LEN || f.jurisdiction.contains('\0') {
        v.add(
            "filter.jurisdiction",
            format!("at most {} bytes, no NUL", explore::JURISDICTION_MAX_LEN),
        );
    }
    out
}

fn edge_kinds(v: &mut Violations, field: &str, kinds: &[i32]) -> Vec<EdgeKind> {
    if kinds.len() > MAX_EDGE_KINDS {
        v.add(field, format!("at most {MAX_EDGE_KINDS}"));
    }
    let mut out = Vec::new();
    for (i, k) in kinds.iter().enumerate() {
        match pb::EdgeKind::try_from(*k)
            .ok()
            .and_then(convert::edge_kind_from)
        {
            Some(k) if !out.contains(&k) => out.push(k),
            Some(_) => {}
            None => v.add(format!("{field}[{i}]"), format!("unknown value {k}")),
        }
    }
    out
}

fn max_field(v: &mut Violations, field: &str, value: u32, max: u32) {
    if value > max {
        v.add(field, format!("at most {max}"));
    }
}

fn text(props: &Value, path: &[&str]) -> String {
    let mut at = props;
    for k in path {
        at = &at[*k];
    }
    at.as_str().unwrap_or_default().to_owned()
}

/// A company as the explorer lists it.
pub(crate) fn summary(row: &CompanyRow) -> pb::CompanySummary {
    let p = &row.node.props;
    let name = text(p, &["name"]);
    let aliases = p["names"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .filter(|n| *n != name)
                .take(SUMMARY_ALIASES_MAX)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let identifiers = row
        .identifiers
        .iter()
        .filter_map(|a| {
            let (kind, value) = a.strip_prefix("id:")?.split_once(':')?;
            Some(pb::CompanyIdentifier {
                kind: kind.to_owned(),
                value: value.to_owned(),
            })
        })
        .collect();
    pb::CompanySummary {
        id: row.node.id.to_string(),
        aliases,
        identifiers,
        jurisdiction: text(p, &["jurisdiction"]),
        website: text(p, &["website", "url"]),
        favicon_domain: text(p, &["icon", "domain"]),
        document_count: row.documents,
        degree: row.degree,
        enriched: row.enriched,
        name,
    }
}

impl Api {
    pub(crate) async fn list_companies_rpc(
        &self,
        req: &pb::ListCompaniesRequest,
    ) -> Result<pb::ListCompaniesResponse, Status> {
        let mut v = Violations::default();
        let filter = filter(&mut v, req.filter.as_ref());
        let order = match pb::CompanyOrder::try_from(req.order) {
            Ok(pb::CompanyOrder::Unspecified | pb::CompanyOrder::Documents) => {
                CompanyOrder::Documents
            }
            Ok(pb::CompanyOrder::Degree) => CompanyOrder::Degree,
            Ok(pb::CompanyOrder::Name) => CompanyOrder::Name,
            Err(_) => {
                v.add("order", format!("unknown value {}", req.order));
                CompanyOrder::Documents
            }
        };
        max_field(&mut v, "page_size", req.page_size, RECORDS_MAX_LIMIT);
        let after = if req.page_token.is_empty() {
            None
        } else {
            let parsed = (req.page_token.len() <= PAGE_TOKEN_MAX_LEN)
                .then(|| req.page_token.parse::<CompanyCursor>().ok())
                .flatten()
                .filter(|c| c.order == order);
            if parsed.is_none() {
                v.add(
                    "page_token",
                    "not a token from a previous page of this order",
                );
            }
            parsed
        };
        v.check()?;
        let backend = self.state.backend()?;
        let stale = explore::fold_for_read(backend.as_ref())
            .await
            .map_err(error::model)?;
        let limit = records_limit(req.page_size);
        let rows = backend
            .list_companies(&filter, order, after.as_ref(), limit, RECORDS_PAGE_BYTES)
            .await
            .map_err(error::model)?;
        let next_page_token = match rows.last() {
            Some(last) if page_full(&rows, limit, RECORDS_PAGE_BYTES, company_row_bytes) => {
                CompanyCursor::of(order, last).to_string()
            }
            _ => String::new(),
        };
        Ok(pb::ListCompaniesResponse {
            companies: rows.iter().map(summary).collect(),
            next_page_token,
            stale,
        })
    }

    pub(crate) async fn company_graph_rpc(
        &self,
        req: &pb::CompanyGraphRequest,
    ) -> Result<pb::CompanyGraphResponse, Status> {
        let mut v = Violations::default();
        let filter = filter(&mut v, req.filter.as_ref());
        max_field(&mut v, "limit", req.limit, COMPANY_GRAPH_NODES_MAX);
        max_field(&mut v, "max_links", req.max_links, COMPANY_GRAPH_LINKS_MAX);
        v.check()?;
        let backend = self.state.backend()?;
        let g = explore::company_graph(
            backend.as_ref(),
            &filter,
            req.min_weight.max(1),
            cap(
                req.limit,
                COMPANY_GRAPH_NODES_DEFAULT,
                COMPANY_GRAPH_NODES_MAX,
            ),
            cap(
                req.max_links,
                COMPANY_GRAPH_LINKS_DEFAULT,
                COMPANY_GRAPH_LINKS_MAX,
            ),
        )
        .await
        .map_err(error::model)?;
        let links = g
            .links
            .iter()
            .map(|l| pb::CompanyLink {
                a_id: l.a.to_string(),
                b_id: l.b.to_string(),
                shared_documents: l.documents,
                roles: l
                    .roles
                    .iter()
                    .map(|r| pb::CompanyRole {
                        company_id: r.company.to_string(),
                        role: r.role.clone(),
                        documents: r.documents,
                    })
                    .collect(),
                events: g
                    .events
                    .get(&(l.a, l.b))
                    .map(|es| es.iter().map(convert::edge).collect())
                    .unwrap_or_default(),
            })
            .collect();
        Ok(pb::CompanyGraphResponse {
            companies: g.companies.iter().map(summary).collect(),
            links,
            truncated: g.truncated,
            stale: g.stale,
        })
    }

    pub(crate) async fn subgraph_rpc(
        &self,
        req: &pb::SubgraphRequest,
    ) -> Result<pb::SubgraphResponse, Status> {
        let mut v = Violations::default();
        if req.seed_ids.is_empty() {
            v.add("seed_ids", "required");
        }
        if req.seed_ids.len() > SUBGRAPH_SEEDS_MAX {
            v.add("seed_ids", format!("at most {SUBGRAPH_SEEDS_MAX}"));
        }
        let seeds: Vec<_> = req
            .seed_ids
            .iter()
            .take(SUBGRAPH_SEEDS_MAX)
            .enumerate()
            .filter_map(|(i, s)| validate::uuid(&mut v, &format!("seed_ids[{i}]"), s))
            .collect();
        max_field(&mut v, "depth", req.depth, SUBGRAPH_DEPTH_MAX);
        max_field(&mut v, "limit", req.limit, SUBGRAPH_NODES_MAX);
        max_field(&mut v, "max_edges", req.max_edges, SUBGRAPH_EDGES_MAX);
        let mut node_kinds: Vec<NodeKind> = Vec::new();
        for (i, k) in req.node_kinds.iter().enumerate() {
            match pb::NodeKind::try_from(*k)
                .ok()
                .and_then(convert::node_kind_from)
            {
                Some(k) if !node_kinds.contains(&k) => node_kinds.push(k),
                Some(_) => {}
                None => v.add(format!("node_kinds[{i}]"), format!("unknown value {k}")),
            }
        }
        let edge_kinds = edge_kinds(&mut v, "edge_kinds", &req.edge_kinds);
        let as_of = validate::date(&mut v, "as_of", &req.as_of);
        v.check()?;
        let q = SubgraphQuery {
            seeds,
            depth: req.depth,
            node_kinds,
            edge_kinds,
            max_nodes: cap(req.limit, SUBGRAPH_NODES_DEFAULT, SUBGRAPH_NODES_MAX),
            max_edges: cap(req.max_edges, SUBGRAPH_EDGES_DEFAULT, SUBGRAPH_EDGES_MAX),
            as_of,
        };
        let g = explore::subgraph(self.state.backend()?.as_ref(), &q)
            .await
            .map_err(error::model)?;
        Ok(pb::SubgraphResponse {
            nodes: g
                .nodes
                .iter()
                .map(|r| pb::SubgraphNode {
                    node: Some(convert::node(&r.node)),
                    depth: r.depth,
                })
                .collect(),
            edges: g.edges.iter().map(convert::edge).collect(),
            truncated: g.truncated,
        })
    }

    pub(crate) async fn path_rpc(&self, req: &pb::PathRequest) -> Result<pb::PathResponse, Status> {
        let mut v = Violations::default();
        let from = validate::uuid(&mut v, "from_id", &req.from_id);
        let to = validate::uuid(&mut v, "to_id", &req.to_id);
        max_field(&mut v, "max_depth", req.max_depth, PATH_DEPTH_MAX);
        max_field(&mut v, "max_paths", req.max_paths, PATH_PATHS_MAX);
        let edge_kinds = edge_kinds(&mut v, "edge_kinds", &req.edge_kinds);
        let as_of = validate::date(&mut v, "as_of", &req.as_of);
        v.check()?;
        let (Some(from), Some(to)) = (from, to) else {
            return Err(Status::invalid_argument("from_id, to_id: required"));
        };
        let q = PathQuery {
            from,
            to,
            max_depth: cap(req.max_depth, PATH_DEPTH_DEFAULT, PATH_DEPTH_MAX),
            edge_kinds,
            as_of,
            max_paths: cap(req.max_paths, 1, PATH_PATHS_MAX),
        };
        let found = explore::shortest_paths(self.state.backend()?.as_ref(), &q)
            .await
            .map_err(error::model)?;
        Ok(pb::PathResponse {
            paths: found
                .paths
                .iter()
                .map(|p| pb::GraphPath {
                    nodes: p.nodes.iter().map(convert::node).collect(),
                    edges: p.edges.iter().map(convert::edge).collect(),
                })
                .collect(),
            truncated: found.truncated,
        })
    }
}
