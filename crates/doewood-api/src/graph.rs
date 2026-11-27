//! `GraphService`: nodes, neighbours and a company's documents, the merge log (in
//! `merges`), enrichment proposals (in `proposals`) and the explorer's reads (in `explore`).

use doewood_model::{
    EdgeCursor, EdgeKind, NODE_KEY_MAX_LEN, NeighbourQuery, NodeKind, RECORDS_MAX_LIMIT,
    RECORDS_PAGE_BYTES, neighbour_page_bytes, node_page_bytes, page_full, records_limit,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::pb::graph_service_server::GraphService;
use crate::pb::{
    self, CompanyDocumentsRequest, CompanyDocumentsResponse, GetNodeRequest, Neighbour,
    NeighboursRequest, NeighboursResponse, Node, get_node_request::Lookup,
};
use crate::validate::{self, Violations};
use crate::{Api, convert, error};

/// Most edge kinds one query lists.
const MAX_EDGE_KINDS: usize = 32;

impl Api {
    async fn node_or_404(&self, id: Uuid) -> Result<doewood_model::Node, Status> {
        self.state
            .backend()?
            .node(id)
            .await
            .map_err(error::model)?
            .ok_or_else(|| Status::not_found(format!("node {id} does not exist")))
    }
}

/// Checks `page_size` and parses `page_token` as `ListRecords` does (issue #112): at most
/// [`RECORDS_MAX_LIMIT`], and a token from a previous page.
fn page(v: &mut Violations, page_size: u32, page_token: &str) -> Option<EdgeCursor> {
    if page_size > RECORDS_MAX_LIMIT {
        v.add("page_size", format!("at most {RECORDS_MAX_LIMIT}"));
    }
    if page_token.is_empty() {
        return None;
    }
    let parsed = page_token.parse().ok();
    if parsed.is_none() {
        v.add("page_token", "not a token from a previous page");
    }
    parsed
}

fn node_kind(v: &mut Violations, field: &str, value: i32) -> Option<NodeKind> {
    if let Ok(k) = pb::NodeKind::try_from(value) {
        convert::node_kind_from(k)
    } else {
        v.add(field, format!("unknown value {value}"));
        None
    }
}

#[tonic::async_trait]
impl GraphService for Api {
    async fn get_node(&self, request: Request<GetNodeRequest>) -> Result<Response<Node>, Status> {
        let mut v = Violations::default();
        let node = match &request.get_ref().lookup {
            None => {
                v.add("lookup", "set id or key");
                v.check()?;
                None
            }
            Some(Lookup::Id(id)) => {
                let id = validate::uuid(&mut v, "id", id);
                v.check()?;
                match id {
                    Some(id) => Some(self.node_or_404(id).await?),
                    None => None,
                }
            }
            Some(Lookup::Key(key)) => {
                let kind = node_kind(&mut v, "key.kind", key.kind);
                if kind.is_none() {
                    v.add("key.kind", "required");
                }
                if key.key.is_empty() || key.key.len() > NODE_KEY_MAX_LEN {
                    v.add("key.key", format!("must be 1 to {NODE_KEY_MAX_LEN} bytes"));
                }
                v.check()?;
                let Some(kind) = kind else {
                    return Err(Status::invalid_argument("key.kind: required"));
                };
                Some(
                    self.state
                        .backend()?
                        .node_by_key(kind, &key.key)
                        .await
                        .map_err(error::model)?
                        .ok_or_else(|| {
                            Status::not_found(format!("no {kind} node with this key"))
                        })?,
                )
            }
        };
        let node = node.ok_or_else(|| Status::invalid_argument("lookup: set id or key"))?;
        Ok(Response::new(convert::node(&node)))
    }

    async fn neighbours(
        &self,
        request: Request<NeighboursRequest>,
    ) -> Result<Response<NeighboursResponse>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        let id = validate::uuid(&mut v, "node_id", &req.node_id);
        let direction = if let Ok(d) = pb::Direction::try_from(req.direction) {
            convert::direction_from(d)
        } else {
            v.add("direction", format!("unknown value {}", req.direction));
            doewood_model::Direction::Both
        };
        if req.edge_kinds.len() > MAX_EDGE_KINDS {
            v.add("edge_kinds", format!("at most {MAX_EDGE_KINDS}"));
        }
        let mut edge_kinds = Vec::new();
        for (i, k) in req.edge_kinds.iter().enumerate() {
            match pb::EdgeKind::try_from(*k)
                .ok()
                .and_then(convert::edge_kind_from)
            {
                Some(k) => edge_kinds.push(k),
                None => v.add(format!("edge_kinds[{i}]"), format!("unknown value {k}")),
            }
        }
        let node_kind = node_kind(&mut v, "node_kind", req.node_kind);
        let at = validate::date(&mut v, "at", &req.at);
        let after = page(&mut v, req.page_size, &req.page_token);
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("node_id: required"));
        };
        let query = NeighbourQuery {
            direction,
            edge_kinds,
            node_kind,
            at,
            include_inactive: req.include_inactive,
        };
        let limit = records_limit(req.page_size);
        let found = self
            .state
            .backend()?
            .neighbours_page(id, &query, after.as_ref(), limit, RECORDS_PAGE_BYTES)
            .await
            .map_err(error::model)?;
        // One store call when there is something to list (issue #124): the node is read only
        // to tell a missing node (`NOT_FOUND`) from one with no matching edges.
        if found.is_empty() {
            self.node_or_404(id).await?;
        }
        // A page that ended at its size or byte budget may have a next one; the token is the
        // last edge's place in the order (opaque to clients).
        let next_page_token = match found.last() {
            Some(last) if page_full(&found, limit, RECORDS_PAGE_BYTES, neighbour_page_bytes) => {
                EdgeCursor::of(&last.edge).to_string()
            }
            _ => String::new(),
        };
        Ok(Response::new(NeighboursResponse {
            neighbours: found
                .iter()
                .map(|n| Neighbour {
                    edge: Some(convert::edge(&n.edge)),
                    node: Some(convert::node(&n.node)),
                })
                .collect(),
            next_page_token,
        }))
    }

    async fn company_documents(
        &self,
        request: Request<CompanyDocumentsRequest>,
    ) -> Result<Response<CompanyDocumentsResponse>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        let id = validate::uuid(&mut v, "company_id", &req.company_id);
        let after = page(&mut v, req.page_size, &req.page_token);
        if after.is_some_and(|a| a.kind != EdgeKind::Party) {
            v.add("page_token", "not a token from a previous page");
        }
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("company_id: required"));
        };
        let limit = records_limit(req.page_size);
        let docs = self
            .state
            .backend()?
            .company_documents_page(id, after.as_ref(), limit, RECORDS_PAGE_BYTES)
            .await
            .map_err(error::model)?;
        // One store call when the company has documents (issue #124): documents come from
        // `party` edges, which always end at a company (ADR 0001 section 7), so the node is
        // read only when there are none, to tell a missing node (`NOT_FOUND`) or a
        // non-company (`INVALID_ARGUMENT`) from a company without documents.
        if docs.is_empty() {
            let node = self.node_or_404(id).await?;
            if node.kind != NodeKind::Company {
                let mut v = Violations::default();
                v.add(
                    "company_id",
                    format!("node {id} is a {}, not a company", node.kind),
                );
                v.check()?;
            }
        }
        let next_page_token = match docs.last() {
            Some(last)
                if page_full(&docs, limit, RECORDS_PAGE_BYTES, |d| {
                    node_page_bytes(&d.node)
                }) =>
            {
                last.edge.to_string()
            }
            _ => String::new(),
        };
        Ok(Response::new(CompanyDocumentsResponse {
            documents: docs.iter().map(|d| convert::node(&d.node)).collect(),
            next_page_token,
        }))
    }

    async fn list_companies(
        &self,
        request: Request<pb::ListCompaniesRequest>,
    ) -> Result<Response<pb::ListCompaniesResponse>, Status> {
        Ok(Response::new(
            self.list_companies_rpc(request.get_ref()).await?,
        ))
    }

    async fn company_graph(
        &self,
        request: Request<pb::CompanyGraphRequest>,
    ) -> Result<Response<pb::CompanyGraphResponse>, Status> {
        Ok(Response::new(
            self.company_graph_rpc(request.get_ref()).await?,
        ))
    }

    async fn subgraph(
        &self,
        request: Request<pb::SubgraphRequest>,
    ) -> Result<Response<pb::SubgraphResponse>, Status> {
        Ok(Response::new(self.subgraph_rpc(request.get_ref()).await?))
    }

    async fn path(
        &self,
        request: Request<pb::PathRequest>,
    ) -> Result<Response<pb::PathResponse>, Status> {
        Ok(Response::new(self.path_rpc(request.get_ref()).await?))
    }

    async fn list_merges(
        &self,
        request: Request<pb::ListMergesRequest>,
    ) -> Result<Response<pb::ListMergesResponse>, Status> {
        Ok(Response::new(
            self.list_merges_rpc(request.get_ref()).await?,
        ))
    }

    async fn merge_companies(
        &self,
        request: Request<pb::MergeCompaniesRequest>,
    ) -> Result<Response<pb::MergeEntry>, Status> {
        Ok(Response::new(
            self.merge_companies_rpc(request.get_ref()).await?,
        ))
    }

    async fn undo_merge(
        &self,
        request: Request<pb::UndoMergeRequest>,
    ) -> Result<Response<pb::UndoMergeResponse>, Status> {
        Ok(Response::new(self.undo_merge_rpc(request.get_ref()).await?))
    }

    async fn split_company(
        &self,
        request: Request<pb::SplitCompanyRequest>,
    ) -> Result<Response<pb::MergeEntry>, Status> {
        Ok(Response::new(
            self.split_company_rpc(request.get_ref()).await?,
        ))
    }

    async fn dismiss_pair(
        &self,
        request: Request<pb::DismissPairRequest>,
    ) -> Result<Response<pb::Dismissal>, Status> {
        Ok(Response::new(
            self.dismiss_pair_rpc(request.get_ref()).await?,
        ))
    }

    async fn list_dismissals(
        &self,
        request: Request<pb::ListDismissalsRequest>,
    ) -> Result<Response<pb::ListDismissalsResponse>, Status> {
        Ok(Response::new(
            self.list_dismissals_rpc(request.get_ref()).await?,
        ))
    }

    async fn list_proposals(
        &self,
        request: Request<pb::ListProposalsRequest>,
    ) -> Result<Response<pb::ListProposalsResponse>, Status> {
        Ok(Response::new(
            self.list_proposals_rpc(request.get_ref()).await?,
        ))
    }

    async fn accept_proposal(
        &self,
        request: Request<pb::AcceptProposalRequest>,
    ) -> Result<Response<pb::AcceptProposalResponse>, Status> {
        Ok(Response::new(
            self.accept_proposal_rpc(request.get_ref()).await?,
        ))
    }

    async fn reject_proposal(
        &self,
        request: Request<pb::RejectProposalRequest>,
    ) -> Result<Response<pb::RejectProposalResponse>, Status> {
        Ok(Response::new(
            self.reject_proposal_rpc(request.get_ref()).await?,
        ))
    }
}
