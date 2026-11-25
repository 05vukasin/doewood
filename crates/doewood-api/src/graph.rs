//! `GraphService`: nodes, neighbours and a company's documents, and the merge log (in
//! `merges`).

use doewood_model::{NODE_KEY_MAX_LEN, NeighbourQuery, NodeKind};
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
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("node_id: required"));
        };
        self.node_or_404(id).await?;
        let query = NeighbourQuery {
            direction,
            edge_kinds,
            node_kind,
            at,
            include_inactive: req.include_inactive,
        };
        let found = self
            .state
            .backend()?
            .neighbours(id, &query)
            .await
            .map_err(error::model)?;
        Ok(Response::new(NeighboursResponse {
            neighbours: found
                .iter()
                .map(|n| Neighbour {
                    edge: Some(convert::edge(&n.edge)),
                    node: Some(convert::node(&n.node)),
                })
                .collect(),
        }))
    }

    async fn company_documents(
        &self,
        request: Request<CompanyDocumentsRequest>,
    ) -> Result<Response<CompanyDocumentsResponse>, Status> {
        let mut v = Violations::default();
        let id = validate::uuid(&mut v, "company_id", &request.get_ref().company_id);
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("company_id: required"));
        };
        let node = self.node_or_404(id).await?;
        if node.kind != NodeKind::Company {
            let mut v = Violations::default();
            v.add(
                "company_id",
                format!("node {id} is a {}, not a company", node.kind),
            );
            v.check()?;
        }
        let docs = self
            .state
            .backend()?
            .company_documents(id)
            .await
            .map_err(error::model)?;
        Ok(Response::new(CompanyDocumentsResponse {
            documents: docs.iter().map(convert::node).collect(),
        }))
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
}
