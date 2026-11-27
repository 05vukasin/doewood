//! `GraphService.SimilarDocuments` (issue #169): a document's similar documents by relation,
//! read from its live `similar_to` edges.
//!
//! The edges at the document are read in their index order, a store page at a time, up to
//! [`EDGES_READ_MAX`]. Each edge gives one entry per relation it holds
//! ([`doewood_model::similar::edge_relations`]; an edge from before issue #169 is a near copy
//! or a related version by its `score`). The entries are counted per relation, a relation
//! with more than `similar_crowd_size` matches is a crowd (left out unless asked for), and
//! the rest are sorted (relation, its similarity high to low: masked for a template, raw for
//! the others, then the other one, then hash) and paged by the last entry's place in that
//! order.

use std::cmp::Ordering;

use doewood_model::similar::{EdgeRelation, edge_relations};
use doewood_model::{
    Direction, EdgeKind, NeighbourQuery, NodeKind, RECORDS_MAX_LIMIT, RECORDS_PAGE_BYTES,
    SimilarRelation, page_full, records_limit,
};
use tonic::Status;

use crate::pb;
use crate::validate::{self, Violations};
use crate::{Api, error};

/// Most `similar_to` edges one call reads.
pub(crate) const EDGES_READ_MAX: usize = 10_000;

/// One entry before it is paged.
struct Entry {
    other: String,
    rel: EdgeRelation,
}

/// The place of an entry in the order, as a page token:
/// `<relation>.<raw bits>.<masked bits>.<hash>`, a similarity's bits in hex (empty when
/// unknown).
#[derive(Clone, Debug, PartialEq)]
struct Place {
    relation: SimilarRelation,
    raw: Option<f64>,
    masked: Option<f64>,
    other: String,
}

impl Place {
    fn of(e: &Entry) -> Self {
        Self {
            relation: e.rel.relation,
            raw: e.rel.raw_similarity,
            masked: e.rel.masked_similarity,
            other: e.other.clone(),
        }
    }

    fn to_token(&self) -> String {
        let bits = |x: Option<f64>| {
            x.map(|x| format!("{:016x}", x.to_bits()))
                .unwrap_or_default()
        };
        format!(
            "{}.{}.{}.{}",
            self.relation.as_str(),
            bits(self.raw),
            bits(self.masked),
            self.other
        )
    }

    fn parse(token: &str) -> Option<Self> {
        let mut parts = token.split('.');
        let relation = SimilarRelation::parse(parts.next()?)?;
        let mut bits = || -> Option<Option<f64>> {
            match parts.next()? {
                "" => Some(None),
                b if b.len() == 16 => u64::from_str_radix(b, 16)
                    .ok()
                    .map(|b| Some(f64::from_bits(b)))
                    .filter(|x| x.is_none_or(f64::is_finite)),
                _ => None,
            }
        };
        let (raw, masked) = (bits()?, bits()?);
        let other = parts.next()?.to_owned();
        if parts.next().is_some() || other.is_empty() {
            return None;
        }
        Some(Self {
            relation,
            raw,
            masked,
            other,
        })
    }
}

/// Higher first, unknown last.
fn high_first(a: Option<f64>, b: Option<f64>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => b.total_cmp(&a),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Relation, then its own similarity high to low (masked for a template, raw for the
/// others), then the other one, then hash.
fn order(a: &Place, b: &Place) -> Ordering {
    let by_masked = a.relation == SimilarRelation::SameTemplate;
    let (first, second) = if by_masked {
        (high_first(a.masked, b.masked), high_first(a.raw, b.raw))
    } else {
        (high_first(a.raw, b.raw), high_first(a.masked, b.masked))
    };
    a.relation
        .cmp(&b.relation)
        .then(first)
        .then(second)
        .then_with(|| a.other.cmp(&b.other))
}

fn relation_from(r: pb::SimilarRelation) -> Option<SimilarRelation> {
    match r {
        pb::SimilarRelation::Unspecified => None,
        pb::SimilarRelation::Identical => Some(SimilarRelation::Identical),
        pb::SimilarRelation::NearCopy => Some(SimilarRelation::NearCopy),
        pb::SimilarRelation::RelatedVersion => Some(SimilarRelation::RelatedVersion),
        pb::SimilarRelation::SameTemplate => Some(SimilarRelation::SameTemplate),
        pb::SimilarRelation::SameOrigin => Some(SimilarRelation::SameOrigin),
    }
}

fn relation_to(r: SimilarRelation) -> pb::SimilarRelation {
    match r {
        SimilarRelation::Identical => pb::SimilarRelation::Identical,
        SimilarRelation::NearCopy => pb::SimilarRelation::NearCopy,
        SimilarRelation::RelatedVersion => pb::SimilarRelation::RelatedVersion,
        SimilarRelation::SameTemplate => pb::SimilarRelation::SameTemplate,
        SimilarRelation::SameOrigin => pb::SimilarRelation::SameOrigin,
    }
}

impl Api {
    pub(crate) async fn similar_documents_rpc(
        &self,
        req: &pb::SimilarDocumentsRequest,
    ) -> Result<pb::SimilarDocumentsResponse, Status> {
        let mut v = Violations::default();
        let blake3 = validate::blake3(&mut v, "blake3", &req.blake3);
        let mut asked: Vec<SimilarRelation> = Vec::new();
        for (i, r) in req.relations.iter().enumerate() {
            match pb::SimilarRelation::try_from(*r)
                .ok()
                .and_then(relation_from)
            {
                Some(r) => asked.push(r),
                None => v.add(format!("relations[{i}]"), format!("unknown value {r}")),
            }
        }
        if req.limit > RECORDS_MAX_LIMIT {
            v.add("limit", format!("at most {RECORDS_MAX_LIMIT}"));
        }
        let after = if req.page_token.is_empty() {
            None
        } else {
            let p = Place::parse(&req.page_token);
            if p.is_none() {
                v.add("page_token", "not a token from a previous page");
            }
            p
        };
        v.check()?;
        let Some(blake3) = blake3 else {
            return Err(Status::invalid_argument("blake3: required"));
        };
        if asked.is_empty() {
            asked = SimilarRelation::ALL.to_vec();
        }
        asked.sort();
        asked.dedup();
        let limit = records_limit(req.limit) as usize;
        let settings = self.state.db(doewood_store::Store::ingest_settings).await?;
        let b = self.state.backend()?;
        let hex = blake3.to_hex();
        let node = b
            .node_by_key(NodeKind::Document, &hex)
            .await
            .map_err(error::model)?
            .ok_or_else(|| Status::not_found(format!("document {hex} is not known")))?;
        let query = NeighbourQuery {
            direction: Direction::Both,
            edge_kinds: vec![EdgeKind::SimilarTo],
            node_kind: Some(NodeKind::Document),
            ..NeighbourQuery::default()
        };
        let mut entries: Vec<Entry> = Vec::new();
        let (mut read, mut truncated, mut cursor) = (0usize, false, None);
        loop {
            let page = b
                .neighbours_page(
                    node.id,
                    &query,
                    cursor.as_ref(),
                    RECORDS_MAX_LIMIT,
                    RECORDS_PAGE_BYTES,
                )
                .await
                .map_err(error::model)?;
            let full = page_full(
                &page,
                RECORDS_MAX_LIMIT,
                RECORDS_PAGE_BYTES,
                doewood_model::neighbour_page_bytes,
            );
            cursor = page.last().map(|n| doewood_model::EdgeCursor::of(&n.edge));
            for n in page {
                if read == EDGES_READ_MAX {
                    truncated = true;
                    break;
                }
                read += 1;
                // A pending reference or a node with another key is not a document hash.
                if n.node.key.len() != 64 || n.node.key == hex {
                    continue;
                }
                for rel in
                    edge_relations(&n.edge.edge.props, settings.similar_near_copy_min_jaccard)
                {
                    if asked.contains(&rel.relation) {
                        entries.push(Entry {
                            other: n.node.key.clone(),
                            rel,
                        });
                    }
                }
            }
            if truncated || !full {
                break;
            }
        }
        // Counts and crowds over every entry read.
        let crowd = settings.similar_crowd_size as usize;
        let relations: Vec<pb::SimilarRelationCount> = asked
            .iter()
            .map(|r| {
                let matches = entries.iter().filter(|e| e.rel.relation == *r).count();
                pb::SimilarRelationCount {
                    relation: relation_to(*r) as i32,
                    matches: u32::try_from(matches).unwrap_or(u32::MAX),
                    crowded: matches > crowd,
                }
            })
            .collect();
        let crowded: Vec<SimilarRelation> = asked
            .iter()
            .zip(&relations)
            .filter(|(_, c)| c.crowded)
            .map(|(r, _)| *r)
            .collect();
        let mut listed: Vec<(Place, Entry)> = entries
            .into_iter()
            .filter(|e| req.include_crowded || !crowded.contains(&e.rel.relation))
            .map(|e| (Place::of(&e), e))
            .filter(|(p, _)| {
                after
                    .as_ref()
                    .is_none_or(|a| order(p, a) == Ordering::Greater)
            })
            .collect();
        listed.sort_by(|a, b| order(&a.0, &b.0));
        let more = listed.len() > limit;
        listed.truncate(limit);
        let next_page_token = match listed.last() {
            Some((p, _)) if more => p.to_token(),
            _ => String::new(),
        };
        let documents = listed
            .into_iter()
            .map(|(_, e)| pb::SimilarDocument {
                blake3: e.other,
                relation: relation_to(e.rel.relation) as i32,
                raw_similarity: e.rel.raw_similarity,
                masked_similarity: e.rel.masked_similarity,
                method: e.rel.method.to_owned(),
                detail: e.rel.detail.to_string(),
            })
            .collect();
        Ok(pb::SimilarDocumentsResponse {
            documents,
            next_page_token,
            relations,
            truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_round_trip_and_bad_ones_are_refused() {
        let p = Place {
            relation: SimilarRelation::NearCopy,
            raw: Some(0.93),
            masked: None,
            other: "ab".repeat(32),
        };
        assert_eq!(Place::parse(&p.to_token()), Some(p.clone()));
        for bad in [
            "",
            "near_copy",
            "later...ab",
            "near_copy.zz.. ab",
            "near_copy...",
            "near_copy....x",
            "near_copy.7ff8000000000000..ab",
        ] {
            assert_eq!(Place::parse(bad), None, "{bad}");
        }
        // The order: relation, raw high to low (unknown last), masked, hash.
        let q = Place {
            raw: Some(0.95),
            ..p.clone()
        };
        assert_eq!(order(&q, &p), Ordering::Less);
        let r = Place {
            raw: None,
            ..p.clone()
        };
        assert_eq!(order(&p, &r), Ordering::Less);
        let s = Place {
            relation: SimilarRelation::Identical,
            ..r.clone()
        };
        assert_eq!(order(&s, &p), Ordering::Less);
    }
}
