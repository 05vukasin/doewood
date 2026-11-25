//! The value types the store traits take and return.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use doewood_core::InstanceId;
use serde_json::Value;
use time::Date;
use uuid::Uuid;

use crate::Error;

/// Longest node key accepted, in bytes.
pub const NODE_KEY_MAX_LEN: usize = 2048;

/// Prefix of a pending reference's key: a Document node for a document that another one cites
/// by its number before that document itself is known (`ref:<number key>`).
pub const PENDING_REF_PREFIX: &str = "ref:";

/// The BLAKE3 hash of a file's bytes: a document's identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Blake3(pub [u8; 32]);

impl Blake3 {
    /// From exactly 32 bytes.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, Error> {
        bytes
            .try_into()
            .map(Self)
            .map_err(|_| Error::Invalid(format!("a BLAKE3 hash is 32 bytes, not {}", bytes.len())))
    }

    /// The raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex, the key of the document's graph node.
    pub fn to_hex(&self) -> String {
        use fmt::Write as _;
        self.0.iter().fold(String::with_capacity(64), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }
}

impl fmt::Display for Blake3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Blake3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Blake3({})", self.to_hex())
    }
}

impl FromStr for Blake3 {
    type Err = Error;
    /// Parses 64 hex digits (either case).
    fn from_str(s: &str) -> Result<Self, Error> {
        let bad = || Error::Invalid(format!("{s:?} is not 64 hex digits"));
        if s.len() != 64 || !s.is_ascii() {
            return Err(bad());
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| bad())?;
        }
        Ok(Self(out))
    }
}

/// A source's identity: an S3-compatible endpoint, a bucket and a prefix (`""` for the whole
/// bucket). Instances claim sources by this key.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceKey {
    endpoint: String,
    bucket: String,
    prefix: String,
}

impl SourceKey {
    /// Trims whitespace and trailing `/` from the endpoint, and whitespace from the bucket.
    /// The prefix is kept as given. The endpoint and bucket must not be empty.
    pub fn new(
        endpoint: impl AsRef<str>,
        bucket: impl AsRef<str>,
        prefix: impl Into<String>,
    ) -> Result<Self, Error> {
        let endpoint = endpoint.as_ref().trim().trim_end_matches('/').to_owned();
        let bucket = bucket.as_ref().trim().to_owned();
        if endpoint.is_empty() || bucket.is_empty() {
            return Err(Error::Invalid(
                "a source needs an endpoint and a bucket".to_owned(),
            ));
        }
        Ok(Self {
            endpoint,
            bucket,
            prefix: prefix.into(),
        })
    }

    /// The endpoint URL, without a trailing `/`.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The key prefix; empty for the whole bucket.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl fmt::Display for SourceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.endpoint, self.bucket, self.prefix)
    }
}

impl fmt::Debug for SourceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SourceKey({self})")
    }
}

/// Where one copy of a document lives: an endpoint, a bucket and an object key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectLocation {
    /// The endpoint URL, as in [`SourceKey::endpoint`].
    pub endpoint: String,
    /// The bucket.
    pub bucket: String,
    /// The object key.
    pub key: String,
}

impl ObjectLocation {
    /// The location as stored: the endpoint and bucket normalized like [`SourceKey::new`],
    /// so source filters match it. The endpoint, bucket and key must not be empty.
    pub fn checked(&self) -> Result<Self, Error> {
        let source = SourceKey::new(&self.endpoint, &self.bucket, "")?;
        if self.key.is_empty() {
            return Err(Error::Invalid("an object key cannot be empty".to_owned()));
        }
        Ok(Self {
            endpoint: source.endpoint,
            bucket: source.bucket,
            key: self.key.clone(),
        })
    }

    /// The key of the location's graph node: a JSON array, so no separator can be ambiguous.
    pub fn node_key(&self) -> String {
        Value::from(vec![
            self.endpoint.as_str(),
            self.bucket.as_str(),
            self.key.as_str(),
        ])
        .to_string()
    }
}

/// The step versions of a record: which quail or hedgehog step produced it, at which version.
/// `Reprocess` compares them with the running versions.
pub type StepVersions = BTreeMap<String, String>;

/// Step versions as a JSON object of strings, the form both backends store.
pub fn versions_to_json(versions: &StepVersions) -> Value {
    Value::Object(
        versions
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

/// Step versions back from [`versions_to_json`].
pub fn versions_from_json(value: &Value) -> Result<StepVersions, Error> {
    let bad = || Error::Invalid("step versions must be a JSON object of strings".to_owned());
    value
        .as_object()
        .ok_or_else(bad)?
        .iter()
        .map(|(k, v)| Ok((k.clone(), v.as_str().ok_or_else(bad)?.to_owned())))
        .collect()
}

/// One quail or hedgehog record, keyed by the file's BLAKE3.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    /// The file's hash.
    pub blake3: Blake3,
    /// The record, as JSON.
    pub body: Value,
    /// The step versions that produced it.
    pub versions: StepVersions,
    /// When it was last written, in Unix seconds.
    pub updated_at: i64,
}

/// Largest page of [`crate::RecordStore::list_records`].
pub const RECORDS_MAX_LIMIT: u32 = 1000;

/// The page size [`crate::RecordStore::list_records`] uses for `limit`.
pub fn records_limit(limit: u32) -> u32 {
    match limit {
        0 => 50,
        n => n.min(RECORDS_MAX_LIMIT),
    }
}

/// What a put did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// There was no row.
    Inserted,
    /// The row changed.
    Updated,
    /// The row already held exactly this; nothing was written.
    Unchanged,
}

macro_rules! text_enum {
    ($(#[$doc:meta])* $name:ident, $what:literal { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)+ }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $($(#[$vdoc])* $variant,)+
        }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &[Self] = &[$(Self::$variant,)+];

            /// The name stored in the database.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self, Error> {
                match s {
                    $($text => Ok(Self::$variant),)+
                    _ => Err(Error::Invalid(format!(concat!("unknown ", $what, " {:?}"), s))),
                }
            }
        }
    };
}

text_enum!(
    /// The kinds of graph node (ADR 0001, section 7).
    NodeKind, "node kind" {
        /// A file's content, keyed by its BLAKE3 in hex; or a pending reference to a document
        /// known only by its cited number, keyed by [`PENDING_REF_PREFIX`] and that number.
        Document = "document",
        /// Where a copy of a document lives, keyed by [`ObjectLocation::node_key`].
        Location = "location",
        /// A company, keyed by its stable group id or normalized name (ADR 0002, section 1).
        Company = "company",
    }
);

text_enum!(
    /// The kinds of graph edge: ADR 0001 section 7 between documents, locations and companies,
    /// and the dated corporate events of ADR 0002 section 3 between companies.
    EdgeKind, "edge kind" {
        /// Document → Location: a copy of the document lives there.
        LocatedAt = "located_at",
        /// Document → Company, with a `role` in the props (buyer, supplier, signatory, ...).
        Party = "party",
        /// Document → Document: amends the target.
        Amends = "amends",
        /// Document → Document: cites the target.
        Cites = "cites",
        /// Document → Document, with a `score` in the props (`MinHash` LSH, `SimHash`).
        SimilarTo = "similar_to",
        /// Company → Company: the source is a parent of the target.
        ParentOf = "parent_of",
        /// Company → Company: the source is a subsidiary of the target.
        SubsidiaryOf = "subsidiary_of",
        /// Company → Company: the source acquired the target.
        Acquired = "acquired",
        /// Company → Company: the source merged into the target.
        MergedInto = "merged_into",
        /// Company → Company: the source was renamed to the target.
        RenamedTo = "renamed_to",
        /// Company → Company: the source is a branch of the target.
        BranchOf = "branch_of",
        /// Company → Company: the source is a trading name of the target.
        TradingNameOf = "trading_name_of",
        /// Company → Company: the source was a parent of the target.
        FormerParentOf = "former_parent_of",
        /// Company → Company: the source was liquidated (the target is the liquidator or
        /// the company itself).
        Liquidated = "liquidated",
    }
);

text_enum!(
    /// Where an edge's claim comes from (ADR 0002, section 3).
    EdgeSource, "edge source" {
        /// A public registry.
        Registry = "registry",
        /// A statement in a document.
        Document = "document",
        /// A web page.
        Web = "web",
        /// A person.
        Manual = "manual",
        /// A storage listing: Document → Location edges.
        Storage = "storage",
    }
);

text_enum!(
    /// The life cycle of an edge (ADR 0002, section 3).
    EdgeState, "edge state" {
        /// Suggested (web, LLM); never active on its own.
        Proposed = "proposed",
        /// Believed true.
        Active = "active",
        /// Disputed by another claim.
        Contested = "contested",
        /// Withdrawn.
        Retracted = "retracted",
        /// Replaced by a newer claim.
        Superseded = "superseded",
    }
);

text_enum!(
    /// Who merged or split companies (ADR 0002 section 1.8).
    MergeActorKind, "merge actor" {
        /// Ingest's company matching.
        Ingest = "ingest",
        /// The enrichment job (registries).
        Enrichment = "enrichment",
        /// A person, through the API.
        Manual = "manual",
    }
);

text_enum!(
    /// What a merge log entry did.
    MergeOp, "merge operation" {
        /// One company absorbed another.
        Merge = "merge",
        /// One company was split in two.
        Split = "split",
    }
);

text_enum!(
    /// What an enrichment proposal proposes (ADR 0002 section 1.6, issue #52).
    ProposalKind, "proposal kind" {
        /// A registry record a name search found, for the company to adopt.
        RegistryRecord = "registry_record",
        /// Another company that may be the same, to merge.
        Merge = "merge",
    }
);

text_enum!(
    /// Why the gate left a decision to a person.
    ProposalReason, "proposal reason" {
        /// The fused probability is in the review band (0.50 to 0.90).
        ReviewBand = "review_band",
        /// Two conflicting high-precision hits.
        Conflict = "conflict",
        /// The evidence said merge, but the pair is dismissed (kept from props, schema 5).
        Dismissed = "dismissed",
    }
);

text_enum!(
    /// Where a proposal stands.
    ProposalState, "proposal state" {
        /// Waiting for a person.
        Open = "open",
        /// Accepted: merged, or the record adopted.
        Accepted = "accepted",
        /// Rejected.
        Rejected = "rejected",
    }
);

impl EdgeState {
    /// Whether neighbour queries return the edge by default: everything but retracted and
    /// superseded.
    pub fn is_live(self) -> bool {
        !matches!(self, Self::Retracted | Self::Superseded)
    }
}

/// A graph node.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    /// Assigned on first insert, then stable.
    pub id: Uuid,
    /// The kind.
    pub kind: NodeKind,
    /// Unique per kind.
    pub key: String,
    /// Attributes, as a JSON object.
    pub props: Value,
}

/// Checks a node key and props before they reach a backend.
pub fn validate_node(kind: NodeKind, key: &str, props: &Value) -> Result<(), Error> {
    if key.is_empty() || key.len() > NODE_KEY_MAX_LEN {
        return Err(Error::Invalid(format!(
            "a node key is 1 to {NODE_KEY_MAX_LEN} bytes"
        )));
    }
    if key.contains('\0') {
        return Err(Error::Invalid("a node key cannot contain NUL".to_owned()));
    }
    let pending_ref = key
        .strip_prefix(PENDING_REF_PREFIX)
        .is_some_and(|k| !k.is_empty());
    if kind == NodeKind::Document
        && !pending_ref
        && key.parse::<Blake3>().map(|b| b.to_hex()).ok().as_deref() != Some(key)
    {
        return Err(Error::Invalid(
            "a document key is its BLAKE3 in lowercase hex, or `ref:` and a cited number"
                .to_owned(),
        ));
    }
    if !props.is_object() {
        return Err(Error::Invalid(
            "node props must be a JSON object".to_owned(),
        ));
    }
    Ok(())
}

/// An edge to upsert. Its identity is `(src, dst, kind, source, valid_from)`: upserting the
/// same identity again replaces the props, evidence, confidence, state and `valid_to`.
#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    /// The source node.
    pub src: Uuid,
    /// The target node.
    pub dst: Uuid,
    /// The kind.
    pub kind: EdgeKind,
    /// Attributes, as a JSON object (`role`, `score`, ...).
    pub props: Value,
    /// Why we believe it: registry record ids, document spans, URLs. A JSON array.
    pub evidence: Value,
    /// From 0 to 1.
    pub confidence: f32,
    /// The life-cycle state.
    pub state: EdgeState,
    /// First day the edge holds; `None` means since always.
    pub valid_from: Option<Date>,
    /// First day the edge no longer holds; `None` means still holds.
    pub valid_to: Option<Date>,
    /// Where the claim comes from.
    pub source: EdgeSource,
}

impl Edge {
    /// An active, undated edge with confidence 1, empty props and no evidence.
    pub fn new(src: Uuid, dst: Uuid, kind: EdgeKind, source: EdgeSource) -> Self {
        Self {
            src,
            dst,
            kind,
            props: Value::Object(serde_json::Map::new()),
            evidence: Value::Array(Vec::new()),
            confidence: 1.0,
            state: EdgeState::Active,
            valid_from: None,
            valid_to: None,
            source,
        }
    }

    /// Sets the props.
    #[must_use]
    pub fn with_props(mut self, props: Value) -> Self {
        self.props = props;
        self
    }

    /// Sets the evidence.
    #[must_use]
    pub fn with_evidence(mut self, evidence: Value) -> Self {
        self.evidence = evidence;
        self
    }

    /// Sets the confidence.
    #[must_use]
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence;
        self
    }

    /// Sets the state.
    #[must_use]
    pub fn with_state(mut self, state: EdgeState) -> Self {
        self.state = state;
        self
    }

    /// Sets the validity: from `from` (inclusive) to `to` (exclusive).
    #[must_use]
    pub fn valid(mut self, from: Option<Date>, to: Option<Date>) -> Self {
        self.valid_from = from;
        self.valid_to = to;
        self
    }

    /// Checks the edge before it reaches a backend.
    pub fn validate(&self) -> Result<(), Error> {
        if !(0.0..=1.0).contains(&self.confidence) {
            return Err(Error::Invalid(format!(
                "confidence {} is not between 0 and 1",
                self.confidence
            )));
        }
        if !self.props.is_object() {
            return Err(Error::Invalid(
                "edge props must be a JSON object".to_owned(),
            ));
        }
        if !self.evidence.is_array() {
            return Err(Error::Invalid(
                "edge evidence must be a JSON array".to_owned(),
            ));
        }
        if let (Some(from), Some(to)) = (self.valid_from, self.valid_to)
            && to < from
        {
            return Err(Error::Invalid(format!(
                "valid_to {to} is before valid_from {from}"
            )));
        }
        Ok(())
    }

    /// Whether the edge holds on `day`.
    pub fn holds_on(&self, day: Date) -> bool {
        self.valid_from.is_none_or(|f| f <= day) && self.valid_to.is_none_or(|t| day < t)
    }
}

/// A stored edge with its id.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEdge {
    /// Assigned on first insert, then stable.
    pub id: Uuid,
    /// The edge.
    pub edge: Edge,
}

/// Which way to follow edges from a node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Direction {
    /// Edges whose source is the node.
    Out,
    /// Edges whose target is the node.
    In,
    /// Both.
    #[default]
    Both,
}

/// A neighbour query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NeighbourQuery {
    /// Which way to follow edges.
    pub direction: Direction,
    /// Only these edge kinds; empty means all.
    pub edge_kinds: Vec<EdgeKind>,
    /// Only neighbours of this kind.
    pub node_kind: Option<NodeKind>,
    /// Only edges that hold on this day (see [`Edge::holds_on`]).
    pub at: Option<Date>,
    /// Also return retracted and superseded edges.
    pub include_inactive: bool,
}

/// One neighbour: the edge followed and the node at its other end.
#[derive(Clone, Debug, PartialEq)]
pub struct Neighbour {
    /// The edge.
    pub edge: StoredEdge,
    /// The node at the other end.
    pub node: Node,
}

/// The nodes [`crate::GraphStore::upsert_location`] touched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Located {
    /// The Document node.
    pub document: Uuid,
    /// The Location node.
    pub location: Uuid,
}

/// What [`crate::GraphStore::end_location`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocationEnded {
    /// The Document node that lived there.
    pub document: Uuid,
    /// The Location node.
    pub location: Uuid,
    /// Live `located_at` edges the document still has elsewhere.
    pub remaining: u32,
}

/// An alias of a node: an identifier or a name key that resolves to it.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeAlias {
    /// The node it resolves to; `None` when the alias is blocked.
    pub node: Option<Uuid>,
    /// Why: the evidence and confidence of the claim (a JSON object).
    pub props: Value,
}

/// Checks an alias before it reaches a backend: the same rules as a node key.
pub fn validate_alias(alias: &str, props: &Value) -> Result<(), Error> {
    if alias.is_empty() || alias.len() > NODE_KEY_MAX_LEN || alias.contains('\0') {
        return Err(Error::Invalid(format!(
            "an alias is 1 to {NODE_KEY_MAX_LEN} bytes without NUL"
        )));
    }
    if !props.is_object() {
        return Err(Error::Invalid(
            "alias props must be a JSON object".to_owned(),
        ));
    }
    Ok(())
}

/// A document's `MinHash` signature in the LSH band index, as a candidate lookup returns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sketch {
    /// The document.
    pub blake3: Blake3,
    /// Its `MinHash` signature, as quail writes it.
    pub signature: Vec<u8>,
    /// How many of the looked-up band keys it shares.
    pub shared_bands: u32,
}

/// Most band keys one sketch carries.
pub const SKETCH_MAX_BANDS: usize = 256;

/// Largest `MinHash` signature accepted, in bytes.
pub const SKETCH_MAX_SIGNATURE: usize = 64 * 1024;

/// Checks a sketch before it reaches a backend.
pub fn validate_sketch(signature: &[u8], bands: &[u64]) -> Result<(), Error> {
    if signature.is_empty() || signature.len() > SKETCH_MAX_SIGNATURE {
        return Err(Error::Invalid(format!(
            "a MinHash signature is 1 to {SKETCH_MAX_SIGNATURE} bytes"
        )));
    }
    if bands.len() > SKETCH_MAX_BANDS {
        return Err(Error::Invalid(format!(
            "a sketch has at most {SKETCH_MAX_BANDS} band keys"
        )));
    }
    Ok(())
}

/// A band key as both backends store it: the same 64 bits as a signed integer.
#[allow(clippy::cast_possible_wrap)]
pub fn band_to_i64(band: u64) -> i64 {
    band as i64
}

/// Who owns a source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceClaim {
    /// The source.
    pub key: SourceKey,
    /// The owning instance.
    pub owner: InstanceId,
    /// When it was first claimed, in Unix seconds.
    pub claimed_at: i64,
}

/// A document's searchable text and filter fields.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchDoc {
    /// The document; its record must exist.
    pub blake3: Blake3,
    /// The title (weighted above the body).
    pub title: String,
    /// The text.
    pub body: String,
    /// The document kind (`invoice`, `contract`, ...).
    pub kind: Option<String>,
    /// The document's own date.
    pub date: Option<Date>,
    /// The document's main amount.
    pub amount: Option<f64>,
}

/// Largest page of search hits.
pub const SEARCH_MAX_LIMIT: u32 = 1000;

/// A search. Every filter that is set must match. The text matches documents containing
/// every word of it; only plain words are portable across backends.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchQuery {
    /// Words to match in the title or body; `None` (or no words) matches every document.
    pub text: Option<String>,
    /// Only documents with a live `party` edge to this company.
    pub company: Option<Uuid>,
    /// Only documents of this kind.
    pub kind: Option<String>,
    /// Only documents dated on or after this day.
    pub date_from: Option<Date>,
    /// Only documents dated on or before this day.
    pub date_to: Option<Date>,
    /// Only documents with an amount of at least this.
    pub amount_min: Option<f64>,
    /// Only documents with an amount of at most this.
    pub amount_max: Option<f64>,
    /// Only documents with a location in this source.
    pub source: Option<SourceKey>,
    /// Page size; 0 means 50, and at most [`SEARCH_MAX_LIMIT`].
    pub limit: u32,
    /// Hits to skip.
    pub offset: u32,
}

impl SearchQuery {
    /// The words of the text: runs of letters and digits.
    pub fn words(&self) -> Vec<String> {
        self.text
            .as_deref()
            .unwrap_or("")
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(str::to_lowercase)
            .collect()
    }

    /// The page size to use.
    pub fn effective_limit(&self) -> u32 {
        match self.limit {
            0 => 50,
            n => n.min(SEARCH_MAX_LIMIT),
        }
    }
}

/// One search hit. Ordered by score, then date (newest first), then hash.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    /// The document.
    pub blake3: Blake3,
    /// Its title.
    pub title: String,
    /// Its kind.
    pub kind: Option<String>,
    /// Its date.
    pub date: Option<Date>,
    /// Its amount.
    pub amount: Option<f64>,
    /// The text relevance; higher is better, 0 without text. Not comparable across backends.
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blake3_hex_round_trip() {
        let b = Blake3([0xab; 32]);
        assert_eq!(b.to_hex(), "ab".repeat(32));
        assert_eq!(b.to_hex().parse::<Blake3>().unwrap(), b);
        assert_eq!("AB".repeat(32).parse::<Blake3>().unwrap(), b);
        assert!("ab".parse::<Blake3>().is_err());
        assert!("zz".repeat(32).parse::<Blake3>().is_err());
        assert!(Blake3::from_slice(&[1; 31]).is_err());
    }

    #[test]
    fn source_keys_are_normalized() {
        let k = SourceKey::new(" http://minio:9000/ ", "docs ", "inbox/").unwrap();
        assert_eq!(k.endpoint(), "http://minio:9000");
        assert_eq!(k.bucket(), "docs");
        assert_eq!(k.prefix(), "inbox/");
        assert!(SourceKey::new("", "b", "").is_err());
        assert!(SourceKey::new("http://x", " ", "").is_err());
    }

    #[test]
    fn enums_round_trip_their_names() {
        for k in EdgeKind::ALL {
            assert_eq!(k.as_str().parse::<EdgeKind>().unwrap(), *k);
        }
        for k in NodeKind::ALL {
            assert_eq!(k.as_str().parse::<NodeKind>().unwrap(), *k);
        }
        assert!("nope".parse::<EdgeState>().is_err());
    }

    #[test]
    fn node_and_edge_checks() {
        let obj = serde_json::json!({});
        assert!(validate_node(NodeKind::Company, "acme", &obj).is_ok());
        assert!(validate_node(NodeKind::Company, "", &obj).is_err());
        assert!(validate_node(NodeKind::Company, "a", &serde_json::json!([])).is_err());
        assert!(validate_node(NodeKind::Document, "acme", &obj).is_err());
        assert!(validate_node(NodeKind::Document, &"AB".repeat(32), &obj).is_err());
        assert!(validate_node(NodeKind::Document, &"ab".repeat(32), &obj).is_ok());
        assert!(validate_node(NodeKind::Document, "ref:inv-42", &obj).is_ok());
        assert!(validate_node(NodeKind::Document, "ref:", &obj).is_err());

        let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
        let day = |d| Date::from_calendar_date(2020, time::Month::May, d).unwrap();
        let e = Edge::new(a, b, EdgeKind::ParentOf, EdgeSource::Registry);
        assert!(e.validate().is_ok());
        assert!(e.clone().with_confidence(1.5).validate().is_err());
        assert!(
            e.clone()
                .valid(Some(day(2)), Some(day(1)))
                .validate()
                .is_err()
        );
        let dated = e.valid(Some(day(2)), Some(day(4)));
        assert!(!dated.holds_on(day(1)));
        assert!(dated.holds_on(day(2)));
        assert!(dated.holds_on(day(3)));
        assert!(!dated.holds_on(day(4)));
    }

    #[test]
    fn search_words_and_limits() {
        let q = SearchQuery {
            text: Some("  Invoice, ACME-42! ".to_owned()),
            ..SearchQuery::default()
        };
        assert_eq!(q.words(), ["invoice", "acme", "42"]);
        assert_eq!(q.effective_limit(), 50);
        let q = SearchQuery {
            limit: 5000,
            ..SearchQuery::default()
        };
        assert_eq!(q.effective_limit(), SEARCH_MAX_LIMIT);
    }
}
