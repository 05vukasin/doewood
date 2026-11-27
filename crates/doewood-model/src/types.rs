//! The value types the store traits take and return.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use doewood_core::InstanceId;
use serde_json::Value;
use serde_json::value::RawValue;
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

/// The endpoint of a push source (ADR 0004): `push:` and the source's id. A push source has
/// no bucket and no prefix: its key and its locations are `push:<source id>` and the key the
/// application gave.
pub const PUSH_ENDPOINT_PREFIX: &str = "push:";

/// The endpoint of the push source with this id (`push:<id>`).
pub fn push_endpoint(id: impl fmt::Display) -> String {
    format!("{PUSH_ENDPOINT_PREFIX}{id}")
}

/// Whether `endpoint` is a push source's (`push:<id>`), not an S3 URL.
pub fn is_push_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with(PUSH_ENDPOINT_PREFIX)
}

/// A source's identity: an S3-compatible endpoint, a bucket and a prefix (`""` for the whole
/// bucket), or a push source's `push:<id>` with no bucket and no prefix (ADR 0004). Instances
/// claim sources by this key.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceKey {
    endpoint: String,
    bucket: String,
    prefix: String,
}

impl SourceKey {
    /// Trims whitespace and trailing `/` from the endpoint, and whitespace from the bucket.
    /// The prefix is kept as given. The endpoint and bucket must not be empty, except for a
    /// push source (`push:<id>`, [`is_push_endpoint`]), which has neither bucket nor prefix.
    pub fn new(
        endpoint: impl AsRef<str>,
        bucket: impl AsRef<str>,
        prefix: impl Into<String>,
    ) -> Result<Self, Error> {
        let endpoint = endpoint.as_ref().trim().trim_end_matches('/').to_owned();
        let bucket = bucket.as_ref().trim().to_owned();
        let prefix = prefix.into();
        if is_push_endpoint(&endpoint) {
            if endpoint.len() == PUSH_ENDPOINT_PREFIX.len()
                || !bucket.is_empty()
                || !prefix.is_empty()
            {
                return Err(Error::Invalid(
                    "a push source is `push:<id>`, with no bucket and no prefix".to_owned(),
                ));
            }
        } else if endpoint.is_empty() || bucket.is_empty() {
            return Err(Error::Invalid(
                "a source needs an endpoint and a bucket".to_owned(),
            ));
        }
        Ok(Self {
            endpoint,
            bucket,
            prefix,
        })
    }

    /// The key of the push source with this id: `push:<id>`, no bucket, no prefix.
    pub fn push(id: impl fmt::Display) -> Self {
        Self {
            endpoint: push_endpoint(id),
            bucket: String::new(),
            prefix: String::new(),
        }
    }

    /// Whether this is a push source's key.
    pub fn is_push(&self) -> bool {
        is_push_endpoint(&self.endpoint)
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
    /// so source filters match it. The endpoint, bucket and key must not be empty (a push
    /// location, `push:<id>`, has no bucket).
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
    /// `[endpoint, bucket, key]` for an S3 object, `["push:<source id>", key]` for a pushed
    /// one (ADR 0004).
    pub fn node_key(&self) -> String {
        if is_push_endpoint(&self.endpoint) && self.bucket.is_empty() {
            return Value::from(vec![self.endpoint.as_str(), self.key.as_str()]).to_string();
        }
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
#[derive(Clone, Debug)]
pub struct Record {
    /// The file's hash.
    pub blake3: Blake3,
    /// The record, as JSON text (compact, keys sorted: as
    /// [`crate::canonical::to_canonical`] writes it). Readers deserialize the fields they need
    /// from it, without a `Value` tree (issue #98).
    pub body: Box<RawValue>,
    /// The step versions that produced it.
    pub versions: StepVersions,
    /// When it was last written, in Unix seconds.
    pub updated_at: i64,
}

impl Record {
    /// The body as a `Value` tree: for tests and the few writers that edit a record.
    pub fn body_value(&self) -> Result<Value, Error> {
        serde_json::from_str(self.body.get())
            .map_err(|e| Error::Invalid(format!("record body is not JSON: {e}")))
    }
}

/// Records are equal when their hashes, versions, times and body texts are.
impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.blake3 == other.blake3
            && self.body.get() == other.body.get()
            && self.versions == other.versions
            && self.updated_at == other.updated_at
    }
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

/// The byte budget of a page of [`crate::RecordStore::list_records`] that the API uses: 3 MiB
/// of record bodies (issue #111). A page of 1000 big records would otherwise be held in memory
/// several times over and only then meet the 64 MiB message cap. A page ends with the record
/// that reaches the budget, so with records up to about 1 MiB a page stays under the 4 MiB
/// that gRPC clients accept by default.
pub const RECORDS_PAGE_BYTES: usize = 3 << 20;

/// What a record counts against a page's byte budget: its body's JSON text.
pub fn record_page_bytes(record: &Record) -> usize {
    record.body.get().len()
}

/// Whether a page [`crate::RecordStore::list_records`] returned for `limit` and `max_bytes`
/// ended at either bound, so a next page may follow.
pub fn records_page_full(page: &[Record], limit: u32, max_bytes: usize) -> bool {
    page.len() >= records_limit(limit) as usize
        || page.iter().map(record_page_bytes).sum::<usize>() >= max_bytes
}

/// Checks a batch of hashes for [`crate::RecordStore::record_versions`] and
/// [`crate::RecordStore::fact_texts`]: at most [`RECORDS_MAX_LIMIT`].
///
/// # Errors
///
/// [`Error::Invalid`] when there are more.
pub fn validate_hashes(hashes: &[Blake3]) -> Result<(), Error> {
    if hashes.len() > RECORDS_MAX_LIMIT as usize {
        return Err(Error::Invalid(format!(
            "at most {RECORDS_MAX_LIMIT} hashes per call"
        )));
    }
    Ok(())
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

/// What [`crate::GraphStore::upsert_live_edge`] wrote: the edge's id and the ends it landed
/// on, after following `merged_into` from merged-away companies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveEdge {
    /// The edge's id.
    pub id: Uuid,
    /// The source it landed on.
    pub src: Uuid,
    /// The target it landed on.
    pub dst: Uuid,
    /// `merged_into` hops followed, both ends together: 0 when both ends were live.
    pub redirects: u32,
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

/// A position in the order of [`crate::GraphStore::neighbours`] (issue #112): an edge's kind,
/// its `valid_from` (undated first) and its id. A page of
/// [`crate::GraphStore::neighbours_page`] or [`crate::GraphStore::company_documents_page`]
/// starts after it. As text (a page token) it is `<kind>.<Julian day of valid_from, empty when
/// undated>.<id>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgeCursor {
    /// The edge's kind.
    pub kind: EdgeKind,
    /// The day the edge starts to hold; `None` for an undated edge.
    pub valid_from: Option<Date>,
    /// The edge's id.
    pub id: Uuid,
}

impl EdgeCursor {
    /// The position of a stored edge.
    pub fn of(edge: &StoredEdge) -> Self {
        Self {
            kind: edge.edge.kind,
            valid_from: edge.edge.valid_from,
            id: edge.id,
        }
    }
}

impl fmt::Display for EdgeCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.", self.kind)?;
        if let Some(d) = self.valid_from {
            write!(f, "{}", d.to_julian_day())?;
        }
        write!(f, ".{}", self.id.as_hyphenated())
    }
}

impl FromStr for EdgeCursor {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let bad = || Error::Invalid(format!("not an edge position: {s:?}"));
        let mut parts = s.splitn(3, '.');
        let (Some(kind), Some(day), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(bad());
        };
        let valid_from = if day.is_empty() {
            None
        } else {
            let day: i32 = day.parse().map_err(|_| bad())?;
            Some(Date::from_julian_day(day).map_err(|_| bad())?)
        };
        Ok(Self {
            kind: kind.parse().map_err(|_| bad())?,
            valid_from,
            id: Uuid::try_parse(id).map_err(|_| bad())?,
        })
    }
}

/// A document of [`crate::GraphStore::company_documents_page`]: the node, and the position of
/// its first live `party` edge to the company, which orders it.
#[derive(Clone, Debug, PartialEq)]
pub struct PartyDocument {
    /// The Document node.
    pub node: Node,
    /// Its first live `party` edge to the company.
    pub edge: EdgeCursor,
}

/// The length of `value` as JSON text, counted without building the text.
pub fn json_len(value: &Value) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut c = Count(0);
    // Writing a `Value` to a sink that never fails cannot fail.
    let _ = serde_json::to_writer(&mut c, value);
    c.0
}

/// What a node counts against a page's byte budget (issue #112): its key and its props' JSON
/// text.
pub fn node_page_bytes(node: &Node) -> usize {
    node.key.len() + json_len(&node.props)
}

/// What a neighbour counts against a page's byte budget: its node's bytes
/// ([`node_page_bytes`]) and its edge's props and evidence as JSON text.
pub fn neighbour_page_bytes(n: &Neighbour) -> usize {
    node_page_bytes(&n.node) + json_len(&n.edge.edge.props) + json_len(&n.edge.edge.evidence)
}

/// Whether a page of `limit` items (as [`records_limit`] reads it) and `max_bytes`, each
/// counted by `bytes`, ended at either bound, so a next page may follow: the rule
/// [`crate::GraphStore::neighbours_page`] and [`crate::GraphStore::company_documents_page`]
/// stop by.
pub fn page_full<T>(page: &[T], limit: u32, max_bytes: usize, bytes: impl Fn(&T) -> usize) -> bool {
    page.len() >= records_limit(limit) as usize
        || page.iter().map(bytes).sum::<usize>() >= max_bytes
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

/// Most aliases per [`crate::GraphStore::aliases`] or [`crate::GraphStore::claim_aliases`] call.
pub const ALIASES_MAX: usize = 1000;

/// Refuses more than [`ALIASES_MAX`] aliases in one call.
pub fn validate_alias_count(n: usize) -> Result<(), Error> {
    if n > ALIASES_MAX {
        return Err(Error::Invalid(format!(
            "at most {ALIASES_MAX} aliases per call"
        )));
    }
    Ok(())
}

/// Most keys per [`crate::GraphStore::node_ids`] call and edges per
/// [`crate::GraphStore::upsert_edges`] call.
pub const GRAPH_BATCH_MAX: usize = 1000;

/// Refuses more than [`GRAPH_BATCH_MAX`] keys or edges in one call.
pub fn validate_batch_count(n: usize) -> Result<(), Error> {
    if n > GRAPH_BATCH_MAX {
        return Err(Error::Invalid(format!(
            "at most {GRAPH_BATCH_MAX} keys or edges per call"
        )));
    }
    Ok(())
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

/// What [`crate::SketchStore::sketch_candidates_within`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Candidates {
    /// The candidates, as [`crate::SketchStore::sketch_candidates`] returns them over the
    /// bands that were looked at.
    pub sketches: Vec<Sketch>,
    /// Distinct band keys skipped because their bucket was too large.
    pub skipped_bands: u32,
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
    fn edge_cursor_round_trip() {
        let id = Uuid::now_v7();
        for valid_from in [
            None,
            Some(Date::from_calendar_date(2024, time::Month::March, 9).unwrap()),
        ] {
            let c = EdgeCursor {
                kind: EdgeKind::SubsidiaryOf,
                valid_from,
                id,
            };
            let text = c.to_string();
            assert_eq!(text.parse::<EdgeCursor>().unwrap(), c, "{text}");
        }
        assert_eq!(
            EdgeCursor {
                kind: EdgeKind::Party,
                valid_from: None,
                id: Uuid::nil(),
            }
            .to_string(),
            "party..00000000-0000-0000-0000-000000000000"
        );
        for bad in [
            "",
            "party",
            "party.",
            "party..x",
            "nope..00000000-0000-0000-0000-000000000000",
            "party.x.00000000-0000-0000-0000-000000000000",
            "party.99999999999.00000000-0000-0000-0000-000000000000",
            "0011223344556677889900112233445566778899001122334455667788990011",
        ] {
            assert!(bad.parse::<EdgeCursor>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn page_bytes() {
        assert_eq!(
            json_len(&serde_json::json!({"a": [1, "b"]})),
            r#"{"a":[1,"b"]}"#.len()
        );
        let node = Node {
            id: Uuid::nil(),
            kind: NodeKind::Document,
            key: "abc".into(),
            props: serde_json::json!({"x": 1}),
        };
        assert_eq!(node_page_bytes(&node), 3 + 7);
        let page = vec![node.clone(), node];
        assert!(page_full(&page, 2, usize::MAX, node_page_bytes));
        assert!(!page_full(&page, 3, 21, node_page_bytes));
        assert!(page_full(&page, 3, 20, node_page_bytes));
    }

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
    fn push_keys_have_no_bucket() {
        let k = SourceKey::push("0190");
        assert_eq!(k.endpoint(), "push:0190");
        assert!(k.is_push() && k.bucket().is_empty() && k.prefix().is_empty());
        assert_eq!(SourceKey::new("push:0190", "", "").unwrap(), k);
        assert!(SourceKey::new("push:", "", "").is_err());
        assert!(SourceKey::new("push:0190", "docs", "").is_err());
        assert!(SourceKey::new("push:0190", "", "in/").is_err());
        let at = ObjectLocation {
            endpoint: "push:0190".into(),
            bucket: String::new(),
            key: "a/b.pdf".into(),
        };
        assert_eq!(at.checked().unwrap(), at);
        assert_eq!(at.node_key(), r#"["push:0190","a/b.pdf"]"#);
        let s3 = ObjectLocation {
            endpoint: "http://m".into(),
            bucket: "docs".into(),
            key: "a".into(),
        };
        assert_eq!(s3.node_key(), r#"["http://m","docs","a"]"#);
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
