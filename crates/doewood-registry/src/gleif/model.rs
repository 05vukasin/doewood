//! GLEIF answers, parsed into types. The JSON:API documents of `api.gleif.org/api/v1` are read
//! leniently (every field optional, unknown fields ignored), then checked: an LEI must pass
//! ISO 17442 and a record must have a legal name, otherwise it is refused.

use doewood_enrich::{IdKind, Identifier};
use serde::Deserialize;
use time::Date;

use crate::parse::{date, text};

/// An LEI record (Level 1), plus the successor entities and legal-entity events it carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeiRecord {
    /// The LEI, validated.
    pub lei: Identifier,
    /// The legal name.
    pub legal_name: String,
    /// The language tag of the legal name, as GLEIF gives it.
    pub legal_name_language: Option<String>,
    /// Other names (previous legal names, trading names, transliterations).
    pub other_names: Vec<OtherName>,
    /// ISO 3166-1 alpha-2 country of the legal address.
    pub legal_country: Option<String>,
    /// City of the legal address.
    pub legal_city: Option<String>,
    /// The legal jurisdiction (`DE`, `US-DE`).
    pub jurisdiction: Option<String>,
    /// The registration authority code (`RA000...`), or GLEIF's `other` text.
    pub registered_at: Option<String>,
    /// The entity's id in that registry (`registeredAs`), for example a company number.
    pub registered_as: Option<String>,
    /// The ISO 20275 legal form code (ELF), or GLEIF's `other` text.
    pub legal_form: Option<String>,
    /// `GENERAL`, `BRANCH`, `FUND`, `SOLE_PROPRIETOR`, ...
    pub category: Option<String>,
    /// Whether the entity is active.
    pub entity_status: EntityStatus,
    /// The LEI registration status (`ISSUED`, `LAPSED`, `MERGED`, `RETIRED`, `ANNULLED`, ...).
    pub registration_status: Option<String>,
    /// When the record was last updated at GLEIF.
    pub last_update: Option<Date>,
    /// When the entity ceased to exist, and why (`DISSOLVED`, `CORPORATE_ACTION`, `OTHER`).
    pub expiration: Option<Expiration>,
    /// The successor entities (after a merger or another corporate action).
    pub successors: Vec<Successor>,
    /// Legal-entity events (name changes, mergers, ...), with dates.
    pub events: Vec<EntityEvent>,
}

/// Another name of the entity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtherName {
    /// The name.
    pub name: String,
    /// GLEIF's type (`PREVIOUS_LEGAL_NAME`, `TRADING_OR_OPERATING_NAME`, ...).
    pub kind: Option<String>,
}

/// Active or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityStatus {
    /// `ACTIVE`.
    Active,
    /// `INACTIVE`: the entity ceased to exist.
    Inactive,
    /// Missing or another value.
    Unknown,
}

/// When and why an entity ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expiration {
    /// The date.
    pub date: Option<Date>,
    /// `DISSOLVED`, `CORPORATE_ACTION` or `OTHER`.
    pub reason: Option<String>,
}

/// A successor entity: an LEI (validated) or only a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Successor {
    /// Its LEI, when GLEIF gives a valid one.
    pub lei: Option<Identifier>,
    /// Its name, when GLEIF gives one.
    pub name: Option<String>,
}

/// A legal-entity event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityEvent {
    /// What happened.
    pub kind: EventKind,
    /// GLEIF's code, as sent.
    pub code: String,
    /// `PENDING`, `COMPLETED`, `WITHDRAWN_CANCELLED`, ...
    pub status: Option<String>,
    /// When it took effect.
    pub effective: Option<Date>,
    /// When it was recorded at GLEIF.
    pub recorded: Option<Date>,
}

/// The legal-entity event types doewood cares about; the rest are [`EventKind::Other`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// `CHANGE_LEGAL_NAME`: a rename (the LEI stays).
    LegalNameChange,
    /// `CHANGE_LEGAL_FORM`.
    LegalFormChange,
    /// `MERGERS_AND_ACQUISITIONS`.
    MergerOrAcquisition,
    /// `ABSORPTION`.
    Absorption,
    /// `DEMERGER`.
    Demerger,
    /// `SPIN_OFF`.
    SpinOff,
    /// `BREAKUP`.
    Breakup,
    /// `TRANSFORMATION_*`.
    Transformation,
    /// `LIQUIDATION`, `BANKRUPTCY`, `INSOLVENCY`, `DISSOLUTION`.
    Closure,
    /// Anything else.
    Other,
}

impl EventKind {
    fn from_code(code: &str) -> Self {
        match code {
            "CHANGE_LEGAL_NAME" => Self::LegalNameChange,
            "CHANGE_LEGAL_FORM" => Self::LegalFormChange,
            "MERGERS_AND_ACQUISITIONS" => Self::MergerOrAcquisition,
            "ABSORPTION" => Self::Absorption,
            "DEMERGER" => Self::Demerger,
            "SPIN_OFF" => Self::SpinOff,
            "BREAKUP" => Self::Breakup,
            "LIQUIDATION" | "BANKRUPTCY" | "INSOLVENCY" | "DISSOLUTION" => Self::Closure,
            c if c.starts_with("TRANSFORMATION_") => Self::Transformation,
            _ => Self::Other,
        }
    }

    /// Whether this event moves the entity into a successor (merger, absorption, breakup,
    /// transformation).
    pub fn has_successor(&self) -> bool {
        matches!(
            self,
            Self::MergerOrAcquisition | Self::Absorption | Self::Breakup | Self::Transformation
        )
    }
}

/// A succession with its date: "this entity went into `successor` on `date`" (ADR 0002
/// section 3: `merged_into {date}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Succession {
    /// The successor.
    pub successor: Successor,
    /// The event behind it, when the record has one.
    pub event: Option<EventKind>,
    /// When: the effective date of that event, else the expiration date.
    pub date: Option<Date>,
}

impl LeiRecord {
    /// The successors with their dates. The date is the effective date of the latest
    /// completed (or undated-status) successor event, else the expiration date.
    pub fn successions(&self) -> Vec<Succession> {
        let event = self
            .events
            .iter()
            .filter(|e| e.kind.has_successor())
            .filter(|e| e.status.as_deref().is_none_or(|s| s == "COMPLETED"))
            .max_by_key(|e| e.effective);
        let date = event
            .and_then(|e| e.effective)
            .or_else(|| self.expiration.as_ref().and_then(|x| x.date));
        self.successors
            .iter()
            .map(|s| Succession {
                successor: s.clone(),
                event: event.map(|e| e.kind.clone()),
                date,
            })
            .collect()
    }

    /// The renames (`CHANGE_LEGAL_NAME` events) with their effective dates.
    pub fn renames(&self) -> impl Iterator<Item = &EntityEvent> {
        self.events
            .iter()
            .filter(|e| e.kind == EventKind::LegalNameChange)
    }
}

/// Which parent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ParentKind {
    /// The direct accounting consolidation parent.
    Direct,
    /// The ultimate accounting consolidation parent.
    Ultimate,
}

impl ParentKind {
    /// The path segment: `direct` or `ultimate`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Ultimate => "ultimate",
        }
    }

    fn relationship_type(self) -> &'static str {
        match self {
            Self::Direct => "IS_DIRECTLY_CONSOLIDATED_BY",
            Self::Ultimate => "IS_ULTIMATELY_CONSOLIDATED_BY",
        }
    }
}

/// What GLEIF says about a parent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parent {
    /// A reported relationship to a parent with an LEI.
    Reported(ParentRelationship),
    /// The entity reported why no parent is given.
    Exception(ReportingException),
    /// Neither a relationship nor an exception is on record.
    NotReported,
}

/// A Level 2 relationship record: `child` is consolidated by `parent`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentRelationship {
    /// Direct or ultimate.
    pub kind: ParentKind,
    /// The reporting entity (the start node).
    pub child: Identifier,
    /// The parent (the end node).
    pub parent: Identifier,
    /// Whether the relationship is current.
    pub status: RelationshipStatus,
    /// The start of the relationship period.
    pub valid_from: Option<Date>,
    /// The end of the relationship period.
    pub valid_to: Option<Date>,
    /// The relationship's registration status (`PUBLISHED`, `LAPSED`, ...).
    pub registration_status: Option<String>,
}

/// Active or not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelationshipStatus {
    /// `ACTIVE`.
    Active,
    /// `INACTIVE`.
    Inactive,
    /// Missing or another value.
    Unknown,
}

/// A reporting exception: why an entity reports no parent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportingException {
    /// Direct or ultimate.
    pub kind: ParentKind,
    /// The reporting entity.
    pub lei: Identifier,
    /// The reason.
    pub reason: ExceptionReason,
    /// GLEIF's reason code, as sent.
    pub code: String,
}

/// The reasons GLEIF accepts for not reporting a parent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExceptionReason {
    /// `NATURAL_PERSONS`: controlled by natural persons, no parent entity.
    NaturalPersons,
    /// `NON_CONSOLIDATING`: the parent does not consolidate it.
    NonConsolidating,
    /// `NO_KNOWN_PERSON`: no known controlling person.
    NoKnownPerson,
    /// `NON_PUBLIC` (and its older detailed codes: legal obstacles, consent not obtained,
    /// binding legal commitments, disclosure detrimental): the parent is not disclosed.
    NonPublic,
    /// Another code.
    Other,
}

impl ExceptionReason {
    fn from_code(code: &str) -> Self {
        match code {
            "NATURAL_PERSONS" => Self::NaturalPersons,
            "NON_CONSOLIDATING" => Self::NonConsolidating,
            "NO_KNOWN_PERSON" => Self::NoKnownPerson,
            "NON_PUBLIC"
            | "BINDING_LEGAL_COMMITMENTS"
            | "LEGAL_OBSTACLES"
            | "DISCLOSURE_DETRIMENTAL"
            | "DETRIMENT_NOT_EXCLUDED"
            | "CONSENT_NOT_OBTAINED" => Self::NonPublic,
            _ => Self::Other,
        }
    }
}

// The JSON:API documents, read leniently.

#[derive(Deserialize)]
struct One<T> {
    data: Option<T>,
}

#[derive(Deserialize)]
struct Many<T> {
    data: Option<Vec<T>>,
}

#[derive(Deserialize)]
struct Resource<A> {
    id: Option<String>,
    attributes: Option<A>,
}

#[derive(Deserialize)]
struct RawRecord {
    lei: Option<String>,
    entity: Option<RawEntity>,
    registration: Option<RawRegistration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawEntity {
    legal_name: Option<RawName>,
    other_names: Option<Vec<RawName>>,
    legal_address: Option<RawAddress>,
    registered_at: Option<RawCode>,
    registered_as: Option<String>,
    jurisdiction: Option<String>,
    category: Option<String>,
    legal_form: Option<RawCode>,
    status: Option<String>,
    expiration: Option<RawExpiration>,
    successor_entity: Option<RawSuccessor>,
    successor_entities: Option<Vec<RawSuccessor>>,
    event_groups: Option<Vec<RawEventGroup>>,
}

#[derive(Deserialize)]
struct RawName {
    name: Option<String>,
    language: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct RawAddress {
    country: Option<String>,
    city: Option<String>,
}

#[derive(Deserialize)]
struct RawCode {
    id: Option<String>,
    other: Option<String>,
}

#[derive(Deserialize)]
struct RawExpiration {
    date: Option<String>,
    reason: Option<String>,
}

#[derive(Deserialize)]
struct RawSuccessor {
    lei: Option<String>,
    name: Option<String>,
}

#[derive(Deserialize)]
struct RawEventGroup {
    events: Option<Vec<RawEvent>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawEvent {
    #[serde(rename = "type")]
    kind: Option<String>,
    status: Option<String>,
    effective_date: Option<String>,
    recorded_date: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawRegistration {
    status: Option<String>,
    last_update_date: Option<String>,
}

#[derive(Deserialize)]
struct RawRelationshipRecord {
    relationship: Option<RawRelationship>,
    registration: Option<RawRegistration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawRelationship {
    start_node: Option<RawNode>,
    end_node: Option<RawNode>,
    #[serde(rename = "type")]
    kind: Option<String>,
    status: Option<String>,
    periods: Option<Vec<RawPeriod>>,
}

#[derive(Deserialize)]
struct RawNode {
    id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPeriod {
    start_date: Option<String>,
    end_date: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct RawException {
    lei: Option<String>,
    reason: Option<String>,
}

/// Parses the document of `GET /lei-records/{lei}`.
pub(crate) fn record(body: &[u8]) -> Result<Option<LeiRecord>, String> {
    let doc: One<Resource<RawRecord>> = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    doc.data.map(lei_record).transpose()
}

/// Parses the document of `GET /lei-records?filter...`; records that fail the checks are
/// skipped (and counted).
pub(crate) fn records(body: &[u8]) -> Result<(Vec<LeiRecord>, usize), String> {
    let doc: Many<Resource<RawRecord>> = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let mut skipped = 0;
    let mut out = Vec::new();
    for r in doc.data.unwrap_or_default() {
        match lei_record(r) {
            Ok(r) => out.push(r),
            Err(_) => skipped += 1,
        }
    }
    Ok((out, skipped))
}

/// Parses the document of `GET /lei-records/{lei}/{kind}-parent-relationship`.
pub(crate) fn relationship(
    body: &[u8],
    kind: ParentKind,
    child: &Identifier,
) -> Result<Option<ParentRelationship>, String> {
    let doc: One<Resource<RawRelationshipRecord>> =
        serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let Some(res) = doc.data else {
        return Ok(None);
    };
    let attrs = res
        .attributes
        .ok_or("relationship record without attributes")?;
    let rel = attrs
        .relationship
        .ok_or("relationship record without a relationship")?;
    let start = lei(rel.start_node.and_then(|n| n.id).as_deref())
        .ok_or("relationship start node is not a valid LEI")?;
    let end = lei(rel.end_node.and_then(|n| n.id).as_deref())
        .ok_or("relationship end node is not a valid LEI")?;
    if &start != child {
        return Err(format!(
            "relationship of {} returned for {}",
            start.value, child.value
        ));
    }
    if start == end {
        return Err("an entity is not its own parent".to_owned());
    }
    match rel.kind.as_deref() {
        Some(t) if t == kind.relationship_type() => {}
        other => return Err(format!("unexpected relationship type {other:?}")),
    }
    let periods = rel.periods.unwrap_or_default();
    let period = periods
        .iter()
        .find(|p| p.kind.as_deref() == Some("RELATIONSHIP_PERIOD"))
        .or_else(|| periods.first());
    Ok(Some(ParentRelationship {
        kind,
        child: start,
        parent: end,
        status: match rel.status.as_deref() {
            Some("ACTIVE") => RelationshipStatus::Active,
            Some("INACTIVE") => RelationshipStatus::Inactive,
            _ => RelationshipStatus::Unknown,
        },
        valid_from: period.and_then(|p| date(p.start_date.as_deref())),
        valid_to: period.and_then(|p| date(p.end_date.as_deref())),
        registration_status: attrs.registration.and_then(|r| text(r.status)),
    }))
}

/// Parses the document of `GET /lei-records/{lei}/{kind}-parent-reporting-exception`.
pub(crate) fn exception(
    body: &[u8],
    kind: ParentKind,
    child: &Identifier,
) -> Result<Option<ReportingException>, String> {
    let doc: One<Resource<RawException>> =
        serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let Some(res) = doc.data else {
        return Ok(None);
    };
    let attrs = res
        .attributes
        .ok_or("reporting exception without attributes")?;
    let reporting = match attrs.lei.as_deref() {
        Some(l) => lei(Some(l)).ok_or("reporting exception LEI is not valid")?,
        None => child.clone(),
    };
    if &reporting != child {
        return Err(format!(
            "reporting exception of {} returned for {}",
            reporting.value, child.value
        ));
    }
    let code = text(attrs.reason).ok_or("reporting exception without a reason")?;
    Ok(Some(ReportingException {
        kind,
        lei: reporting,
        reason: ExceptionReason::from_code(&code),
        code,
    }))
}

fn lei_record(res: Resource<RawRecord>) -> Result<LeiRecord, String> {
    let attrs = res.attributes.ok_or("record without attributes")?;
    let raw = attrs.lei.or(res.id);
    let lei_id = lei(raw.as_deref()).ok_or_else(|| format!("not a valid LEI: {raw:?}"))?;
    let entity = attrs.entity.ok_or("record without an entity")?;
    let legal = entity.legal_name.ok_or("record without a legal name")?;
    let legal_name = text(legal.name).ok_or("record without a legal name")?;
    let address = entity.legal_address;
    let mut successors: Vec<Successor> = entity
        .successor_entities
        .unwrap_or_default()
        .into_iter()
        .chain(entity.successor_entity)
        .filter_map(successor)
        .collect();
    let mut seen = Vec::new();
    successors.retain(|s| {
        let fresh = !seen.contains(s);
        if fresh {
            seen.push(s.clone());
        }
        fresh
    });
    let registration = entity_registration(attrs.registration);
    Ok(LeiRecord {
        lei: lei_id,
        legal_name,
        legal_name_language: text(legal.language),
        other_names: entity
            .other_names
            .unwrap_or_default()
            .into_iter()
            .filter_map(|n| {
                Some(OtherName {
                    name: text(n.name)?,
                    kind: text(n.kind),
                })
            })
            .collect(),
        legal_country: address.as_ref().and_then(|a| country(a.country.as_deref())),
        legal_city: address.and_then(|a| text(a.city)),
        jurisdiction: text(entity.jurisdiction),
        registered_at: entity.registered_at.and_then(code),
        registered_as: text(entity.registered_as),
        legal_form: entity.legal_form.and_then(code),
        category: text(entity.category),
        entity_status: match entity.status.as_deref() {
            Some("ACTIVE") => EntityStatus::Active,
            Some("INACTIVE") => EntityStatus::Inactive,
            _ => EntityStatus::Unknown,
        },
        registration_status: registration.0,
        last_update: registration.1,
        expiration: entity.expiration.and_then(|x| {
            let e = Expiration {
                date: date(x.date.as_deref()),
                reason: text(x.reason),
            };
            (e.date.is_some() || e.reason.is_some()).then_some(e)
        }),
        successors,
        events: entity
            .event_groups
            .unwrap_or_default()
            .into_iter()
            .flat_map(|g| g.events.unwrap_or_default())
            .filter_map(|e| {
                let code = text(e.kind)?;
                Some(EntityEvent {
                    kind: EventKind::from_code(&code),
                    code,
                    status: text(e.status),
                    effective: date(e.effective_date.as_deref()),
                    recorded: date(e.recorded_date.as_deref()),
                })
            })
            .collect(),
    })
}

fn entity_registration(r: Option<RawRegistration>) -> (Option<String>, Option<Date>) {
    r.map_or((None, None), |r| {
        (text(r.status), date(r.last_update_date.as_deref()))
    })
}

fn successor(s: RawSuccessor) -> Option<Successor> {
    let s = Successor {
        lei: lei(s.lei.as_deref()),
        name: text(s.name),
    };
    (s.lei.is_some() || s.name.is_some()).then_some(s)
}

fn code(c: RawCode) -> Option<String> {
    text(c.id).or_else(|| text(c.other))
}

/// A valid LEI, or nothing.
fn lei(raw: Option<&str>) -> Option<Identifier> {
    Identifier::parse(IdKind::Lei, raw?)
}

/// Two ASCII letters, uppercased.
fn country(s: Option<&str>) -> Option<String> {
    let s = s?.trim();
    (s.len() == 2 && s.bytes().all(|b| b.is_ascii_alphabetic())).then(|| s.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_codes() {
        assert_eq!(
            EventKind::from_code("CHANGE_LEGAL_NAME"),
            EventKind::LegalNameChange
        );
        assert_eq!(
            EventKind::from_code("TRANSFORMATION_BRANCH_TO_SUBSIDIARY"),
            EventKind::Transformation
        );
        assert_eq!(EventKind::from_code("LIQUIDATION"), EventKind::Closure);
        assert_eq!(EventKind::from_code("SOMETHING_NEW"), EventKind::Other);
        assert!(EventKind::MergerOrAcquisition.has_successor());
        assert!(!EventKind::LegalNameChange.has_successor());
    }

    #[test]
    fn exception_codes() {
        assert_eq!(
            ExceptionReason::from_code("NATURAL_PERSONS"),
            ExceptionReason::NaturalPersons
        );
        assert_eq!(
            ExceptionReason::from_code("CONSENT_NOT_OBTAINED"),
            ExceptionReason::NonPublic
        );
        assert_eq!(
            ExceptionReason::from_code("NEW_CODE"),
            ExceptionReason::Other
        );
    }

    #[test]
    fn country_and_text() {
        assert_eq!(country(Some("rs")), Some("RS".to_owned()));
        assert_eq!(country(Some("SRB")), None);
        assert_eq!(country(Some("1A")), None);
        assert_eq!(text(Some("  ".to_owned())), None);
        assert_eq!(text(Some("x".repeat(2000))).unwrap().len(), 1000);
    }

    #[test]
    fn hostile_shapes_are_errors_not_panics() {
        for body in [
            &b""[..],
            b"null",
            b"[]",
            b"{\"data\": 5}",
            b"{\"data\": {\"attributes\": []}}",
            b"{\"data\": {\"attributes\": {\"entity\": {\"successorEntities\": 3}}}}",
        ] {
            let _ = record(body);
            let _ = records(body);
        }
        // `data: null` is no record, not an error.
        assert_eq!(record(b"{\"data\": null}"), Ok(None));
        assert_eq!(records(b"{\"data\": null}"), Ok((Vec::new(), 0)));
    }
}
