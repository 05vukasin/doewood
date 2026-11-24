//! Companies House answers, parsed into types.
//!
//! The JSON is read leniently (every field optional, unknown fields ignored; an item that does
//! not read is skipped). **Personal data is kept minimal** (ADR 0002, the privacy rule): the
//! raw shapes below declare only the fields doewood keeps, so a natural person's date of birth,
//! residential or correspondence address, occupation, name parts and officer id are dropped
//! when the answer is read, and are never cached (officer, PSC and filing pages are cached as
//! these shapes re-serialized, not as the raw body).

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::Date;

use crate::parse::{date, text};
use crate::trust::TrustTier;

/// A Companies House company number: eight characters, either eight digits or a two-letter
/// prefix (`SC`, `NI`, `OC`, `LP`, `FC`, ...) and six digits (`R0` and six digits for some old
/// Northern Ireland companies).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompanyNumber(String);

impl CompanyNumber {
    /// `raw` (spaces dropped, case ignored), zero-padded to eight characters after its
    /// prefix: `123456` is `00123456`, `sc12345` is `SC012345`. `None` for anything else, and
    /// for all-zero numbers.
    pub fn parse(raw: &str) -> Option<Self> {
        let s: String = raw
            .chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| c.to_ascii_uppercase())
            .collect();
        if s.is_empty() || s.len() > 8 || !s.is_ascii() {
            return None;
        }
        let prefix_len = if s.starts_with("R0") {
            2
        } else {
            s.bytes().take_while(u8::is_ascii_uppercase).count()
        };
        let (prefix, digits) = s.split_at(prefix_len);
        if !matches!(prefix.len(), 0 | 2)
            || digits.is_empty()
            || !digits.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let width = 8 - prefix.len();
        if digits.len() > width || digits.bytes().all(|b| b == b'0') {
            return None;
        }
        Some(Self(format!("{prefix}{digits:0>width$}")))
    }

    /// The eight characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CompanyNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A company's status (`company_status`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// `active`.
    Active,
    /// `dissolved`.
    Dissolved,
    /// `liquidation`.
    Liquidation,
    /// `receivership`.
    Receivership,
    /// `administration`.
    Administration,
    /// `voluntary-arrangement`.
    VoluntaryArrangement,
    /// `converted-closed`.
    ConvertedClosed,
    /// `insolvency-proceedings`.
    InsolvencyProceedings,
    /// `registered` (overseas entities).
    Registered,
    /// `removed` (overseas entities).
    Removed,
    /// `open` (overseas companies).
    Open,
    /// `closed` (overseas companies).
    Closed,
    /// Another value, as sent.
    Other(String),
    /// Missing.
    Unknown,
}

impl Status {
    fn from_code(code: Option<&str>) -> Self {
        match code.map(str::trim) {
            None | Some("") => Self::Unknown,
            Some("active") => Self::Active,
            Some("dissolved") => Self::Dissolved,
            Some("liquidation") => Self::Liquidation,
            Some("receivership") => Self::Receivership,
            Some("administration") => Self::Administration,
            Some("voluntary-arrangement") => Self::VoluntaryArrangement,
            Some("converted-closed") => Self::ConvertedClosed,
            Some("insolvency-proceedings") => Self::InsolvencyProceedings,
            Some("registered") => Self::Registered,
            Some("removed") => Self::Removed,
            Some("open") => Self::Open,
            Some("closed") => Self::Closed,
            Some(other) => Self::Other(other.chars().take(100).collect()),
        }
    }

    /// Whether the company still exists in the register (active, or in an insolvency or
    /// arrangement procedure, or registered / open).
    pub fn is_live(&self) -> bool {
        !matches!(
            self,
            Self::Dissolved
                | Self::ConvertedClosed
                | Self::Removed
                | Self::Closed
                | Self::Other(_)
                | Self::Unknown
        )
    }
}

/// A company profile (`GET /company/{number}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Company {
    /// The company number.
    pub number: CompanyNumber,
    /// The registered name.
    pub name: String,
    /// The status.
    pub status: Status,
    /// `company_status_detail` (`active-proposal-to-strike-off`, `converted-to-plc`, ...).
    pub status_detail: Option<String>,
    /// `type` (`ltd`, `plc`, `llp`, `limited-partnership`, ...).
    pub company_type: Option<String>,
    /// `jurisdiction` (`england-wales`, `scotland`, `northern-ireland`, ...).
    pub jurisdiction: Option<String>,
    /// `date_of_creation`.
    pub created: Option<Date>,
    /// `date_of_cessation`.
    pub ceased: Option<Date>,
    /// Earlier names with their dates (the dated renames of ADR 0002 section 3).
    pub previous_names: Vec<PreviousName>,
    /// SIC codes.
    pub sic_codes: Vec<String>,
    /// The registered office (a company's address, not a person's).
    pub registered_office: Option<Office>,
    /// `has_insolvency_history`.
    pub has_insolvency_history: Option<bool>,
    /// Always [`TrustTier::Official`].
    pub tier: TrustTier,
}

/// An earlier name of the company.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreviousName {
    /// The name.
    pub name: String,
    /// `effective_from`.
    pub from: Option<Date>,
    /// `ceased_on`: the day of the rename to the next name.
    pub to: Option<Date>,
}

/// A company's registered office.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Office {
    /// `address_line_1`, `address_line_2`, as given.
    pub lines: Vec<String>,
    /// `locality`.
    pub locality: Option<String>,
    /// `postal_code`.
    pub postal_code: Option<String>,
    /// `country`.
    pub country: Option<String>,
}

/// A page-by-page list (officers, PSCs, filings), as far as it was read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing<T> {
    /// The items, in Companies House's order.
    pub items: Vec<T>,
    /// `total_results` of the list, when sent.
    pub total: Option<u64>,
    /// Whether reading stopped at the configured cap before the end of the list.
    pub truncated: bool,
}

/// A corporate officer's or a corporate PSC's registration: how ownership chains are followed.
/// Never set for a natural person.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Registration {
    /// `identification_type` (`uk-limited-company`, `other-corporate-body-or-firm`, ...).
    pub kind: Option<String>,
    /// `registration_number`: for a UK company, its company number.
    pub number: Option<String>,
    /// `place_registered` (a register's name).
    pub place_registered: Option<String>,
    /// `country_registered`.
    pub country: Option<String>,
    /// `legal_authority` (the law it is governed by).
    pub legal_authority: Option<String>,
    /// `legal_form`.
    pub legal_form: Option<String>,
}

/// An officer (`GET /company/{number}/officers`). For a natural person only the name, the
/// role, the dates, the nationality and the country of residence are kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Officer {
    /// The name as registered (for a person, `SURNAME, Forenames`).
    pub name: String,
    /// `officer_role` (`director`, `secretary`, `corporate-director`, `llp-member`, ...).
    pub role: Option<String>,
    /// Whether the officer is a company or firm (a `corporate-*` role, or identification
    /// given with one).
    pub corporate: bool,
    /// `appointed_on`.
    pub appointed: Option<Date>,
    /// `resigned_on`.
    pub resigned: Option<Date>,
    /// `nationality` (natural persons).
    pub nationality: Option<String>,
    /// `country_of_residence` (natural persons).
    pub country_of_residence: Option<String>,
    /// A corporate officer's registration.
    pub registration: Option<Registration>,
}

/// What kind of person with significant control (`kind`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PscKind {
    /// `individual-person-with-significant-control` (or an overseas entity's
    /// `individual-beneficial-owner`): a natural person.
    Individual,
    /// `corporate-entity-person-with-significant-control` (or `corporate-entity-beneficial-
    /// owner`): a company, with its registration.
    CorporateEntity,
    /// `legal-person-person-with-significant-control` (or `legal-person-beneficial-owner`): a
    /// body such as a government or a public authority.
    LegalPerson,
    /// `super-secure-person-with-significant-control` (or `super-secure-beneficial-owner`):
    /// the details are suppressed by Companies House; no name.
    SuperSecure,
    /// Another kind, as sent.
    Other(String),
}

impl PscKind {
    fn from_code(code: &str) -> Self {
        let c = code.trim();
        if c.starts_with("individual-") {
            Self::Individual
        } else if c.starts_with("corporate-entity-") {
            Self::CorporateEntity
        } else if c.starts_with("legal-person-") {
            Self::LegalPerson
        } else if c.starts_with("super-secure-") {
            Self::SuperSecure
        } else {
            Self::Other(c.chars().take(100).collect())
        }
    }

    /// Whether the PSC is a natural person (an individual, or a suppressed super-secure one).
    pub fn is_natural_person(&self) -> bool {
        matches!(self, Self::Individual | Self::SuperSecure)
    }
}

/// A person with significant control (`GET /company/{number}/persons-with-significant-
/// control`). For a natural person only the name, the dates, the nationality, the country of
/// residence and the natures of control are kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Psc {
    /// The kind.
    pub kind: PscKind,
    /// Whether it is an overseas entity's registrable beneficial owner (`*-beneficial-owner`).
    pub beneficial_owner: bool,
    /// The name (none for a super-secure PSC).
    pub name: Option<String>,
    /// `notified_on`.
    pub notified: Option<Date>,
    /// `ceased_on`.
    pub ceased: Option<Date>,
    /// `nationality` (individuals).
    pub nationality: Option<String>,
    /// `country_of_residence` (individuals).
    pub country_of_residence: Option<String>,
    /// `natures_of_control` (`ownership-of-shares-75-to-100-percent`, ...).
    pub natures_of_control: Vec<String>,
    /// A corporate entity's or legal person's registration (never set for an individual).
    pub registration: Option<Registration>,
}

/// Why a filing is kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilingEvent {
    /// A change of name (category `change-of-name`).
    NameChange,
    /// A merger (a `*merger*` description, a cross-border merger form `CB..`, or a legacy
    /// filing whose text mentions a merger). Best-effort.
    Merger,
}

/// A filing of the filing history that is a name change or merger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filing {
    /// Why it is kept.
    pub event: FilingEvent,
    /// `transaction_id`.
    pub transaction_id: Option<String>,
    /// `category`.
    pub category: Option<String>,
    /// `type` (the form: `NM01`, `CERTNM`, ...).
    pub form: Option<String>,
    /// `description` (an enumeration key such as `change-of-name-by-resolution`).
    pub description: Option<String>,
    /// The free text of a legacy filing (`description_values.description`), when the filing
    /// is one.
    pub text: Option<String>,
    /// `date`: when it was filed.
    pub date: Option<Date>,
    /// `action_date`: when it took effect, when sent.
    pub action_date: Option<Date>,
}

/// A name search hit (`GET /search/companies`): a candidate only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    /// The company number.
    pub number: CompanyNumber,
    /// `title`: the name.
    pub name: String,
    /// The status.
    pub status: Status,
    /// `company_type`.
    pub company_type: Option<String>,
    /// `date_of_creation`.
    pub created: Option<Date>,
    /// `date_of_cessation`.
    pub ceased: Option<Date>,
    /// `address_snippet`: the registered office in one line.
    pub address: Option<String>,
    /// Always [`TrustTier::Official`].
    pub tier: TrustTier,
}

// ---- The raw shapes. ----

#[derive(Deserialize)]
struct RawCompany {
    company_number: Option<String>,
    company_name: Option<String>,
    company_status: Option<String>,
    company_status_detail: Option<String>,
    #[serde(rename = "type")]
    company_type: Option<String>,
    jurisdiction: Option<String>,
    date_of_creation: Option<String>,
    date_of_cessation: Option<String>,
    #[serde(default)]
    previous_company_names: Vec<Value>,
    #[serde(default)]
    sic_codes: Vec<Value>,
    registered_office_address: Option<Value>,
    has_insolvency_history: Option<bool>,
}

#[derive(Deserialize)]
struct RawPreviousName {
    name: Option<String>,
    effective_from: Option<String>,
    ceased_on: Option<String>,
}

#[derive(Deserialize)]
struct RawOffice {
    address_line_1: Option<String>,
    address_line_2: Option<String>,
    locality: Option<String>,
    postal_code: Option<String>,
    country: Option<String>,
}

/// A raw list page.
#[derive(Deserialize)]
struct RawPage {
    #[serde(default)]
    items: Vec<Value>,
    total_results: Option<u64>,
}

/// A list page as cached: the kept fields of the kept items, the list's total, and how many
/// raw items the page had (for paging).
#[derive(Serialize, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub(super) struct Stored<T> {
    pub items: Vec<T>,
    pub total: Option<u64>,
    pub consumed: usize,
}

#[derive(Serialize, Deserialize, Default)]
pub(super) struct KeptRegistration {
    #[serde(skip_serializing_if = "Option::is_none")]
    identification_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    registration_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    place_registered: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_registered: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    legal_authority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    legal_form: Option<String>,
}

/// The kept fields of an officer. No `date_of_birth`, `address`, `occupation`,
/// `name_elements` or `links`.
#[derive(Serialize, Deserialize)]
pub(super) struct KeptOfficer {
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    officer_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    appointed_on: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resigned_on: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nationality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_of_residence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identification: Option<KeptRegistration>,
}

/// The kept fields of a PSC. No `date_of_birth`, `address`, `name_elements`,
/// `principal_office_address`, `etag` or `links`.
#[derive(Serialize, Deserialize)]
pub(super) struct KeptPsc {
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notified_on: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ceased_on: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nationality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_of_residence: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    natures_of_control: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identification: Option<KeptRegistration>,
}

#[derive(Serialize, Deserialize, Default)]
struct KeptDescriptionValues {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// The kept fields of a filing. Only name changes and mergers are kept; their
/// `description_values` only for a legacy filing's text.
#[derive(Serialize, Deserialize)]
pub(super) struct KeptFiling {
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    form: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description_values: Option<KeptDescriptionValues>,
    #[serde(skip_serializing_if = "Option::is_none")]
    date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_date: Option<String>,
}

#[derive(Deserialize)]
struct RawSearch {
    #[serde(default)]
    items: Vec<Value>,
}

#[derive(Deserialize)]
struct RawHit {
    company_number: Option<String>,
    title: Option<String>,
    company_status: Option<String>,
    company_type: Option<String>,
    date_of_creation: Option<String>,
    date_of_cessation: Option<String>,
    address_snippet: Option<String>,
}

// ---- Reading. ----

/// A company profile; it must be about `number` and have a name.
pub(super) fn company(body: &[u8], number: &CompanyNumber) -> Result<Company, String> {
    let raw: RawCompany = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let got = raw
        .company_number
        .as_deref()
        .and_then(CompanyNumber::parse)
        .ok_or("no valid company_number")?;
    if &got != number {
        return Err(format!("company {got} returned for {number}"));
    }
    let name = text(raw.company_name).ok_or("no company_name")?;
    let previous_names = raw
        .previous_company_names
        .into_iter()
        .filter_map(|v| serde_json::from_value::<RawPreviousName>(v).ok())
        .filter_map(|p| {
            Some(PreviousName {
                name: text(p.name)?,
                from: date(p.effective_from.as_deref()),
                to: date(p.ceased_on.as_deref()),
            })
        })
        .collect();
    let sic_codes = raw
        .sic_codes
        .into_iter()
        .filter_map(|v| match v {
            Value::String(s) => text(Some(s)),
            _ => None,
        })
        .collect();
    let registered_office = raw
        .registered_office_address
        .and_then(|v| serde_json::from_value::<RawOffice>(v).ok())
        .map(|o| Office {
            lines: [o.address_line_1, o.address_line_2]
                .into_iter()
                .filter_map(text)
                .collect(),
            locality: text(o.locality),
            postal_code: text(o.postal_code),
            country: text(o.country),
        });
    Ok(Company {
        number: got,
        name,
        status: Status::from_code(raw.company_status.as_deref()),
        status_detail: text(raw.company_status_detail),
        company_type: text(raw.company_type),
        jurisdiction: text(raw.jurisdiction),
        created: date(raw.date_of_creation.as_deref()),
        ceased: date(raw.date_of_cessation.as_deref()),
        previous_names,
        sic_codes,
        registered_office,
        has_insolvency_history: raw.has_insolvency_history,
        tier: TrustTier::Official,
    })
}

/// A raw list page, its items read as `T` (an item that does not read is skipped).
pub(super) fn page<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<Stored<T>, String> {
    let raw: RawPage = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let consumed = raw.items.len();
    let items = raw
        .items
        .into_iter()
        .filter_map(|v| serde_json::from_value::<T>(v).ok())
        .collect();
    Ok(Stored {
        items,
        total: raw.total_results,
        consumed,
    })
}

/// A cached page.
pub(super) fn stored<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<Stored<T>, String> {
    serde_json::from_slice(body).map_err(|e| e.to_string())
}

fn registration(r: KeptRegistration) -> Option<Registration> {
    let r = Registration {
        kind: text(r.identification_type),
        number: text(r.registration_number),
        place_registered: text(r.place_registered),
        country: text(r.country_registered),
        legal_authority: text(r.legal_authority),
        legal_form: text(r.legal_form),
    };
    (r != Registration::default()).then_some(r)
}

impl KeptOfficer {
    /// Whether the officer is a company or firm.
    fn corporate(&self) -> bool {
        self.officer_role
            .as_deref()
            .is_some_and(|r| r.trim().starts_with("corporate-"))
    }

    /// Drops what is not kept for a natural person (the registration). Items without a name
    /// are refused.
    pub(super) fn minimise(mut self) -> Option<Self> {
        text(self.name.clone())?;
        if !self.corporate() {
            self.identification = None;
        }
        Some(self)
    }

    pub(super) fn officer(self) -> Option<Officer> {
        let corporate = self.corporate();
        let name = text(self.name)?;
        Some(Officer {
            name,
            role: text(self.officer_role),
            corporate,
            appointed: date(self.appointed_on.as_deref()),
            resigned: date(self.resigned_on.as_deref()),
            nationality: text(self.nationality),
            country_of_residence: text(self.country_of_residence),
            registration: if corporate {
                self.identification.and_then(registration)
            } else {
                None
            },
        })
    }
}

impl KeptPsc {
    fn psc_kind(&self) -> PscKind {
        PscKind::from_code(self.kind.as_deref().unwrap_or(""))
    }

    /// Drops what is not kept for the kind: no registration for a natural person; no
    /// nationality or residence for a company or legal person; nothing but the kind, the
    /// dates and the natures of control for a super-secure PSC.
    pub(super) fn minimise(mut self) -> Option<Self> {
        self.kind
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty())?;
        match self.psc_kind() {
            PscKind::Individual => self.identification = None,
            PscKind::SuperSecure => {
                self.identification = None;
                self.name = None;
                self.nationality = None;
                self.country_of_residence = None;
            }
            PscKind::CorporateEntity | PscKind::LegalPerson => {
                self.nationality = None;
                self.country_of_residence = None;
            }
            PscKind::Other(_) => {
                self.identification = None;
                self.nationality = None;
                self.country_of_residence = None;
            }
        }
        Some(self)
    }

    pub(super) fn psc(self) -> Psc {
        let kind = self.psc_kind();
        let beneficial_owner = self
            .kind
            .as_deref()
            .is_some_and(|k| k.trim().ends_with("-beneficial-owner"));
        let corporate = matches!(kind, PscKind::CorporateEntity | PscKind::LegalPerson);
        let person = kind == PscKind::Individual;
        Psc {
            name: if kind == PscKind::SuperSecure {
                None
            } else {
                text(self.name)
            },
            beneficial_owner,
            notified: date(self.notified_on.as_deref()),
            ceased: date(self.ceased_on.as_deref()),
            nationality: if person { text(self.nationality) } else { None },
            country_of_residence: if person {
                text(self.country_of_residence)
            } else {
                None
            },
            natures_of_control: self
                .natures_of_control
                .into_iter()
                .filter_map(|n| text(Some(n)))
                .collect(),
            registration: if corporate {
                self.identification.and_then(registration)
            } else {
                None
            },
            kind,
        }
    }
}

impl KeptFiling {
    fn legacy_text(&self) -> Option<&str> {
        self.description_values
            .as_ref()
            .and_then(|d| d.description.as_deref())
    }

    /// Why the filing is kept, if it is.
    pub(super) fn event(&self) -> Option<FilingEvent> {
        let category = self.category.as_deref().unwrap_or("").trim();
        let description = self.description.as_deref().unwrap_or("").trim();
        let form = self.form.as_deref().unwrap_or("").trim();
        if description.contains("merger")
            || (form.len() > 2
                && form.starts_with("CB")
                && form[2..].bytes().all(|b| b.is_ascii_digit()))
            || (description == "legacy"
                && self
                    .legacy_text()
                    .is_some_and(|t| t.to_ascii_lowercase().contains("merger")))
        {
            return Some(FilingEvent::Merger);
        }
        if description.contains("exemption") {
            return None;
        }
        if category == "change-of-name" || description.contains("change-of-name") {
            return Some(FilingEvent::NameChange);
        }
        None
    }

    /// Keeps a name change or merger, without the description values unless it is a legacy
    /// filing.
    pub(super) fn minimise(mut self) -> Option<Self> {
        self.event()?;
        if self.description.as_deref().map(str::trim) != Some("legacy") {
            self.description_values = None;
        }
        Some(self)
    }

    pub(super) fn filing(self) -> Option<Filing> {
        let event = self.event()?;
        let text_value = self
            .description_values
            .and_then(|d| d.description)
            .and_then(|t| text(Some(t)));
        Some(Filing {
            event,
            transaction_id: text(self.transaction_id),
            category: text(self.category),
            form: text(self.form),
            description: text(self.description),
            text: text_value,
            date: date(self.date.as_deref()),
            action_date: date(self.action_date.as_deref()),
        })
    }
}

/// The search hits with a valid company number and a name.
pub(super) fn search(body: &[u8]) -> Result<Vec<SearchHit>, String> {
    let raw: RawSearch = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    Ok(raw
        .items
        .into_iter()
        .filter_map(|v| serde_json::from_value::<RawHit>(v).ok())
        .filter_map(|h| {
            Some(SearchHit {
                number: CompanyNumber::parse(h.company_number.as_deref()?)?,
                name: text(h.title)?,
                status: Status::from_code(h.company_status.as_deref()),
                company_type: text(h.company_type),
                created: date(h.date_of_creation.as_deref()),
                ceased: date(h.date_of_cessation.as_deref()),
                address: text(h.address_snippet),
                tier: TrustTier::Official,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn company_numbers() {
        let n = |s: &str| CompanyNumber::parse(s).map(|c| c.as_str().to_owned());
        assert_eq!(n("99000001").as_deref(), Some("99000001"));
        assert_eq!(n(" 990 001 ").as_deref(), Some("00990001"));
        assert_eq!(n("sc990001").as_deref(), Some("SC990001"));
        assert_eq!(n("SC99001").as_deref(), Some("SC099001"));
        assert_eq!(n("OC990001").as_deref(), Some("OC990001"));
        assert_eq!(n("R0990001").as_deref(), Some("R0990001"));
        assert_eq!(n("00000000"), None);
        assert_eq!(n("SC000000"), None);
        assert_eq!(n("990000011"), None);
        assert_eq!(n("S9900001"), None);
        assert_eq!(n("ABC90001"), None);
        assert_eq!(n("SC"), None);
        assert_eq!(n("9900/001"), None);
        assert_eq!(n("../admin"), None);
        assert_eq!(n("9900000é"), None);
        assert_eq!(n(""), None);
    }

    #[test]
    fn filing_events() {
        let f = |category: &str, form: &str, description: &str, legacy: Option<&str>| {
            KeptFiling {
                transaction_id: None,
                category: Some(category.to_owned()),
                form: Some(form.to_owned()),
                description: Some(description.to_owned()),
                description_values: Some(KeptDescriptionValues {
                    description: legacy.map(str::to_owned),
                }),
                date: None,
                action_date: None,
            }
            .event()
        };
        assert_eq!(
            f(
                "change-of-name",
                "CERTNM",
                "certificate-change-of-name-company",
                None
            ),
            Some(FilingEvent::NameChange)
        );
        assert_eq!(
            f("resolution", "RES15", "resolution-change-of-name", None),
            Some(FilingEvent::NameChange)
        );
        assert_eq!(
            f("change-of-name", "NE01", "change-of-name-exemption", None),
            None
        );
        assert_eq!(
            f(
                "incorporation",
                "SE",
                "formation-by-merger-of-societas-europaea",
                None
            ),
            Some(FilingEvent::Merger)
        );
        assert_eq!(
            f("other", "CB01", "legacy", None),
            Some(FilingEvent::Merger)
        );
        assert_eq!(f("other", "CBX", "legacy", None), None);
        assert_eq!(
            f(
                "other",
                "MISC",
                "legacy",
                Some("Notice of cross border MERGER")
            ),
            Some(FilingEvent::Merger)
        );
        assert_eq!(
            f("accounts", "AA", "accounts-with-accounts-type-full", None),
            None
        );
    }
}
