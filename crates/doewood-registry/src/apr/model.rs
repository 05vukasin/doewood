//! APR's company record: status, legal form, municipality, read from the open-data fields.

use std::fmt;

use doewood_enrich::{IdKind, Identifier, normalize};
use serde::Deserialize;
use time::Date;

use crate::parse;

/// A company of the APR business register (Регистар привредних друштава), as the open-data
/// set gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Company {
    /// The registration number (matični broj), validated.
    pub mb: Identifier,
    /// The business name as registered (`PoslovnoIme`), in the script it was registered in.
    pub name: String,
    /// The script of [`Company::name`]. The set gives one spelling only; matching keys are
    /// script-blind ([`doewood_enrich::normalize`]).
    pub script: Script,
    /// The status (`NazivStatus`).
    pub status: Status,
    /// The legal form (`NazivPravneForme`).
    pub legal_form: LegalForm,
    /// The founding date (`DatumOsnivanja`).
    pub founded: Option<Date>,
    /// The municipality of the registered seat (`SifraOpstine`, `NazivOpstine`).
    pub municipality: Option<Municipality>,
    /// The registered activity code (`SifraDelatnosti`, four digits).
    pub activity: Option<String>,
    /// The date of the register snapshot the record is from (`DatumPreseka`).
    pub as_of: Option<Date>,
}

/// The script a name is written in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Script {
    /// At least one Cyrillic letter.
    Cyrillic,
    /// No Cyrillic letter.
    Latin,
}

impl Script {
    /// The script of `s`.
    pub fn of(s: &str) -> Self {
        if s.chars().any(|c| matches!(c, '\u{0400}'..='\u{04FF}')) {
            Self::Cyrillic
        } else {
            Self::Latin
        }
    }
}

/// The municipality of a company's seat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Municipality {
    /// The municipality code (five digits), when given.
    pub code: Option<String>,
    /// The name, as given (Cyrillic in the set).
    pub name: Option<String>,
}

/// A company's status in the register.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Status {
    /// Активан.
    Active,
    /// У ликвидацији.
    InLiquidation,
    /// У принудној ликвидацији.
    InForcedLiquidation,
    /// У стечају.
    InBankruptcy,
    /// Брисан / Обрисан. The open-data set lists no deleted companies today: a deleted
    /// company is absent from it.
    Deleted,
    /// Any other value, as given.
    Other(String),
}

impl Status {
    /// The status of a `NazivStatus` value, in either script.
    pub fn parse(raw: &str) -> Self {
        match normalize(raw).as_str() {
            "aktivan" | "aktivno" => Self::Active,
            "u likvidaciji" | "likvidacija" => Self::InLiquidation,
            "u prinudnoj likvidaciji" | "prinudna likvidacija" => Self::InForcedLiquidation,
            "u stecaju" | "stecaj" => Self::InBankruptcy,
            "brisan" | "obrisan" | "brisano" | "obrisano" => Self::Deleted,
            _ => Self::Other(raw.trim().chars().take(100).collect()),
        }
    }

    /// The name in logs and props.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Active => "active",
            Self::InLiquidation => "in_liquidation",
            Self::InForcedLiquidation => "in_forced_liquidation",
            Self::InBankruptcy => "in_bankruptcy",
            Self::Deleted => "deleted",
            Self::Other(s) => s,
        }
    }

    /// Whether the company is being wound up or is gone (liquidation, bankruptcy, deleted).
    pub fn is_winding_up(&self) -> bool {
        matches!(
            self,
            Self::InLiquidation | Self::InForcedLiquidation | Self::InBankruptcy | Self::Deleted
        )
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A legal form of the Serbian Companies Act, as the register names it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LegalForm {
    /// Друштво са ограниченом одговорношћу (d.o.o.).
    LimitedLiability,
    /// Акционарско друштво (a.d.).
    JointStock,
    /// Отворено акционарско друштво (an older a.d.).
    OpenJointStock,
    /// Затворено акционарско друштво (an older a.d.).
    ClosedJointStock,
    /// Ортачко друштво (o.d.).
    Partnership,
    /// Командитно друштво (k.d.).
    LimitedPartnership,
    /// Јавно предузеће (j.p.).
    PublicEnterprise,
    /// Друштвено предузеће.
    SocialEnterprise,
    /// Задруга.
    Cooperative,
    /// Задружни савез.
    CooperativeUnion,
    /// Представништво страног привредног друштва.
    ForeignRepresentativeOffice,
    /// Огранак страног привредног друштва.
    ForeignBranch,
    /// Any other value (`Друго` too), as given.
    Other(String),
}

impl LegalForm {
    /// The legal form of a `NazivPravneForme` value, in either script.
    pub fn parse(raw: &str) -> Self {
        match normalize(raw).as_str() {
            "drustvo sa ogranicenom odgovornoscu" | "doo" => Self::LimitedLiability,
            "akcionarsko drustvo" | "ad" => Self::JointStock,
            "otvoreno akcionarsko drustvo" => Self::OpenJointStock,
            "zatvoreno akcionarsko drustvo" => Self::ClosedJointStock,
            "ortacko drustvo" | "od" => Self::Partnership,
            "komanditno drustvo" | "kd" => Self::LimitedPartnership,
            "javno preduzece" | "jp" => Self::PublicEnterprise,
            "drustveno preduzece" => Self::SocialEnterprise,
            "zadruga" => Self::Cooperative,
            "zadruzni savez" => Self::CooperativeUnion,
            "predstavnistvo stranog privrednog drustva" => Self::ForeignRepresentativeOffice,
            "ogranak stranog privrednog drustva" => Self::ForeignBranch,
            _ => Self::Other(raw.trim().chars().take(100).collect()),
        }
    }

    /// The canonical spelling in [`doewood_enrich::LEGAL_FORMS`], for the forms it has.
    pub fn canonical(&self) -> Option<&'static str> {
        Some(match self {
            Self::LimitedLiability => "d.o.o.",
            Self::JointStock | Self::OpenJointStock | Self::ClosedJointStock => "a.d.",
            Self::Partnership => "o.d.",
            Self::LimitedPartnership => "k.d.",
            Self::PublicEnterprise => "j.p.",
            _ => return None,
        })
    }
}

/// The open-data fields of one company, in the set's order. A cached record and the snapshot
/// index keep these raw strings, so a better reader applies to them too.
pub(crate) const FIELDS: [&str; 7] = [
    "PoslovnoIme",
    "SifraOpstine",
    "NazivOpstine",
    "NazivStatus",
    "DatumOsnivanja",
    "NazivPravneForme",
    "SifraDelatnosti",
];

/// One company as cached: the raw fields with its MB and the snapshot date.
#[derive(Debug, Default, Deserialize, serde::Serialize)]
#[serde(default)]
pub(crate) struct RawRecord {
    #[serde(rename = "MaticniBroj")]
    pub mb: Option<String>,
    #[serde(rename = "DatumPreseka")]
    pub as_of: Option<String>,
    #[serde(rename = "PoslovnoIme")]
    pub name: Option<String>,
    #[serde(rename = "SifraOpstine")]
    pub municipality_code: Option<String>,
    #[serde(rename = "NazivOpstine")]
    pub municipality: Option<String>,
    #[serde(rename = "NazivStatus")]
    pub status: Option<String>,
    #[serde(rename = "DatumOsnivanja")]
    pub founded: Option<String>,
    #[serde(rename = "NazivPravneForme")]
    pub legal_form: Option<String>,
    #[serde(rename = "SifraDelatnosti")]
    pub activity: Option<String>,
}

/// The company in a cached record, which must be about `mb`.
pub(crate) fn company(body: &[u8], mb: &Identifier) -> Result<Company, String> {
    let raw: RawRecord = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let got = raw
        .mb
        .as_deref()
        .and_then(|m| Identifier::parse(IdKind::RsMb, m))
        .ok_or("no valid MB in the record")?;
    if got != *mb {
        return Err(format!("MB {} returned for {}", got.value, mb.value));
    }
    from_raw(raw, got)
}

/// The company of a raw record whose MB is `mb`.
pub(crate) fn from_raw(raw: RawRecord, mb: Identifier) -> Result<Company, String> {
    let name = parse::text(raw.name).ok_or("a company without a name")?;
    let code = digits(raw.municipality_code, 5);
    let municipality_name = parse::text(raw.municipality);
    let municipality = (code.is_some() || municipality_name.is_some()).then_some(Municipality {
        code,
        name: municipality_name,
    });
    Ok(Company {
        mb,
        script: Script::of(&name),
        name,
        status: parse::text(raw.status)
            .map_or_else(|| Status::Other(String::new()), |s| Status::parse(&s)),
        legal_form: parse::text(raw.legal_form)
            .map_or_else(|| LegalForm::Other(String::new()), |s| LegalForm::parse(&s)),
        founded: parse::date(raw.founded.as_deref()),
        municipality,
        activity: digits(raw.activity, 4),
        as_of: parse::date(raw.as_of.as_deref()),
    })
}

/// `s` when it is exactly `n` ASCII digits (after trimming).
fn digits(s: Option<String>, n: usize) -> Option<String> {
    let s = parse::text(s)?;
    (s.len() == n && s.bytes().all(|b| b.is_ascii_digit())).then_some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_in_both_scripts() {
        for (raw, want) in [
            ("Активан", Status::Active),
            ("aktivan", Status::Active),
            ("У ликвидацији", Status::InLiquidation),
            ("U likvidaciji", Status::InLiquidation),
            ("У принудној ликвидацији", Status::InForcedLiquidation),
            ("У стечају", Status::InBankruptcy),
            ("U stečaju", Status::InBankruptcy),
            ("Обрисан", Status::Deleted),
            ("Брисан", Status::Deleted),
            ("Мировање", Status::Other("Мировање".to_owned())),
        ] {
            assert_eq!(Status::parse(raw), want, "{raw}");
        }
        assert!(Status::InBankruptcy.is_winding_up());
        assert!(!Status::Active.is_winding_up());
    }

    #[test]
    fn legal_forms_map_to_enrich_canonicals() {
        assert_eq!(
            LegalForm::parse("Друштво са ограниченом одговорношћу"),
            LegalForm::LimitedLiability
        );
        assert_eq!(
            LegalForm::parse("Društvo sa ograničenom odgovornošću"),
            LegalForm::LimitedLiability
        );
        assert_eq!(
            LegalForm::parse("Јавно предузеће"),
            LegalForm::PublicEnterprise
        );
        assert_eq!(
            LegalForm::parse("Огранак страног привредног друштва"),
            LegalForm::ForeignBranch
        );
        assert_eq!(
            LegalForm::parse("Друго"),
            LegalForm::Other("Друго".to_owned())
        );
        let known: Vec<&str> = doewood_enrich::LEGAL_FORMS
            .iter()
            .map(|f| f.canonical)
            .collect();
        for f in [
            LegalForm::LimitedLiability,
            LegalForm::JointStock,
            LegalForm::OpenJointStock,
            LegalForm::ClosedJointStock,
            LegalForm::Partnership,
            LegalForm::LimitedPartnership,
            LegalForm::PublicEnterprise,
        ] {
            let c = f.canonical().expect("canonical");
            assert!(known.contains(&c), "{c}");
        }
        assert_eq!(LegalForm::Cooperative.canonical(), None);
    }

    #[test]
    fn scripts() {
        assert_eq!(Script::of("Измишљена Пекара д.о.о."), Script::Cyrillic);
        assert_eq!(Script::of("Izmišljena Pekara d.o.o."), Script::Latin);
        assert_eq!(Script::of("ABC 2 doo"), Script::Latin);
    }

    #[test]
    fn digit_fields() {
        assert_eq!(
            digits(Some(" 70114 ".to_owned()), 5).as_deref(),
            Some("70114")
        );
        assert_eq!(digits(Some("7011".to_owned()), 5), None);
        assert_eq!(digits(Some("70a14".to_owned()), 5), None);
        assert_eq!(digits(None, 5), None);
    }
}
