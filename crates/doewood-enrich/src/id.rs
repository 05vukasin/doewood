//! Registry identifiers and their write guards (ADR 0002 section 1.5): each kind is checked
//! with [`crate::check`] before use, and placeholders and label text are refused.

use crate::check::{self, VatCheck};

/// A registry identifier kind, strongest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IdKind {
    /// LEI (ISO 17442).
    Lei,
    /// EU / UK VAT number with its country prefix.
    Vat,
    /// Serbian tax number (PIB).
    RsPib,
    /// Serbian registration number (matični broj).
    RsMb,
    /// Croatian OIB.
    HrOib,
    /// DUNS number (format only).
    Duns,
    /// IBAN (an account of the company).
    Iban,
}

impl IdKind {
    /// Every kind, strongest first.
    pub const ALL: [Self; 7] = [
        Self::Lei,
        Self::Vat,
        Self::RsPib,
        Self::RsMb,
        Self::HrOib,
        Self::Duns,
        Self::Iban,
    ];

    /// The name in alias keys and props.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lei => "lei",
            Self::Vat => "vat",
            Self::RsPib => "rs_pib",
            Self::RsMb => "rs_mb",
            Self::HrOib => "hr_oib",
            Self::Duns => "duns",
            Self::Iban => "iban",
        }
    }

    /// The kind of a quail `signal.*` fact key.
    pub fn from_fact_key(key: &str) -> Option<Self> {
        Some(match key {
            "signal.lei" => Self::Lei,
            "signal.vat" => Self::Vat,
            "signal.rs_pib" => Self::RsPib,
            "signal.rs_mb" => Self::RsMb,
            "signal.hr_oib" => Self::HrOib,
            "signal.duns" => Self::Duns,
            "signal.iban" => Self::Iban,
            _ => return None,
        })
    }
}

/// A validated identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identifier {
    /// What kind.
    pub kind: IdKind,
    /// Compact and uppercase.
    pub value: String,
}

impl Identifier {
    /// Validates `raw` as a `kind`: compact form, check digits, placeholders refused.
    pub fn parse(kind: IdKind, raw: &str) -> Option<Self> {
        let value: String = raw
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .map(|c| c.to_ascii_uppercase())
            .collect();
        if value.is_empty() || placeholder(&value) {
            return None;
        }
        let ok = match kind {
            IdKind::Lei => check::lei(&value),
            IdKind::Vat => matches!(check::vat(&value), VatCheck::Valid | VatCheck::FormatOnly),
            IdKind::RsPib => check::rs_pib(&value),
            IdKind::RsMb => check::rs_mb(&value),
            IdKind::HrOib => check::hr_oib(&value),
            IdKind::Duns => check::duns(&value),
            IdKind::Iban => check::iban(&value),
        };
        ok.then_some(Self { kind, value })
    }

    /// The alias key: `id:<kind>:<value>`.
    pub fn alias(&self) -> String {
        format!("id:{}:{}", self.kind.as_str(), self.value)
    }
}

/// Placeholder digits ("000000000", "123456789") and label text are never identity.
fn placeholder(v: &str) -> bool {
    let digits: String = v.chars().filter(char::is_ascii_digit).collect();
    if digits.len() >= 6 {
        let first = digits.as_bytes()[0];
        if digits.bytes().all(|b| b == first) {
            return true;
        }
        if "1234567890123456789".contains(digits.as_str()) {
            return true;
        }
    }
    v.chars().all(|c| c.is_ascii_alphabetic())
}
