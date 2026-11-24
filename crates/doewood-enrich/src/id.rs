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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testids::{broken, iban, lei, mb, mod_11, mod_11_10};

    fn ok(kind: IdKind, raw: &str) -> String {
        Identifier::parse(kind, raw)
            .unwrap_or_else(|| panic!("{raw} should be a valid {kind:?}"))
            .value
    }

    fn bad(kind: IdKind, raw: &str) -> bool {
        Identifier::parse(kind, raw).is_none()
    }

    #[test]
    fn every_kind_valid_and_invalid() {
        let pib = mod_11_10("10873452");
        let m = mb("2108734");
        let oib = mod_11_10("6087345219");
        let l = lei("8945002TQV7RMXEW3K");
        let account = iban("RS", "265000000837164209");
        let de = format!("DE{}", mod_11_10("31745208"));
        for (kind, valid) in [
            (IdKind::RsPib, pib.as_str()),
            (IdKind::RsMb, m.as_str()),
            (IdKind::HrOib, oib.as_str()),
            (IdKind::Lei, l.as_str()),
            (IdKind::Iban, account.as_str()),
            (IdKind::Vat, de.as_str()),
        ] {
            assert_eq!(ok(kind, valid), valid, "{kind:?}");
            assert!(bad(kind, &broken(valid)), "{kind:?} {valid}");
        }
        assert_eq!(ok(IdKind::Duns, "15-048-3782"), "150483782");
        assert!(bad(IdKind::Duns, "15048378"));
    }

    #[test]
    fn values_are_compacted_and_uppercased() {
        let pib = mod_11_10("10873452");
        let spaced = format!("{} {} {}", &pib[..3], &pib[3..6], &pib[6..]);
        assert_eq!(ok(IdKind::RsPib, &spaced), pib);
        let account = iban("RS", "265000000837164209");
        let written = format!(
            "{} {} {}",
            account[..4].to_lowercase(),
            &account[4..12],
            &account[12..]
        );
        assert_eq!(ok(IdKind::Iban, &written), account);
        let l = lei("254900PRIMER0000A1");
        assert_eq!(ok(IdKind::Lei, &l.to_lowercase()), l);
        assert_eq!(ok(IdKind::Vat, &format!("rs {pib}")), format!("RS{pib}"));
        let id = Identifier::parse(IdKind::Lei, &l).unwrap();
        assert_eq!(id.alias(), format!("id:lei:{l}"));
    }

    #[test]
    fn vat_of_serbia_norway_and_switzerland() {
        let pib = mod_11_10("10873452");
        assert_eq!(ok(IdKind::Vat, &format!("RS{pib}")), format!("RS{pib}"));
        assert!(bad(IdKind::Vat, &format!("RS{}", broken(&pib))));
        let no = (91_734_520u32..)
            .find_map(|n| mod_11(&format!("{n:08}"), [3, 2, 7, 6, 5, 4, 3, 2]))
            .unwrap();
        assert!(Identifier::parse(IdKind::Vat, &format!("NO {no} MVA")).is_some());
        let ch = (20_837_164u32..)
            .find_map(|n| mod_11(&format!("{n:08}"), [5, 4, 3, 2, 7, 6, 5, 4]))
            .unwrap();
        let written = format!("CHE-{}.{}.{} MWST", &ch[..3], &ch[3..6], &ch[6..]);
        assert_eq!(ok(IdKind::Vat, &written), format!("CHE{ch}MWST"));
        // A format-only member state passes, a checksum one with a wrong digit does not.
        assert!(Identifier::parse(IdKind::Vat, "EE100837164").is_some());
        assert!(bad(
            IdKind::Vat,
            &format!("DE{}", broken(&mod_11_10("31745208")))
        ));
    }

    #[test]
    fn placeholders_and_label_text_are_refused() {
        for (kind, raw) in [
            (IdKind::RsPib, "000000000"),
            (IdKind::RsPib, "111111111"),
            (IdKind::Duns, "123456789"),
            (IdKind::Duns, "999999999"),
            (IdKind::RsMb, "12345678"),
            (IdKind::Vat, "RS123456789"),
            (IdKind::Vat, "VAT NUMBER"),
            (IdKind::Lei, "LEINOTAVAILABLEHERE"),
            (IdKind::Iban, "IBAN"),
            (IdKind::RsPib, "PIB"),
            (IdKind::RsPib, ""),
            (IdKind::RsPib, " - / . "),
        ] {
            assert!(bad(kind, raw), "{kind:?} {raw:?}");
        }
    }

    #[test]
    fn fact_keys_and_names() {
        for kind in IdKind::ALL {
            let key = format!("signal.{}", kind.as_str());
            assert_eq!(IdKind::from_fact_key(&key), Some(kind));
        }
        assert_eq!(IdKind::from_fact_key("signal.bic"), None);
        // Strongest first.
        let mut sorted = IdKind::ALL;
        sorted.sort();
        assert_eq!(sorted, IdKind::ALL);
    }
}
