//! Identifier checks (ADR 0002 section 1.5). Every input is compact and uppercase (no spaces,
//! dots or dashes); [`crate::Identifier::parse`] compacts before it calls these.
//!
//! The check-digit code is quail's (`quail_signals::checksum`), reused here, not copied:
//!
//! - [`lei`]: ISO 17442, 18 alphanumerics and two check digits, ISO 7064 MOD 97-10;
//! - [`iban`]: the country's registry length and ISO 7064 MOD 97-10 after rotation;
//! - [`rs_pib`] (ISO 7064 MOD 11,10), [`rs_mb`] (MOD 11), [`hr_oib`] (MOD 11,10);
//! - [`vat`]: the VAT format per country. EU member states, GB and XI are quail's
//!   [`eu_vat`](quail_signals::checksum::eu_vat) (check digits where a public algorithm exists,
//!   else the format); added here are Serbia (`RS` and the PIB), Norway (`NO`, the
//!   organisasjonsnummer and `MVA`) and Switzerland (`CHE`, the UID and `MWST`, `TVA`, `IVA` or
//!   `TPV`). A VIES lookup comes later, with the registries (ADR 0002 section 2);
//! - [`duns`]: format only (nine digits), as the ADR says: DUNS has no public check digit.

use quail_signals::checksum;

pub use quail_signals::checksum::VatCheck;

/// LEI (ISO 17442): 18 alphanumerics and two check digits, MOD 97-10.
pub fn lei(compact: &str) -> bool {
    checksum::lei(compact)
}

/// IBAN: two-letter country, two check digits, the registry length of that country, MOD 97-10.
pub fn iban(compact: &str) -> bool {
    checksum::iban(compact)
}

/// Serbian PIB: 9 digits, ISO 7064 MOD 11,10.
pub fn rs_pib(compact: &str) -> bool {
    checksum::rs_pib(compact)
}

/// Serbian matični broj: 8 digits, MOD 11.
pub fn rs_mb(compact: &str) -> bool {
    checksum::rs_mb(compact)
}

/// Croatian OIB: 11 digits, ISO 7064 MOD 11,10.
pub fn hr_oib(compact: &str) -> bool {
    checksum::hr_oib(compact)
}

/// DUNS: nine digits. Format only, as ADR 0002 section 1.5 says.
pub fn duns(compact: &str) -> bool {
    compact.len() == 9 && compact.bytes().all(|c| c.is_ascii_digit())
}

/// A VAT number with its country prefix (`DE123456789`, `RS101234567`, `CHE123456788MWST`).
pub fn vat(compact: &str) -> VatCheck {
    if let Some(body) = compact.strip_prefix("CHE") {
        let body = ["MWST", "TVA", "IVA", "TPV"]
            .iter()
            .find_map(|s| body.strip_suffix(s))
            .unwrap_or(body);
        return mod_11(body, [5, 4, 3, 2, 7, 6, 5, 4]);
    }
    let b = compact.as_bytes();
    if b.len() <= 2 || !b[..2].iter().all(u8::is_ascii_uppercase) {
        return VatCheck::Malformed;
    }
    let (country, body) = compact.split_at(2);
    match country {
        "RS" => {
            if body.len() != 9 || !body.bytes().all(|c| c.is_ascii_digit()) {
                VatCheck::Malformed
            } else if checksum::rs_pib(body) {
                VatCheck::Valid
            } else {
                VatCheck::BadChecksum
            }
        }
        "NO" => mod_11(
            body.strip_suffix("MVA").unwrap_or(body),
            [3, 2, 7, 6, 5, 4, 3, 2],
        ),
        _ => checksum::eu_vat(country, body),
    }
}

/// Nine digits whose last is the MOD 11 check digit of the first eight under `weights`
/// (11 - sum mod 11; 11 is written 0, 10 is never issued). Swiss UID and Norwegian
/// organisasjonsnummer.
fn mod_11(body: &str, weights: [u32; 8]) -> VatCheck {
    let b = body.as_bytes();
    if b.len() != 9 || !b.iter().all(u8::is_ascii_digit) {
        return VatCheck::Malformed;
    }
    let sum: u32 = b[..8]
        .iter()
        .zip(weights)
        .map(|(c, w)| u32::from(c - b'0') * w)
        .sum();
    let ok = match 11 - sum % 11 {
        11 => b[8] == b'0',
        10 => false,
        c => u32::from(b[8] - b'0') == c,
    };
    if ok {
        VatCheck::Valid
    } else {
        VatCheck::BadChecksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testids::{broken, iban as make_iban, lei as make_lei, mb, mod_11, mod_11_10};

    const CH: [u32; 8] = [5, 4, 3, 2, 7, 6, 5, 4];
    const NO: [u32; 8] = [3, 2, 7, 6, 5, 4, 3, 2];

    /// The first head from `start` up whose MOD 11 check digit is not 10.
    fn with_mod_11(start: u32, weights: [u32; 8]) -> String {
        (start..)
            .find_map(|n| mod_11(&format!("{n:08}"), weights))
            .expect("a head")
    }

    #[test]
    fn lei_iso_17442() {
        let l = make_lei("8945002TQV7RMXEW3K");
        assert_eq!(l.len(), 20);
        assert!(lei(&l), "{l}");
        let l2 = make_lei("254900PRIMER0000A1");
        assert!(lei(&l2), "{l2}");
        // Wrong check digits, the wrong length, lowercase, letters in the check.
        assert!(!lei(&broken(&l)));
        assert!(!lei(&l[..19]));
        assert!(!lei(&l.to_lowercase()));
        assert!(!lei(&format!("{}AB", &l[..18])));
        assert!(!lei(""));
    }

    #[test]
    fn iban_mod_97() {
        let rs = make_iban("RS", "265000000837164209");
        assert_eq!(rs.len(), 22);
        assert!(iban(&rs), "{rs}");
        let de = make_iban("DE", "870199990837164209");
        assert!(iban(&de), "{de}");
        let gb = make_iban("GB", "PRIM20451797164209");
        assert!(iban(&gb), "{gb}");
        assert!(!iban(&broken(&rs)));
        // The wrong length for the country, an unknown country, lowercase.
        assert!(!iban(&make_iban("RS", "2650000008371642091")));
        assert!(!iban(&make_iban("QQ", "265000000837164209")));
        assert!(!iban(&rs.to_lowercase()));
        assert!(!iban("RS"));
    }

    #[test]
    fn serbian_and_croatian_numbers() {
        let pib = mod_11_10("10873452");
        assert!(rs_pib(&pib));
        assert!(!rs_pib(&broken(&pib)));
        assert!(!rs_pib(&pib[..8]));
        let m = mb("2108734");
        assert!(rs_mb(&m), "{m}");
        assert!(!rs_mb(&broken(&m)));
        assert!(!rs_mb(&format!("{m}0")));
        let oib = mod_11_10("6087345219");
        assert!(hr_oib(&oib));
        assert!(!hr_oib(&broken(&oib)));
        // A PIB is not an OIB: the length differs.
        assert!(!hr_oib(&pib));
    }

    #[test]
    fn duns_is_format_only() {
        assert!(duns("604817352"));
        assert!(duns("080000001"));
        assert!(!duns("60481735"));
        assert!(!duns("6048173520"));
        assert!(!duns("60481735A"));
        assert!(!duns(""));
    }

    #[test]
    fn vat_eu_from_quail() {
        let de = format!("DE{}", mod_11_10("31745208"));
        assert_eq!(vat(&de), VatCheck::Valid);
        assert_eq!(vat(&broken(&de)), VatCheck::BadChecksum);
        let hr = format!("HR{}", mod_11_10("6087345219"));
        assert_eq!(vat(&hr), VatCheck::Valid);
        // Member states without a public algorithm: format only.
        assert_eq!(vat("EE100837164"), VatCheck::FormatOnly);
        assert_eq!(vat("EE10083716"), VatCheck::Malformed);
        assert_eq!(vat("BG2083716420"), VatCheck::FormatOnly);
    }

    #[test]
    fn vat_serbia_norway_switzerland() {
        let pib = mod_11_10("10873452");
        assert_eq!(vat(&format!("RS{pib}")), VatCheck::Valid);
        assert_eq!(vat(&format!("RS{}", broken(&pib))), VatCheck::BadChecksum);
        assert_eq!(vat(&format!("RS{}", &pib[..8])), VatCheck::Malformed);
        assert_eq!(vat(&format!("RS{pib}0")), VatCheck::Malformed);

        let no = with_mod_11(91_734_520, NO);
        assert_eq!(vat(&format!("NO{no}")), VatCheck::Valid, "{no}");
        assert_eq!(vat(&format!("NO{no}MVA")), VatCheck::Valid);
        assert_eq!(vat(&format!("NO{}MVA", broken(&no))), VatCheck::BadChecksum);
        assert_eq!(vat(&format!("NO{}", &no[..8])), VatCheck::Malformed);

        let ch = with_mod_11(20_837_164, CH);
        for suffix in ["", "MWST", "TVA", "IVA", "TPV"] {
            assert_eq!(
                vat(&format!("CHE{ch}{suffix}")),
                VatCheck::Valid,
                "{ch}{suffix}"
            );
        }
        assert_eq!(vat(&format!("CHE{}", broken(&ch))), VatCheck::BadChecksum);
        assert_eq!(vat(&format!("CHE{ch}XYZ")), VatCheck::Malformed);
        assert_eq!(vat(&format!("CH{ch}")), VatCheck::Malformed);
    }

    #[test]
    fn mod_11_check_digit_ten_is_never_issued() {
        // A head whose check digit would be 10: every last digit is refused.
        let head = (10_000_000u32..20_000_000)
            .map(|n| format!("{n:08}"))
            .find(|h| mod_11(h, NO).is_none())
            .expect("a head");
        for d in 0..10 {
            assert_ne!(vat(&format!("NO{head}{d}")), VatCheck::Valid, "{head}{d}");
        }
        // A check digit of 11 is written 0.
        let head = (10_000_000u32..20_000_000)
            .map(|n| format!("{n:08}"))
            .find(|h| {
                let s: u32 = h
                    .bytes()
                    .zip(NO)
                    .map(|(b, w)| u32::from(b - b'0') * w)
                    .sum();
                s.is_multiple_of(11)
            })
            .expect("a head");
        assert_eq!(vat(&format!("NO{head}0")), VatCheck::Valid, "{head}0");
    }

    #[test]
    fn vat_malformed_prefixes() {
        for s in [
            "",
            "D",
            "DE",
            "12345678901",
            "US123456789",
            "XX837164209",
            "de318745208",
        ] {
            assert_eq!(vat(s), VatCheck::Malformed, "{s:?}");
        }
    }
}
