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
