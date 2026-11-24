//! Invented identifiers with their check digits computed here, independently of the code under
//! test (or deliberately broken). None of them is a registered company's.

/// Appends the ISO 7064 MOD 11,10 check digit (PIB, OIB, DE VAT).
pub fn mod_11_10(head: &str) -> String {
    let mut a = 10;
    for d in head.bytes().map(|b| u32::from(b - b'0')) {
        a = (a + d) % 10;
        if a == 0 {
            a = 10;
        }
        a = (a * 2) % 11;
    }
    format!("{head}{}", (11 - a) % 10)
}

/// Serbian MB: seven digits and the MOD 11 check digit (weights 2 7 6 5 4 3 2).
pub fn mb(head: &str) -> String {
    let sum: u32 = head
        .bytes()
        .zip([2, 7, 6, 5, 4, 3, 2])
        .map(|(b, w)| u32::from(b - b'0') * w)
        .sum();
    let k = match 11 - sum % 11 {
        10 | 11 => 0,
        k => k,
    };
    format!("{head}{k}")
}

/// The MOD 11 check digit under `weights` (Swiss UID, Norwegian orgnr); `None` when it would
/// be 10, which is never issued.
pub fn mod_11(head: &str, weights: [u32; 8]) -> Option<String> {
    let sum: u32 = head
        .bytes()
        .zip(weights)
        .map(|(b, w)| u32::from(b - b'0') * w)
        .sum();
    match (11 - sum % 11) % 11 {
        10 => None,
        k => Some(format!("{head}{k}")),
    }
}

/// The remainder mod 97 of `s` with letters as 10..35.
fn rem_97(s: &str) -> u32 {
    s.chars().fold(0, |r, c| {
        let d = c.to_digit(36).expect("alphanumeric");
        if d >= 10 {
            (r * 100 + d) % 97
        } else {
            (r * 10 + d) % 97
        }
    })
}

/// An LEI: 18 characters and the two ISO 17442 check digits.
pub fn lei(head: &str) -> String {
    format!("{head}{:02}", 98 - rem_97(&format!("{head}00")))
}

/// An IBAN: country, the two check digits, the BBAN.
pub fn iban(country: &str, bban: &str) -> String {
    let check = 98 - rem_97(&format!("{bban}{country}00"));
    format!("{country}{check:02}{bban}")
}

/// `s` with its last digit changed (a broken check digit).
pub fn broken(s: &str) -> String {
    let (head, last) = s.split_at(s.len() - 1);
    let d = last.parse::<u32>().expect("a digit");
    format!("{head}{}", (d + 1) % 10)
}
