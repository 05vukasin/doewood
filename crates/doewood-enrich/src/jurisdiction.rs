//! A company's `jurisdiction`: one ISO 3166-1 alpha-2 country (`RS`), or an ISO 3166-2
//! subdivision (`US-CA`) where it is known, always uppercase (issue #201).
//!
//! - [`normalize`]: a value already meant as ISO 3166 (quail's party facts). A list of
//!   candidates (`us|ca`, quail's guess for an `Inc.`: the US or Canada) is not one
//!   jurisdiction, so it gives none.
//! - [`from_sec`]: a value from an SEC source (EDGAR `stateOfIncorporation`, the exhibit 21
//!   jurisdiction column). A two-letter code there is a US state when it is one (`CA` is
//!   California, `US-CA`), and EDGAR's `A0`–`B0` are the Canadian provinces (`CA-ON`). Full
//!   state and province names map the same way.
//!
//! A filter on a country also finds its subdivisions: `US` finds `US-CA`.

/// US states, the District of Columbia and the territories EDGAR uses: code and name.
const US_STATES: &[(&str, &str)] = &[
    ("AL", "alabama"),
    ("AK", "alaska"),
    ("AZ", "arizona"),
    ("AR", "arkansas"),
    ("CA", "california"),
    ("CO", "colorado"),
    ("CT", "connecticut"),
    ("DE", "delaware"),
    ("DC", "district of columbia"),
    ("FL", "florida"),
    ("GA", "georgia"),
    ("HI", "hawaii"),
    ("ID", "idaho"),
    ("IL", "illinois"),
    ("IN", "indiana"),
    ("IA", "iowa"),
    ("KS", "kansas"),
    ("KY", "kentucky"),
    ("LA", "louisiana"),
    ("ME", "maine"),
    ("MD", "maryland"),
    ("MA", "massachusetts"),
    ("MI", "michigan"),
    ("MN", "minnesota"),
    ("MS", "mississippi"),
    ("MO", "missouri"),
    ("MT", "montana"),
    ("NE", "nebraska"),
    ("NV", "nevada"),
    ("NH", "new hampshire"),
    ("NJ", "new jersey"),
    ("NM", "new mexico"),
    ("NY", "new york"),
    ("NC", "north carolina"),
    ("ND", "north dakota"),
    ("OH", "ohio"),
    ("OK", "oklahoma"),
    ("OR", "oregon"),
    ("PA", "pennsylvania"),
    ("RI", "rhode island"),
    ("SC", "south carolina"),
    ("SD", "south dakota"),
    ("TN", "tennessee"),
    ("TX", "texas"),
    ("UT", "utah"),
    ("VT", "vermont"),
    ("VA", "virginia"),
    ("WA", "washington"),
    ("WV", "west virginia"),
    ("WI", "wisconsin"),
    ("WY", "wyoming"),
    ("AS", "american samoa"),
    ("GU", "guam"),
    ("MP", "northern mariana islands"),
    ("PR", "puerto rico"),
    ("VI", "virgin islands"),
];

/// Canadian provinces and territories: EDGAR's code (where it has one), ISO 3166-2 code,
/// name.
const CA_PROVINCES: &[(&str, &str, &str)] = &[
    ("A0", "AB", "alberta"),
    ("A1", "BC", "british columbia"),
    ("A2", "MB", "manitoba"),
    ("A3", "NB", "new brunswick"),
    ("A4", "NL", "newfoundland and labrador"),
    ("A4", "NL", "newfoundland"),
    ("A5", "NS", "nova scotia"),
    ("A6", "ON", "ontario"),
    ("A7", "PE", "prince edward island"),
    ("A8", "QC", "quebec"),
    ("A9", "SK", "saskatchewan"),
    ("B0", "YT", "yukon"),
    ("", "NT", "northwest territories"),
    ("", "NU", "nunavut"),
];

/// Country names an SEC source writes out.
const COUNTRIES: &[(&str, &str)] = &[
    ("united states", "US"),
    ("united states of america", "US"),
    ("usa", "US"),
    ("u s", "US"),
    ("u s a", "US"),
    ("canada", "CA"),
];

/// `raw` as one ISO 3166 code, uppercase: `rs` gives `RS`, `us-ca` gives `US-CA`. A list of
/// several (`us|ca`, `RS,ME`) or anything else gives `None`.
pub fn normalize(raw: &str) -> Option<String> {
    let mut one: Option<String> = None;
    for part in raw.split(['|', ',', ';']) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let code = iso(part)?;
        match &one {
            Some(c) if *c != code => return None,
            _ => one = Some(code),
        }
    }
    one
}

/// A jurisdiction from an SEC source: a US state (code or name) as `US-XX`, an EDGAR
/// province code (`A0`–`B0`) or a province name as `CA-XX`, an ISO 3166-2 code as is, and the
/// United States (`US` too) or Canada by name. Another two-letter code gives `None`: in an
/// SEC source it is no country.
pub fn from_sec(raw: &str) -> Option<String> {
    let t = raw.trim();
    if t.len() == 2 {
        let up = t.to_ascii_uppercase();
        if let Some((code, _)) = US_STATES.iter().find(|(c, _)| *c == up) {
            return Some(format!("US-{code}"));
        }
        if let Some((_, iso, _)) = CA_PROVINCES.iter().find(|(e, _, _)| *e == up) {
            return Some(format!("CA-{iso}"));
        }
        return (up == "US").then_some(up);
    }
    if t.contains('-')
        && let Some(code) = iso(t)
    {
        return Some(code);
    }
    let name = words(t);
    let name = name
        .strip_prefix("state of ")
        .or_else(|| name.strip_prefix("commonwealth of "))
        .or_else(|| name.strip_prefix("province of "))
        .unwrap_or(&name);
    // `Delaware, USA`, `Ontario Canada`: the country after the subdivision says nothing more.
    let name = [
        " united states of america",
        " united states",
        " u s a",
        " usa",
        " u s",
    ]
    .iter()
    .find_map(|s| name.strip_suffix(s))
    .unwrap_or(name);
    if let Some((code, _)) = US_STATES.iter().find(|(_, n)| *n == name) {
        return Some(format!("US-{code}"));
    }
    let province = name.strip_suffix(" canada").unwrap_or(name);
    if let Some((_, iso, _)) = CA_PROVINCES.iter().find(|(_, _, n)| *n == province) {
        return Some(format!("CA-{iso}"));
    }
    COUNTRIES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, c)| (*c).to_owned())
}

/// One ISO 3166 code: two ASCII letters, optionally `-` and one to three letters or digits.
fn iso(s: &str) -> Option<String> {
    let (country, sub) = match s.split_once('-') {
        Some((c, sub)) => (c, Some(sub)),
        None => (s, None),
    };
    let country_ok = country.len() == 2 && country.bytes().all(|b| b.is_ascii_alphabetic());
    let sub_ok = sub
        .is_none_or(|x| (1..=3).contains(&x.len()) && x.bytes().all(|b| b.is_ascii_alphanumeric()));
    (country_ok && sub_ok).then(|| s.to_ascii_uppercase())
}

/// Lowercase words, punctuation as spaces, accents on the few names that have them dropped.
fn words(s: &str) -> String {
    let s: String = s
        .chars()
        .map(|c| match c {
            'é' | 'É' => 'e',
            c if c.is_alphanumeric() => c.to_ascii_lowercase(),
            _ => ' ',
        })
        .collect();
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_keeps_one_code() {
        assert_eq!(normalize("rs").as_deref(), Some("RS"));
        assert_eq!(normalize(" GB ").as_deref(), Some("GB"));
        assert_eq!(normalize("us-ca").as_deref(), Some("US-CA"));
        assert_eq!(normalize("rs|rs").as_deref(), Some("RS"));
        // quail's candidates for an `Inc.`: the US or Canada, so neither.
        assert_eq!(normalize("us|ca"), None);
        assert_eq!(normalize("RS,ME"), None);
        assert_eq!(normalize("england-wales"), None);
        assert_eq!(normalize("Delaware"), None);
        assert_eq!(normalize(""), None);
    }

    #[test]
    fn sec_state_codes_are_us_subdivisions() {
        assert_eq!(from_sec("CA").as_deref(), Some("US-CA"));
        assert_eq!(from_sec("de").as_deref(), Some("US-DE"));
        assert_eq!(from_sec("PR").as_deref(), Some("US-PR"));
        assert_eq!(from_sec("A6").as_deref(), Some("CA-ON"));
        assert_eq!(from_sec("B0").as_deref(), Some("CA-YT"));
        // An EDGAR country code or a two-letter word that is no state is no country.
        assert_eq!(from_sec("X0"), None);
        assert_eq!(from_sec("GB"), None);
        assert_eq!(from_sec("US").as_deref(), Some("US"));
    }

    #[test]
    fn sec_names() {
        assert_eq!(from_sec("California").as_deref(), Some("US-CA"));
        assert_eq!(from_sec("State of Delaware").as_deref(), Some("US-DE"));
        assert_eq!(from_sec("Delaware, U.S.A.").as_deref(), Some("US-DE"));
        assert_eq!(from_sec("New York").as_deref(), Some("US-NY"));
        assert_eq!(from_sec("Québec").as_deref(), Some("CA-QC"));
        assert_eq!(from_sec("Ontario, Canada").as_deref(), Some("CA-ON"));
        assert_eq!(from_sec("United States").as_deref(), Some("US"));
        assert_eq!(from_sec("Canada").as_deref(), Some("CA"));
        assert_eq!(from_sec("us-tx").as_deref(), Some("US-TX"));
        assert_eq!(from_sec("Netherlands"), None);
    }
}
