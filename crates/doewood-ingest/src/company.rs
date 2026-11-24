//! Company identity, the deterministic part of ADR 0002 section 1: name normalization, legal
//! forms, aliases from parentheses, identifiers, and the alias keys the match ladder looks up.
//!
//! - **Normalization** ([`normalize`]): Cyrillic transliterated to Latin (Serbian, Macedonian,
//!   Bulgarian, Russian, Ukrainian), NFKD with the combining marks dropped, casefolded,
//!   punctuation deleted (`U.K.` → `uk`), spaces collapsed. The original spelling is kept
//!   next to the key.
//! - **Legal forms** ([`LEGAL_FORMS`]), with spelling variants and Balkan forms (`d.o.o.`,
//!   `a.d.`, `d.d.`, `j.p.`, `PR`, `s.p.`, `dooel`, ...), are taken off the end (or the start)
//!   of the name; geography and noise words are left out of the key, but never empty it.
//! - **Aliases** from parentheses (`formerly`, `f/k/a`, `n/k/a`, `d/b/a`, `trading as`,
//!   `u daljem tekstu`, ...) and inline `d/b/a`: the bracketed text leaves the base key.
//! - **Role terms** (`Supplier`, `Naručilac`, ...) are never a company.
//! - **Identifiers** are checked with quail's check-digit code before use, and placeholders
//!   are refused.

use quail_signals::checksum::{self, VatCheck};
use unicode_normalization::UnicodeNormalization as _;
use unicode_normalization::char::is_combining_mark;

/// A legal form: its canonical spelling and its spellings after [`normalize`], as token runs.
pub struct LegalForm {
    /// How the form is written in the graph.
    pub canonical: &'static str,
    /// Normalized spellings, each a run of tokens.
    pub variants: &'static [&'static [&'static str]],
}

macro_rules! forms {
    ($($canonical:literal => [$($variant:expr),+ $(,)?]),+ $(,)?) => {
        &[$(LegalForm { canonical: $canonical, variants: &[$($variant),+] }),+]
    };
}

/// Legal forms, Balkan forms first. Longer variants are tried before shorter ones.
pub const LEGAL_FORMS: &[LegalForm] = forms![
    "d.o.o." => [
        &["doo"], &["d", "o", "o"], &["drustvo", "sa", "ogranicenom", "odgovornoscu"],
        &["drustvo", "s", "ogranicenom", "odgovornoscu"], &["drustvo", "za", "ogranicenom", "odgovornoscu"],
    ],
    "dooel" => [&["dooel"], &["d", "o", "o", "e", "l"]],
    "a.d." => [&["ad"], &["a", "d"], &["akcionarsko", "drustvo"], &["akcionarsko", "drustvo"]],
    "d.d." => [&["dd"], &["d", "d"], &["dionicko", "drustvo"]],
    "j.p." => [&["jp"], &["j", "p"], &["javno", "preduzece"], &["javno", "poduzece"]],
    "j.k.p." => [&["jkp"], &["j", "k", "p"], &["javno", "komunalno", "preduzece"]],
    "o.d." => [&["od"], &["o", "d"], &["ortacko", "drustvo"]],
    "k.d." => [&["kd"], &["k", "d"], &["komanditno", "drustvo"]],
    "PR" => [&["pr"], &["preduzetnik"]],
    "s.p." => [&["sp"], &["s", "p"]],
    "ood" => [&["ood"]],
    "eood" => [&["eood"]],
    "Sp. z o.o." => [&["sp", "z", "oo"], &["spolka", "z", "oo"]],
    "GmbH & Co. KG" => [&["gmbh", "co", "kg"]],
    "GmbH" => [&["gmbh"]],
    "AG" => [&["ag"]],
    "KG" => [&["kg"]],
    "SE" => [&["se"]],
    "LLC" => [&["llc"], &["l", "l", "c"]],
    "LLP" => [&["llp"]],
    "Inc." => [&["inc"], &["incorporated"]],
    "Corp." => [&["corp"], &["corporation"]],
    "Ltd" => [&["ltd"], &["limited"]],
    "plc" => [&["plc"]],
    "S.A." => [&["sa"], &["s", "a"]],
    "SAS" => [&["sas"]],
    "SARL" => [&["sarl"]],
    "S.p.A." => [&["spa"], &["s", "p", "a"]],
    "S.r.l." => [&["srl"], &["s", "r", "l"]],
    "B.V." => [&["bv"], &["b", "v"]],
    "N.V." => [&["nv"], &["n", "v"]],
    "Kft." => [&["kft"]],
    "Zrt." => [&["zrt"]],
    "s.r.o." => [&["sro"], &["s", "r", "o"]],
    "a.s." => [&["as"], &["a", "s"]],
    "ApS" => [&["aps"]],
    "AB" => [&["ab"]],
    "Oy" => [&["oy"], &["oyj"]],
];

/// Place names left out of the key (they never veto a match): seats written after the form.
const GEOGRAPHY: &[&str] = &[
    "beograd",
    "belgrade",
    "novi",
    "sad",
    "nis",
    "kragujevac",
    "subotica",
    "zrenjanin",
    "pancevo",
    "cacak",
    "kraljevo",
    "zagreb",
    "split",
    "rijeka",
    "osijek",
    "sarajevo",
    "banja",
    "luka",
    "mostar",
    "tuzla",
    "podgorica",
    "skopje",
    "ljubljana",
    "maribor",
    "sofia",
    "sofija",
    "plovdiv",
    "varna",
    "bucharest",
    "budapest",
    "wien",
    "vienna",
    "berlin",
    "munchen",
    "munich",
    "london",
    "paris",
    "srbija",
    "serbia",
    "hrvatska",
    "croatia",
    "slovenija",
    "slovenia",
    "makedonija",
    "macedonia",
    "bulgaria",
    "bugarska",
    "montenegro",
];

/// Words left out of the key: articles and connectors.
const NOISE: &[&str] = &["the", "and", "und", "et", "i"];

/// Contract role terms: a name made only of these is never a company.
const ROLE_TERMS: &[&str] = &[
    "supplier",
    "customer",
    "buyer",
    "seller",
    "vendor",
    "client",
    "purchaser",
    "contractor",
    "subcontractor",
    "lessor",
    "lessee",
    "licensor",
    "licensee",
    "employer",
    "party",
    "parties",
    "provider",
    "recipient",
    "narucilac",
    "narucioc",
    "dobavljac",
    "kupac",
    "prodavac",
    "izvodjac",
    "podizvodjac",
    "isporucilac",
    "investitor",
    "zakupac",
    "zakupodavac",
    "davalac",
    "primalac",
    "korisnik",
    "ugovarac",
    "poslodavac",
    "strana",
    "ugovorna",
    "ugovorne",
    "strane",
];

/// The kind of an alias found in a name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AliasKind {
    /// `formerly`, `f/k/a`, `prethodno`: a former name.
    FormerName,
    /// `d/b/a`, `trading as`, `t/a`: a trading name.
    TradingName,
    /// `n/k/a`, `now known as`: the current name of the base.
    NowKnownAs,
    /// `u daljem tekstu`, `hereinafter`: a defined term.
    DefinedTerm,
    /// Any other bracketed text.
    Annotation,
}

impl AliasKind {
    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FormerName => "former_name",
            Self::TradingName => "trading_name",
            Self::NowKnownAs => "now_known_as",
            Self::DefinedTerm => "defined_term",
            Self::Annotation => "annotation",
        }
    }
}

/// An alias found in a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alias {
    /// The alias as written.
    pub text: String,
    /// What kind.
    pub kind: AliasKind,
}

/// A company name taken apart for matching.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompanyName {
    /// As written.
    pub original: String,
    /// The key: normalized, without annotations, legal form, geography and noise words.
    pub key: String,
    /// The key without spaces (ladder step 6).
    pub squashed: String,
    /// The canonical legal form, if the name has one.
    pub legal_form: Option<&'static str>,
    /// Aliases from parentheses and inline statements; role terms left out.
    pub aliases: Vec<Alias>,
}

impl CompanyName {
    /// Parses a name. `None` when nothing distinctive is left (empty, only a legal form, or a
    /// contract role term).
    pub fn parse(surface: &str) -> Option<Self> {
        let (base, aliases) = split_aliases(surface);
        let tokens = tokens(&normalize(&base));
        let only_a_form = LEGAL_FORMS
            .iter()
            .flat_map(|f| f.variants.iter())
            .any(|run| run.len() == tokens.len() && starts_with(&tokens, run));
        if only_a_form {
            return None;
        }
        let (core, legal_form) = strip_legal_form(&tokens);
        let core = drop_words(&core);
        if core.is_empty() || core.iter().all(|t| ROLE_TERMS.contains(&t.as_str())) {
            return None;
        }
        let key = core.join(" ");
        let aliases = aliases
            .into_iter()
            .filter(|a| !is_role_term(&a.text))
            .collect();
        Some(Self {
            original: surface.trim().to_owned(),
            squashed: core.concat(),
            key,
            legal_form,
            aliases,
        })
    }

    /// The alias keys the ladder looks this name up by, in ladder order: normalized with its
    /// legal form (steps 1 to 4), squashed (6), legal-form-blind (7).
    pub fn ladder_keys(&self) -> [(LadderStep, String); 3] {
        let form = self.legal_form.unwrap_or("");
        [
            (LadderStep::Normalized, format!("name:{}|{form}", self.key)),
            (
                LadderStep::Squashed,
                format!("squash:{}|{form}", self.squashed),
            ),
            (LadderStep::FormBlind, format!("blind:{}", self.key)),
        ]
    }
}

/// A step of the match ladder that doewood takes (the deterministic ones; the fuzzy steps
/// belong to enrichment and never merge on their own).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LadderStep {
    /// A validated registry identifier (VAT, PIB, MB, LEI, IBAN, ...).
    Identifier,
    /// Exact, lowercase, normalized and annotations stripped: one normalized key.
    Normalized,
    /// A learned alias (a former or trading name stated in a document).
    LearnedAlias,
    /// The key without spaces.
    Squashed,
    /// The key without the legal form, when exactly one company owns it.
    FormBlind,
}

impl LadderStep {
    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identifier => "identifier",
            Self::Normalized => "normalized",
            Self::LearnedAlias => "learned_alias",
            Self::Squashed => "squashed",
            Self::FormBlind => "legal_form_blind",
        }
    }

    /// The evidence reliability of a match at this step (ADR 0002, section 1.6): 0.95 for a
    /// registry identifier, 0.90 for the name ladder, scaled down on the looser steps.
    pub fn confidence(self) -> f32 {
        match self {
            Self::Identifier => 0.95,
            Self::Normalized | Self::LearnedAlias => 0.90,
            Self::Squashed => 0.88,
            Self::FormBlind => 0.85,
        }
    }
}

/// Normalizes text for a key: Cyrillic → Latin, NFKD without combining marks, casefold,
/// punctuation deleted, whitespace collapsed to one space.
pub fn normalize(s: &str) -> String {
    let mut latin = String::with_capacity(s.len());
    for c in s.chars().flat_map(char::to_lowercase) {
        match cyrillic(c) {
            Some(t) => latin.push_str(t),
            None => latin.push(c),
        }
    }
    let mut out = String::with_capacity(latin.len());
    let mut space = false;
    for c in latin.nfkd().filter(|c| !is_combining_mark(*c)) {
        let folded = match c {
            'đ' => "dj",
            'ß' => "ss",
            'æ' => "ae",
            'œ' => "oe",
            'ø' => "o",
            'ł' => "l",
            'þ' => "th",
            'ı' => "i",
            _ => "",
        };
        if !folded.is_empty() {
            if space {
                out.push(' ');
                space = false;
            }
            out.push_str(folded);
        } else if c.is_whitespace() {
            space = !out.is_empty();
        } else if c.is_alphanumeric() {
            if space {
                out.push(' ');
                space = false;
            }
            out.extend(c.to_lowercase());
        }
        // Anything else is punctuation: deleted, so "U.K." becomes "uk".
    }
    out
}

/// Latin for one lowercase Cyrillic letter (Serbian, Macedonian, Bulgarian, Russian and
/// Ukrainian), consistent with the Latin spellings after marks are dropped (`ж` and `ž` both
/// become `z`, `ђ` and `đ` both `dj`).
fn cyrillic(c: char) -> Option<&'static str> {
    Some(match c {
        'а' => "a",
        'б' => "b",
        'в' => "v",
        'г' | 'ґ' => "g",
        'д' => "d",
        'ђ' => "dj",
        'ѓ' => "gj",
        'е' | 'ё' | 'э' => "e",
        'є' => "je",
        'ж' | 'з' => "z",
        'ѕ' | 'џ' => "dz",
        'и' | 'і' => "i",
        'ї' => "ji",
        'й' | 'ј' => "j",
        'к' => "k",
        'ќ' => "kj",
        'л' => "l",
        'љ' => "lj",
        'м' => "m",
        'н' => "n",
        'њ' => "nj",
        'о' => "o",
        'п' => "p",
        'р' => "r",
        'с' | 'ш' => "s",
        'т' => "t",
        'ћ' | 'ц' | 'ч' => "c",
        'у' => "u",
        'ф' => "f",
        'х' => "h",
        'щ' => "sc",
        'ы' => "y",
        'ю' => "ju",
        'я' => "ja",
        'ъ' | 'ь' => "",
        _ => return None,
    })
}

fn tokens(normalized: &str) -> Vec<String> {
    normalized
        .split(' ')
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
        .collect()
}

fn ends_with(tokens: &[String], run: &[&str]) -> bool {
    tokens.len() >= run.len()
        && tokens[tokens.len() - run.len()..]
            .iter()
            .zip(run)
            .all(|(a, b)| a == b)
}

fn starts_with(tokens: &[String], run: &[&str]) -> bool {
    tokens.len() >= run.len() && tokens.iter().zip(run).all(|(a, b)| a == b)
}

/// The longest legal form at the end of the name (seat words after it are skipped), else at
/// its start. Something distinctive must be left.
fn strip_legal_form(tokens: &[String]) -> (Vec<String>, Option<&'static str>) {
    let mut end = tokens.len();
    while end > 1 && GEOGRAPHY.contains(&tokens[end - 1].as_str()) {
        end -= 1;
    }
    let mut best: Option<(usize, &'static str, bool)> = None;
    for form in LEGAL_FORMS {
        for run in form.variants {
            let longer = best.is_none_or(|(n, _, _)| run.len() > n);
            if run.len() < end && longer && ends_with(&tokens[..end], run) {
                best = Some((run.len(), form.canonical, true));
            }
        }
    }
    if best.is_none() {
        for form in LEGAL_FORMS {
            for run in form.variants {
                let longer = best.is_none_or(|(n, _, _)| run.len() > n);
                if run.len() < tokens.len() && longer && starts_with(tokens, run) {
                    best = Some((run.len(), form.canonical, false));
                }
            }
        }
    }
    match best {
        Some((n, form, true)) => (tokens[..end - n].to_vec(), Some(form)),
        Some((n, form, false)) => (tokens[n..].to_vec(), Some(form)),
        None => (tokens.to_vec(), None),
    }
}

/// Leaves out noise words, and geography words at the end (a seat), unless nothing else
/// would be left.
fn drop_words(tokens: &[String]) -> Vec<String> {
    let mut kept: Vec<String> = tokens
        .iter()
        .filter(|t| !NOISE.contains(&t.as_str()))
        .cloned()
        .collect();
    if kept.is_empty() {
        kept = tokens.to_vec();
    }
    while kept.len() > 1 && kept.last().is_some_and(|t| GEOGRAPHY.contains(&t.as_str())) {
        kept.pop();
    }
    kept
}

/// Whether the text names only a contract role ("Supplier", "Naručilac").
pub fn is_role_term(text: &str) -> bool {
    let t = tokens(&normalize(text));
    !t.is_empty() && t.iter().all(|w| ROLE_TERMS.contains(&w.as_str()))
}

/// Markers of an alias, after normalization, longest first.
const ALIAS_MARKERS: &[(&str, AliasKind)] = &[
    ("formerly known as", AliasKind::FormerName),
    ("previously known as", AliasKind::FormerName),
    ("now known as", AliasKind::NowKnownAs),
    ("doing business as", AliasKind::TradingName),
    ("hereinafter referred to as", AliasKind::DefinedTerm),
    ("hereinafter", AliasKind::DefinedTerm),
    ("u daljem tekstu", AliasKind::DefinedTerm),
    ("u daljnjem tekstu", AliasKind::DefinedTerm),
    ("trading as", AliasKind::TradingName),
    ("formerly", AliasKind::FormerName),
    ("prethodno", AliasKind::FormerName),
    ("ranije", AliasKind::FormerName),
    ("bivse", AliasKind::FormerName),
    ("fka", AliasKind::FormerName),
    ("nka", AliasKind::NowKnownAs),
    ("dba", AliasKind::TradingName),
    ("ta", AliasKind::TradingName),
];

/// The base name without bracketed text, and the aliases the brackets and inline statements
/// (`X d/b/a Y`, `X f/k/a Y`) hold.
pub fn split_aliases(surface: &str) -> (String, Vec<Alias>) {
    let mut base = String::with_capacity(surface.len());
    let mut aliases = Vec::new();
    let mut depth = 0usize;
    let mut inner = String::new();
    for c in surface.chars() {
        match c {
            '(' | '[' => {
                if depth == 0 {
                    inner.clear();
                } else {
                    inner.push(c);
                }
                depth += 1;
            }
            ')' | ']' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    if let Some(a) = bracket_alias(&inner) {
                        aliases.push(a);
                    }
                    base.push(' ');
                } else {
                    inner.push(c);
                }
            }
            _ if depth > 0 => inner.push(c),
            _ => base.push(c),
        }
    }
    // An unclosed bracket: its text stays out of the base too.
    if depth > 0
        && let Some(a) = bracket_alias(&inner)
    {
        aliases.push(a);
    }
    // Inline statements: "X d/b/a Y".
    let lower = base.to_lowercase();
    for (marker, kind) in [
        (" d/b/a ", AliasKind::TradingName),
        (" f/k/a ", AliasKind::FormerName),
        (" n/k/a ", AliasKind::NowKnownAs),
        (" t/a ", AliasKind::TradingName),
        (" trading as ", AliasKind::TradingName),
        (" formerly ", AliasKind::FormerName),
    ] {
        if let Some(at) = lower.find(marker)
            && base.is_char_boundary(at)
            && base.is_char_boundary(at + marker.len())
        {
            let other = clean_alias(&base[at + marker.len()..]);
            if !other.is_empty() {
                aliases.push(Alias { text: other, kind });
            }
            base.truncate(at);
            break;
        }
    }
    (
        base.split_whitespace().collect::<Vec<_>>().join(" "),
        aliases,
    )
}

fn clean_alias(s: &str) -> String {
    s.trim_matches(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                ':' | '"' | '\'' | '“' | '”' | '„' | '‘' | '’' | ',' | ';' | '-' | '–'
            )
    })
    .to_owned()
}

/// The alias a bracket holds: a marker and a name, or a bare annotation.
fn bracket_alias(inner: &str) -> Option<Alias> {
    let normalized = normalize(&inner.replace('/', ""));
    for (marker, kind) in ALIAS_MARKERS {
        if normalized == *marker || normalized.starts_with(&format!("{marker} ")) {
            // Cut the same number of words off the original text.
            let words = marker.split(' ').count();
            let text = skip_words(inner, words);
            let text = clean_alias(&text);
            return (!text.is_empty()).then_some(Alias { text, kind: *kind });
        }
    }
    let text = clean_alias(inner);
    (!text.is_empty()).then_some(Alias {
        text,
        kind: AliasKind::Annotation,
    })
}

/// `s` without its first `n` words (a word may hold `/`, as in `d/b/a`).
fn skip_words(s: &str, n: usize) -> String {
    let mut rest = s.trim_start();
    for _ in 0..n {
        rest = rest
            .trim_start_matches(|c: char| !c.is_whitespace())
            .trim_start();
    }
    rest.to_owned()
}

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
            IdKind::Lei => checksum::lei(&value),
            IdKind::Vat => {
                value.len() > 2
                    && value.is_char_boundary(2)
                    && value[..2].chars().all(|c| c.is_ascii_uppercase())
                    && matches!(
                        checksum::eu_vat(&value[..2], &value[2..]),
                        VatCheck::Valid | VatCheck::FormatOnly
                    )
            }
            IdKind::RsPib => checksum::rs_pib(&value),
            IdKind::RsMb => checksum::rs_mb(&value),
            IdKind::HrOib => checksum::hr_oib(&value),
            IdKind::Duns => value.len() == 9 && value.chars().all(|c| c.is_ascii_digit()),
            IdKind::Iban => checksum::iban(&value),
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

    fn key(s: &str) -> String {
        CompanyName::parse(s).map(|n| n.key).unwrap_or_default()
    }

    fn form(s: &str) -> Option<&'static str> {
        CompanyName::parse(s).and_then(|n| n.legal_form)
    }

    #[test]
    fn normalization_folds_case_marks_and_punctuation() {
        assert_eq!(normalize("U.K. Holdings"), "uk holdings");
        assert_eq!(normalize("  Čačak   Šped-Trans  "), "cacak spedtrans");
        assert_eq!(normalize("Straße Æon Øre Łódź"), "strasse aeon ore lodz");
        assert_eq!(normalize("Đorđević"), "djordjevic");
        assert_eq!(
            normalize("ＡＣＭＥ"),
            "acme",
            "NFKD folds full-width letters"
        );
    }

    #[test]
    fn cyrillic_and_latin_spellings_share_a_key() {
        assert_eq!(normalize("Ђорђевић"), normalize("Đorđević"));
        assert_eq!(key("Телеком Србија а.д."), key("Telekom Srbija a.d."));
        assert_eq!(
            key("Нафтна индустрија Србије а.д."),
            "naftna industrija srbije"
        );
        assert_eq!(key("ЖИТО ПРОМЕТ ДОО"), key("Žito Promet d.o.o."));
        assert_eq!(key("Чачанска пивара"), key("Čačanska pivara"));
        // Russian and Ukrainian letters too.
        assert_eq!(normalize("Яндекс"), "jandeks");
        assert_eq!(normalize("Київ"), "kijiv");
        // Macedonian.
        assert_eq!(normalize("Ќерка ѓ"), "kjerka gj");
    }

    #[test]
    fn legal_forms_are_taken_off_in_every_spelling() {
        for s in [
            "ACME d.o.o.",
            "ACME d. o. o.",
            "ACME DOO",
            "Acme д.о.о.",
            "ACME društvo sa ograničenom odgovornošću",
        ] {
            assert_eq!((key(s).as_str(), form(s)), ("acme", Some("d.o.o.")), "{s}");
        }
        assert_eq!(form("Beta a.d."), Some("a.d."));
        assert_eq!(form("Gama d.d."), Some("d.d."));
        assert_eq!(form("Elektro DOOEL Skopje"), Some("dooel"));
        assert_eq!(key("Elektro DOOEL Skopje"), "elektro");
        assert_eq!(form("Marko Marković PR Agencija Zeta"), None);
        assert_eq!(form("Agencija Zeta PR"), Some("PR"));
        assert_eq!(form("Janez Novak s.p."), Some("s.p."));
        assert_eq!(form("Kovač Sp. z o.o."), Some("Sp. z o.o."));
        assert_eq!(form("JP Elektroprivreda Srbije"), Some("j.p."));
        assert_eq!(key("JP Elektroprivreda Srbije"), "elektroprivreda srbije");
        assert_eq!(form("Alpha Widgets, LLC"), Some("LLC"));
        assert_eq!(form("Beta Components GmbH"), Some("GmbH"));
        assert_eq!(form("Gamma Holdings Limited"), Some("Ltd"));
        // A seat after the form is not part of the key.
        assert_eq!(key("ACME d.o.o. Beograd"), "acme");
        assert_eq!(key("ACME d.o.o. Novi Sad"), "acme");
        // A legal form alone is no company.
        assert!(CompanyName::parse("d.o.o.").is_none());
        assert!(CompanyName::parse("...").is_none());
    }

    #[test]
    fn geography_and_noise_never_empty_the_key() {
        assert_eq!(key("The Beograd Company d.o.o."), "beograd company");
        assert_eq!(key("Beograd d.o.o."), "beograd");
        assert_eq!(key("Srbija"), "srbija");
    }

    #[test]
    fn role_terms_are_never_companies() {
        assert!(CompanyName::parse("Supplier").is_none());
        assert!(CompanyName::parse("Naručilac").is_none());
        assert!(CompanyName::parse("НАРУЧИЛАЦ").is_none());
        assert!(CompanyName::parse("Ugovorne strane").is_none());
        assert!(is_role_term("Dobavljač"));
        assert!(!is_role_term("Dobavljač Plus"));
    }

    #[test]
    fn parentheses_become_aliases_and_leave_the_key() {
        let n = CompanyName::parse("Beta Trade d.o.o. (formerly Gamma Trade d.o.o.)").unwrap();
        assert_eq!(n.key, "beta trade");
        assert_eq!(n.legal_form, Some("d.o.o."));
        assert_eq!(
            n.aliases,
            [Alias {
                text: "Gamma Trade d.o.o.".into(),
                kind: AliasKind::FormerName
            }]
        );
        let n = CompanyName::parse("Alpha Ltd (f/k/a Omega Ltd)").unwrap();
        assert_eq!(n.aliases[0].kind, AliasKind::FormerName);
        assert_eq!(n.aliases[0].text, "Omega Ltd");
        let n = CompanyName::parse("Alpha Ltd (trading as \"AlphaShop\")").unwrap();
        assert_eq!(
            (n.aliases[0].kind, n.aliases[0].text.as_str()),
            (AliasKind::TradingName, "AlphaShop")
        );
        // A defined term that is a role term is dropped; the key is unaffected.
        let n = CompanyName::parse("ACME d.o.o. (u daljem tekstu: Naručilac)").unwrap();
        assert_eq!(n.key, "acme");
        assert!(n.aliases.is_empty());
        let n = CompanyName::parse("ACME d.o.o. (u daljem tekstu: \"ACME\")").unwrap();
        assert_eq!(
            n.aliases,
            [Alias {
                text: "ACME".into(),
                kind: AliasKind::DefinedTerm
            }]
        );
        let n = CompanyName::parse("Delta Inc. d/b/a Delta Foods").unwrap();
        assert_eq!(n.key, "delta");
        assert_eq!(
            (n.aliases[0].kind, n.aliases[0].text.as_str()),
            (AliasKind::TradingName, "Delta Foods")
        );
        let n = CompanyName::parse("Epsilon GmbH (n/k/a Zeta GmbH)").unwrap();
        assert_eq!(n.aliases[0].kind, AliasKind::NowKnownAs);
        let n = CompanyName::parse("Eta d.o.o. [Serbia]").unwrap();
        assert_eq!(n.aliases[0].kind, AliasKind::Annotation);
        assert_eq!(n.key, "eta");
    }

    #[test]
    fn ladder_keys_carry_the_form_except_the_blind_one() {
        let n = CompanyName::parse("Alfa Beta d.o.o.").unwrap();
        let keys: Vec<String> = n.ladder_keys().into_iter().map(|(_, k)| k).collect();
        assert_eq!(
            keys,
            [
                "name:alfa beta|d.o.o.",
                "squash:alfabeta|d.o.o.",
                "blind:alfa beta"
            ]
        );
        let other = CompanyName::parse("ALFA BETA a.d.").unwrap();
        assert_ne!(n.ladder_keys()[0].1, other.ladder_keys()[0].1);
        assert_eq!(n.ladder_keys()[2].1, other.ladder_keys()[2].1);
    }

    /// A PIB with its MOD 11,10 check digit.
    fn pib(head: &str) -> String {
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

    /// An LEI with its ISO 17442 (MOD 97-10) check digits.
    fn lei(head: &str) -> String {
        let mut r = 0u32;
        for c in format!("{head}00").chars() {
            let d = c.to_digit(36).unwrap();
            r = if d >= 10 {
                (r * 100 + d) % 97
            } else {
                (r * 10 + d) % 97
            };
        }
        format!("{head}{:02}", 98 - r)
    }

    #[test]
    fn identifiers_are_validated_and_compacted() {
        let p = pib("10123456");
        assert!(checksum::rs_pib(&p));
        let id = Identifier::parse(IdKind::RsPib, &p).unwrap();
        assert_eq!(id.alias(), format!("id:rs_pib:{p}"));
        let bad = format!("{}{}", &p[..8], (p.as_bytes()[8] - b'0' + 1) % 10);
        assert!(Identifier::parse(IdKind::RsPib, &bad).is_none());
        // Placeholders.
        assert!(Identifier::parse(IdKind::RsPib, "000000000").is_none());
        assert!(Identifier::parse(IdKind::Duns, "123456789").is_none());
        assert!(Identifier::parse(IdKind::Duns, "150483782").is_some());
        assert!(Identifier::parse(IdKind::Vat, "NOTAVATNUMBER").is_none());
        // MB, from quail's own example.
        assert!(Identifier::parse(IdKind::RsMb, "10000009").is_some());
        assert!(Identifier::parse(IdKind::RsMb, "10000008").is_none());
        // IBAN and LEI, compacted and uppercased first.
        let iban = Identifier::parse(IdKind::Iban, "de89 3704 0044 0532 0130 00").unwrap();
        assert_eq!(iban.value, "DE89370400440532013000");
        assert!(Identifier::parse(IdKind::Iban, "DE88370400440532013000").is_none());
        let lei = lei("5493001KJTIIGC8Y1R");
        assert!(Identifier::parse(IdKind::Lei, &lei).is_some(), "{lei}");
        let wrong = format!(
            "{}{:02}",
            &lei[..18],
            (lei[18..].parse::<u32>().unwrap() + 1) % 100
        );
        assert!(Identifier::parse(IdKind::Lei, &wrong).is_none());
        assert_eq!(IdKind::from_fact_key("signal.rs_pib"), Some(IdKind::RsPib));
        assert_eq!(IdKind::from_fact_key("signal.date"), None);
    }
}
