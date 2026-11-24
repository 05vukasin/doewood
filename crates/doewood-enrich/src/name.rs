//! Company names: normalization, Latin/Cyrillic keys, legal forms, noise and geography words,
//! contract role terms and aliases from parentheses (ADR 0002 sections 1.1 and 1.2).
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
        #[expect(
            clippy::cast_possible_truncation,
            reason = "two-decimal constants, the nearest f32"
        )]
        let c = self.reliability() as f32;
        c
    }

    /// [`Self::confidence`] as the `f64` evidence fusion works in
    /// ([`crate::fusion::Evidence::ladder`]).
    pub fn reliability(self) -> f64 {
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
