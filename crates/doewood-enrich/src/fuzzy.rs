//! The fuzzy name score and the jurisdiction veto (ADR 0002 section 1.4): ladder steps 10
//! (fuzzy token match) and 11 (fuzzy alias). A fuzzy match decides for one document only and
//! is never saved as an alias (section 1.3).
//!
//! The rules, in the ADR's words:
//!
//! - tokens are paired one to one, allowing one Damerau-Levenshtein edit on tokens of 5+
//!   characters; CJK tokens get no fuzziness;
//! - a subset match may add at most one token, and must share at least 2;
//! - otherwise Jaccard ≥ 0.6 is required;
//! - an overlap made only of typo matches is not enough;
//! - **veto:** if the names share at most one core word and are in different jurisdictions,
//!   they are different companies ("Bank of America" ≠ "Bank of China").
//!
//! The tokens are those of [`CompanyName::key`]: normalized, without the legal form,
//! annotations, seat and noise words. A *core word* is a key token that is not a function
//! word ([`FUNCTION_WORDS`]: `of`, `za`, `de`, ...), so "Bank of America" and "Bank of China"
//! share one core word, `bank`.

use crate::name::CompanyName;

/// Least token length (in characters) for a one-edit typo pair (ADR 0002 section 1.4).
pub const TYPO_MIN_CHARS: usize = 5;
/// Least Jaccard for a match that is not a subset (ADR 0002 section 1.4).
pub const MIN_JACCARD: f64 = 0.6;
/// Least shared tokens of a subset match (ADR 0002 section 1.4).
pub const SUBSET_MIN_SHARED: usize = 2;
/// Most tokens a subset match may add (ADR 0002 section 1.4).
pub const SUBSET_MAX_EXTRA: usize = 1;

/// Function words that are not core words for the veto. They stay in the key (the key is
/// the ladder's and does not change here); only the veto count skips them.
pub const FUNCTION_WORDS: &[&str] = &[
    "of", "the", "and", "for", "a", "an", "de", "du", "des", "la", "le", "les", "di", "da", "del",
    "der", "die", "das", "den", "von", "van", "und", "et", "y", "e", "i", "u", "za", "na", "od",
    "iz", "sa", "s", "z", "do", "pri",
];

/// The outcome of comparing two names.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FuzzyVerdict {
    /// The names match fuzzily.
    Match(FuzzyMatch),
    /// The rules are not met: no evidence either way.
    NoMatch,
    /// The jurisdiction veto: different companies.
    Veto,
}

/// How two names matched.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FuzzyMatch {
    /// Jaccard over the paired tokens: paired / (left + right - paired), in (0, 1].
    pub score: f64,
    /// Pairs of equal tokens.
    pub exact: usize,
    /// Pairs one Damerau-Levenshtein edit apart.
    pub typo: usize,
    /// One name's tokens are all paired and the other adds one.
    pub subset: bool,
}

/// Compares two parsed names. `left_country` and `right_country` are the jurisdictions
/// (ISO 3166-1 alpha-2) when known; the veto needs both, and different.
pub fn score(
    left: &CompanyName,
    left_country: Option<&str>,
    right: &CompanyName,
    right_country: Option<&str>,
) -> FuzzyVerdict {
    let a: Vec<&str> = left.key.split(' ').filter(|t| !t.is_empty()).collect();
    let b: Vec<&str> = right.key.split(' ').filter(|t| !t.is_empty()).collect();
    score_tokens(&a, left_country, &b, right_country)
}

/// [`score`] on key tokens.
pub fn score_tokens(
    left: &[&str],
    left_country: Option<&str>,
    right: &[&str],
    right_country: Option<&str>,
) -> FuzzyVerdict {
    if left.is_empty() || right.is_empty() {
        return FuzzyVerdict::NoMatch;
    }
    let pairs = pair(left, right);
    let different_places = matches!(
        (left_country, right_country),
        (Some(l), Some(r)) if !l.eq_ignore_ascii_case(r)
    );
    let shared_core = pairs
        .iter()
        .filter(|(i, j, _)| core(left[*i]) && core(right[*j]))
        .count();
    if different_places && shared_core <= 1 {
        return FuzzyVerdict::Veto;
    }
    let exact = pairs.iter().filter(|(_, _, typo)| !typo).count();
    let typo = pairs.len() - exact;
    if exact == 0 {
        // Only typo matches, or nothing in common.
        return FuzzyVerdict::NoMatch;
    }
    let paired = pairs.len();
    let (small, large) = if left.len() <= right.len() {
        (left.len(), right.len())
    } else {
        (right.len(), left.len())
    };
    let subset = paired == small && large > small;
    let ok = if subset {
        large - small <= SUBSET_MAX_EXTRA && paired >= SUBSET_MIN_SHARED
    } else {
        jaccard(paired, left.len(), right.len()) >= MIN_JACCARD
    };
    if !ok {
        return FuzzyVerdict::NoMatch;
    }
    FuzzyVerdict::Match(FuzzyMatch {
        score: jaccard(paired, left.len(), right.len()),
        exact,
        typo,
        subset,
    })
}

fn jaccard(paired: usize, left: usize, right: usize) -> f64 {
    let union = left + right - paired;
    // Token counts of a name are small: exact in f64.
    #[expect(clippy::cast_precision_loss, reason = "token counts are tiny")]
    let j = paired as f64 / union as f64;
    j
}

fn core(token: &str) -> bool {
    !FUNCTION_WORDS.contains(&token)
}

/// One-to-one pairs `(left index, right index, typo)`: equal tokens first, then the rest one
/// edit apart, each in order.
fn pair(left: &[&str], right: &[&str]) -> Vec<(usize, usize, bool)> {
    let mut used_l = vec![false; left.len()];
    let mut used_r = vec![false; right.len()];
    let mut out = Vec::new();
    for (i, l) in left.iter().enumerate() {
        if let Some(j) = (0..right.len()).find(|&j| !used_r[j] && right[j] == *l) {
            used_l[i] = true;
            used_r[j] = true;
            out.push((i, j, false));
        }
    }
    for (i, l) in left.iter().enumerate() {
        if used_l[i] {
            continue;
        }
        if let Some(j) = (0..right.len()).find(|&j| !used_r[j] && typo_pair(l, right[j])) {
            used_l[i] = true;
            used_r[j] = true;
            out.push((i, j, true));
        }
    }
    out
}

/// Two different tokens, both of 5+ characters and neither CJK, one Damerau-Levenshtein edit
/// apart.
pub fn typo_pair(a: &str, b: &str) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    a.len() >= TYPO_MIN_CHARS
        && b.len() >= TYPO_MIN_CHARS
        && !a.iter().chain(&b).any(|c| cjk(*c))
        && a != b
        && one_edit(&a, &b)
}

/// At most one insertion, deletion, substitution or transposition of neighbours.
fn one_edit(a: &[char], b: &[char]) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if long.len() - short.len() > 1 {
        return false;
    }
    let Some(i) = short.iter().zip(long).position(|(x, y)| x != y) else {
        // A common prefix: equal, or one character added at the end.
        return true;
    };
    if short.len() == long.len() {
        // Substitution, or a transposition of neighbours.
        short[i + 1..] == long[i + 1..]
            || (i + 1 < short.len()
                && short[i] == long[i + 1]
                && short[i + 1] == long[i]
                && short[i + 2..] == long[i + 2..])
    } else {
        // One character inserted into the longer one.
        short[i..] == long[i + 1..]
    }
}

/// Han, Hiragana, Katakana, Hangul and the CJK symbol blocks.
fn cjk(c: char) -> bool {
    matches!(c as u32,
        0x2E80..=0x2FDF
        | 0x3000..=0x30FF
        | 0x3100..=0x31FF
        | 0x3400..=0x4DBF
        | 0x4E00..=0x9FFF
        | 0xAC00..=0xD7AF
        | 0xF900..=0xFAFF
        | 0xFF66..=0xFF9F
        | 0x20000..=0x2FA1F)
}
