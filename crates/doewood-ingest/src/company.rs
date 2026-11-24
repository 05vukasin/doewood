//! Company identity, the deterministic part of ADR 0002 section 1, from `doewood-enrich`:
//! name normalization, legal forms, aliases from parentheses, identifiers, and the alias keys
//! the match ladder looks up. The rules live there; the store lookups that use them are in
//! [`crate::graph`].

pub use doewood_enrich::id::{IdKind, Identifier};
pub use doewood_enrich::name::{
    Alias, AliasKind, CompanyName, LEGAL_FORMS, LadderStep, LegalForm, is_role_term, normalize,
    split_aliases,
};

// The check-digit code the tests below assert on, as before the move.
#[cfg(test)]
use doewood_enrich::check as checksum;

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
