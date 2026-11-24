//! Stable company group ids (ADR 0002 section 1.7):
//! `group_id = uuid5(NAMESPACE, normalized LEI)`, or the APR MB, or another national registry
//! id, so merges are idempotent across instances and batches.
//!
//! - The LEI is hashed as it is (compact, uppercase: 20 characters). Other registry ids are
//!   hashed as `<kind>:<value>` ([`IdKind::as_str`]), because two kinds can share a value
//!   (a PIB and a DUNS are both nine digits).
//! - Strongest first: LEI, then MB, then PIB, OIB and VAT. IBAN and DUNS are not registry ids
//!   and never key a group.
//! - A *different* LEI never splits a group (branches have their own LEIs): the id is chosen
//!   once, when the group is created; later identifiers join it and do not re-key it. That is
//!   the store's job; this module only computes the id.

use uuid::Uuid;

use crate::id::{IdKind, Identifier};

/// The namespace of company group ids: `uuid5(NAMESPACE_URL, NAMESPACE_NAME)`.
pub const NAMESPACE: Uuid = Uuid::from_u128(0x54ff_e879_25bc_52f5_b60f_9380_88ea_7bc7);

/// The URL [`NAMESPACE`] is derived from.
pub const NAMESPACE_NAME: &str = "https://github.com/05vukasin/doewood/company-group";

/// The kinds that key a group, strongest first.
pub const GROUP_KINDS: [IdKind; 5] = [
    IdKind::Lei,
    IdKind::RsMb,
    IdKind::RsPib,
    IdKind::HrOib,
    IdKind::Vat,
];

/// The group id of one registry identifier; `None` for a kind that never keys a group.
pub fn group_id(id: &Identifier) -> Option<Uuid> {
    if !GROUP_KINDS.contains(&id.kind) {
        return None;
    }
    let name = if id.kind == IdKind::Lei {
        id.value.clone()
    } else {
        format!("{}:{}", id.kind.as_str(), id.value)
    };
    Some(Uuid::new_v5(&NAMESPACE, name.as_bytes()))
}

/// The group id of a company from its strongest registry identifier ([`GROUP_KINDS`] order;
/// within a kind, the smallest value, so the order of `ids` does not matter).
pub fn group_id_of(ids: &[Identifier]) -> Option<Uuid> {
    GROUP_KINDS.iter().find_map(|kind| {
        ids.iter()
            .filter(|i| i.kind == *kind)
            .min_by(|a, b| a.value.cmp(&b.value))
            .and_then(group_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testids::{iban, lei, mb, mod_11_10};

    fn id(kind: IdKind, raw: &str) -> Identifier {
        Identifier::parse(kind, raw).unwrap_or_else(|| panic!("{raw} should be a {kind:?}"))
    }

    #[test]
    fn namespace_is_derived_from_its_name() {
        assert_eq!(
            NAMESPACE,
            Uuid::new_v5(&Uuid::NAMESPACE_URL, NAMESPACE_NAME.as_bytes())
        );
    }

    #[test]
    fn group_ids_are_stable() {
        let l = id(IdKind::Lei, &lei("8945002TQV7RMXEW3K"));
        let g = group_id(&l).unwrap();
        assert_eq!(g.get_version_num(), 5);
        assert_eq!(g, Uuid::new_v5(&NAMESPACE, l.value.as_bytes()));
        // Pinned (cross-checked with Python's uuid.uuid5): a change here would split every
        // group already stored.
        assert_eq!(g.to_string(), "714ed58a-db87-5b32-b394-4de4d6ce8577");
        let m = id(IdKind::RsMb, &mb("2108734"));
        assert_eq!(
            group_id(&m).unwrap().to_string(),
            "9240447e-dec6-52c8-866a-108889ccbad6"
        );
        // The same LEI written differently gives the same id.
        let written = l.value.to_lowercase();
        let again = id(
            IdKind::Lei,
            &format!("{} {}", &written[..10], &written[10..]),
        );
        assert_eq!(group_id(&again), Some(g));
    }

    #[test]
    fn strongest_registry_id_keys_the_group() {
        let l = id(IdKind::Lei, &lei("8945002TQV7RMXEW3K"));
        let m = id(IdKind::RsMb, &mb("2108734"));
        let p = id(IdKind::RsPib, &mod_11_10("10873452"));
        let account = id(IdKind::Iban, &iban("RS", "265000000837164209"));
        let duns = id(IdKind::Duns, "604817352");
        let all = [
            account.clone(),
            p.clone(),
            m.clone(),
            l.clone(),
            duns.clone(),
        ];
        assert_eq!(group_id_of(&all), group_id(&l));
        let mut reversed = all.clone();
        reversed.reverse();
        assert_eq!(group_id_of(&reversed), group_id(&l));
        assert_eq!(group_id_of(&[p.clone(), m.clone()]), group_id(&m));
        assert_eq!(group_id_of(std::slice::from_ref(&p)), group_id(&p));
        // IBAN and DUNS never key a group.
        assert_eq!(group_id(&account), None);
        assert_eq!(group_id(&duns), None);
        assert_eq!(group_id_of(&[account, duns]), None);
        assert_eq!(group_id_of(&[]), None);
        // Two LEIs (a branch has its own): the smaller one, whatever the order.
        let other = id(IdKind::Lei, &lei("254900PRIMER0000A1"));
        let first = group_id_of(&[l.clone(), other.clone()]);
        assert_eq!(first, group_id_of(&[other.clone(), l.clone()]));
        let smaller = if l.value <= other.value { &l } else { &other };
        assert_eq!(first, group_id(smaller));
    }

    #[test]
    fn kinds_with_the_same_digits_get_different_ids() {
        // A nine-digit PIB and the same digits as a VAT body are different names.
        let pib = mod_11_10("10873452");
        let p = id(IdKind::RsPib, &pib);
        let v = id(IdKind::Vat, &format!("RS{pib}"));
        assert_ne!(group_id(&p), group_id(&v));
        assert_eq!(
            group_id(&p),
            Some(Uuid::new_v5(&NAMESPACE, format!("rs_pib:{pib}").as_bytes()))
        );
    }
}
