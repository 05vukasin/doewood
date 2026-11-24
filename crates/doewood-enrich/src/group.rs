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
