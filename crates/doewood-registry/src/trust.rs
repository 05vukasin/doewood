//! How far a registry's facts are trusted (ADR 0002 sections 2 and 3).

use crate::warning::Registry;

/// The trust tier of a registry's answers, for the enrichment job to weigh them by. Ordered
/// from the lowest: `Community < Official`.
///
/// - [`TrustTier::Official`]: an authoritative registry (GLEIF, VIES, SEC EDGAR, and later APR
///   and Companies House). A validated identifier from one is the strongest evidence (ADR 0002
///   section 1.6, "registry identifier").
/// - [`TrustTier::Community`]: Wikidata, community-edited: the lowest tier of section 2. Its
///   facts fill gaps; an official registry's fact wins over it, and it is never on its own a
///   "registry identifier" hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrustTier {
    /// Community-edited (Wikidata): the lowest tier.
    Community,
    /// An authoritative registry.
    Official,
}

impl TrustTier {
    /// The name in logs and props.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Community => "community",
            Self::Official => "official",
        }
    }
}

impl Registry {
    /// The trust tier of this registry's answers.
    pub fn tier(self) -> TrustTier {
        match self {
            Self::Gleif | Self::Vies | Self::Edgar => TrustTier::Official,
            Self::Wikidata => TrustTier::Community,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wikidata_is_the_lowest_tier() {
        assert!(TrustTier::Community < TrustTier::Official);
        assert_eq!(Registry::Wikidata.tier(), TrustTier::Community);
        for r in [Registry::Gleif, Registry::Vies, Registry::Edgar] {
            assert!(Registry::Wikidata.tier() < r.tier(), "{r}");
        }
    }
}
