//! The company being enriched, as the job sees it: its name, jurisdiction and identifiers
//! (from the aliases it owns, so only identifiers no other company holds), and the registry
//! ids earlier passes stored.

use std::collections::BTreeSet;

use doewood_enrich::{CompanyName, IdKind, Identifier};
use doewood_model::{Backend, Node, NodeKind, Result, merged_into};
use doewood_registry::companies_house::CompanyNumber;
use doewood_registry::edgar::Cik;
use doewood_registry::wikidata::QId;
use serde_json::Value;
use uuid::Uuid;

/// Most `merged_into` hops followed.
const MAX_HOPS: usize = 64;

/// The alias of a Companies House company number.
pub(crate) fn ch_alias(number: &CompanyNumber) -> String {
    format!("reg:companies_house:{}", number.as_str())
}

/// The alias of an EDGAR CIK.
pub(crate) fn cik_alias(cik: Cik) -> String {
    format!("reg:edgar:{cik}")
}

/// The alias of a Wikidata item.
pub(crate) fn qid_alias(qid: QId) -> String {
    format!("reg:wikidata:{qid}")
}

/// The alias of a verified website domain.
pub(crate) fn domain_alias(domain: &str) -> String {
    format!("domain:{domain}")
}

/// A company.
#[derive(Clone, Debug)]
pub(crate) struct Subject {
    /// The node.
    pub node: Node,
    /// Its name, parsed.
    pub name: Option<CompanyName>,
    /// Its jurisdiction (ISO 3166-1 alpha-2, uppercase), when known.
    pub country: Option<String>,
    /// The identifiers it owns.
    pub ids: BTreeSet<Identifier>,
    /// Its Companies House number, when known.
    pub ch: Option<CompanyNumber>,
    /// Its EDGAR CIK, when known.
    pub cik: Option<Cik>,
    /// Its Wikidata item, when known.
    pub qid: Option<QId>,
}

impl Subject {
    /// The live company `id` stands for (following `merged_into`), with what it owns.
    pub async fn load(b: &dyn Backend, mut id: Uuid) -> Result<Option<Self>> {
        let mut node = None;
        for _ in 0..MAX_HOPS {
            let Some(n) = b.node(id).await? else {
                return Ok(None);
            };
            if let Some(next) = merged_into(&n) {
                id = next;
            } else {
                node = Some(n);
                break;
            }
        }
        let Some(node) = node.filter(|n| n.kind == NodeKind::Company) else {
            return Ok(None);
        };
        let mut s = Self {
            name: node
                .props
                .get("name")
                .and_then(Value::as_str)
                .and_then(CompanyName::parse),
            country: node
                .props
                .get("jurisdiction")
                .and_then(Value::as_str)
                .and_then(country_code),
            node,
            ids: BTreeSet::new(),
            ch: None,
            cik: None,
            qid: None,
        };
        for a in b.aliases_of(s.node.id).await? {
            s.learn_alias(&a.alias);
        }
        if s.country.is_none() {
            s.country = s.implied_country();
        }
        Ok(Some(s))
    }

    /// The node id.
    pub fn id(&self) -> Uuid {
        self.node.id
    }

    fn learn_alias(&mut self, alias: &str) {
        if let Some(rest) = alias.strip_prefix("id:")
            && let Some((kind, value)) = rest.split_once(':')
            && let Some(kind) = IdKind::ALL.iter().find(|k| k.as_str() == kind)
            && let Some(id) = Identifier::parse(*kind, value)
        {
            self.ids.insert(id);
        } else if let Some(n) = alias.strip_prefix("reg:companies_house:") {
            self.ch = CompanyNumber::parse(n);
        } else if let Some(n) = alias.strip_prefix("reg:edgar:") {
            self.cik = Cik::parse(n);
        } else if let Some(n) = alias.strip_prefix("reg:wikidata:") {
            self.qid = QId::parse(n);
        }
    }

    /// The first identifier of `kind`.
    pub fn id_of(&self, kind: IdKind) -> Option<&Identifier> {
        self.ids.iter().find(|i| i.kind == kind)
    }

    /// The country its identifiers imply: Serbian ids, an OIB, a VAT prefix, a UK number.
    fn implied_country(&self) -> Option<String> {
        if self.id_of(IdKind::RsMb).is_some() || self.id_of(IdKind::RsPib).is_some() {
            return Some("RS".to_owned());
        }
        if self.id_of(IdKind::HrOib).is_some() {
            return Some("HR".to_owned());
        }
        if let Some(v) = self.id_of(IdKind::Vat) {
            return country_code(&v.value[..2])
                .map(|c| if c == "EL" { "GR".to_owned() } else { c });
        }
        self.ch.as_ref().map(|_| "GB".to_owned())
    }
}

/// The first two-letter code in `raw` (`RS`, `rs`, `RS,ME`), uppercase.
pub(crate) fn country_code(raw: &str) -> Option<String> {
    raw.split(|c: char| !c.is_ascii_alphabetic())
        .find(|t| t.len() == 2)
        .map(str::to_ascii_uppercase)
}

/// The EU member states (and Northern Ireland) VIES answers for, as VAT prefixes.
pub(crate) const VIES_PREFIXES: &[&str] = &[
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT",
    "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK", "XI",
];
