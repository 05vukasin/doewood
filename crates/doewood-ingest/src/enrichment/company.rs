//! One company's enrichment: the registries in jurisdiction order, driven by its identifiers
//! (or a name search without one), and what they say, as [`Findings`] for the gate and the
//! writer.
//!
//! The order (ADR 0002 section 2): the registry of the company's country first (APR for
//! Serbia, Companies House for the UK, EDGAR for the US, VIES for an EU VAT number), then the
//! other national registries it has an identifier for, then GLEIF, then Wikidata, the lowest
//! tier. What one registry gives can unlock another: GLEIF's `registeredAs` is a UK company
//! number or a Serbian MB, EDGAR gives an LEI. A registry is asked at most once per pass.

// `b` (the backend), `s` (the company) and `f` (the findings) are used throughout.
#![allow(clippy::many_single_char_names)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use doewood_core::InstanceId;
use doewood_enrich::{CompanyName, IdKind, Identifier, jurisdiction};
use doewood_model::{
    Backend, DueCompany, EdgeKind, EdgeState, Error, MERGE_CHAIN_MAX, ProposalRequest, Result,
};
use doewood_registry::apr::Company as AprCompany;
use doewood_registry::companies_house::{CompanyNumber, PscKind, Status as ChStatus};
use doewood_registry::edgar::Cik;
use doewood_registry::gleif::{
    EntityStatus, EventKind, LeiRecord, Parent, ParentKind, RelationshipStatus,
};
use doewood_registry::wikidata::{Period, QId, Relation, WikidataItem};
use doewood_registry::{Registry, Warning, WarningKind};
use serde_json::{Value, json};
use time::{Date, OffsetDateTime};
use tracing::{info, warn};
use uuid::Uuid;

use super::clients::Clients;
use super::gate::{self, NameCandidate};
use super::subject::{Subject, VIES_PREFIXES};
use super::{Outcome, due_registries, schedule_name};

/// How long a registry's answer is good for: its client's refresh (docs/registries.md, ADR
/// 0002 section 2). EDGAR's is the submissions' one day, standing in for "when new filings
/// appear" while its daily index feed is off ([`fed_refresh_interval`] when it is on).
pub fn refresh_interval(r: Registry) -> Duration {
    const DAY: u64 = 24 * 60 * 60;
    Duration::from_secs(match r {
        Registry::Gleif | Registry::Wikidata | Registry::Apr | Registry::CompaniesHouse => 30 * DAY,
        Registry::Vies => 90 * DAY,
        Registry::Edgar => DAY,
    })
}

/// The refresh with EDGAR's daily index feed on: new filings make the company due, so EDGAR
/// is refreshed every 30 days like the other official registries.
pub fn fed_refresh_interval(r: Registry) -> Duration {
    match r {
        Registry::Edgar => refresh_interval(Registry::Gleif),
        r => refresh_interval(r),
    }
}

/// Confidence of an official registry's edge (the registry identifier's reliability).
pub(crate) const OFFICIAL: f32 = 0.95;
/// Confidence of a Wikidata edge: the lowest tier, proposed only.
pub(crate) const COMMUNITY: f32 = 0.5;
/// Confidence of an Exhibit 21 row or an 8-K text match: best-effort, proposed only.
pub(crate) const BEST_EFFORT: f32 = 0.6;

/// Most Exhibit 21 rows, 8-K texts and Wikidata relations followed per company.
const MAX_ROWS: usize = 500;
const MAX_ACQUISITIONS: usize = 3;
const MAX_RELATIONS: usize = 10;

/// The other end of a planned edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Other {
    /// The company itself (`liquidated`, `renamed_to`).
    Itself,
    /// The company with this LEI; created when no company has it.
    Lei {
        lei: Identifier,
        name: Option<String>,
    },
    /// The company with this Companies House number; created when no company has it.
    Ch {
        number: CompanyNumber,
        name: Option<String>,
    },
    /// The company with this Wikidata item, if one has it (never created).
    Qid { qid: QId },
    /// A company that owns this name's keys, if one does (never created).
    Name { name: String },
}

/// An edge to write, from the enriched company's point of view.
#[derive(Clone, Debug)]
pub(crate) struct Planned {
    pub kind: EdgeKind,
    /// The company is the source (else the target).
    pub outgoing: bool,
    pub other: Other,
    pub valid_from: Option<Date>,
    pub valid_to: Option<Date>,
    pub state: EdgeState,
    pub confidence: f32,
    pub evidence: Value,
    pub props: Value,
}

/// A name the company had, with the day it stopped (the rename's date).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormerName {
    pub name: String,
    pub until: Option<Date>,
    pub evidence: Value,
}

/// An identifier a registry gave, and how sure the company is the record's.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Found {
    /// The alias (`id:lei:...`, `reg:companies_house:...`).
    pub alias: String,
    /// The record it came from.
    pub evidence: Value,
    /// `None`: the record was found by the company's own identifier; `Some(p)`: by a name
    /// search, adopted at fused probability `p`.
    pub by_name: Option<f64>,
}

/// What the registries said about one company.
#[derive(Debug, Default)]
pub(crate) struct Findings {
    /// Identifiers and registry ids.
    pub found: Vec<Found>,
    /// Legal names, official registries first.
    pub names: Vec<String>,
    /// Former names.
    pub former: Vec<FormerName>,
    /// Facts per registry, for the company's props (`registry.<name>`).
    pub facts: BTreeMap<&'static str, Value>,
    /// Edges.
    pub edges: Vec<Planned>,
    /// Official websites (Wikidata), as host names.
    pub websites: Vec<String>,
    /// Name-search candidates in the review band, for a person.
    pub proposals: Vec<ProposalRequest>,
    /// The warnings.
    pub warnings: Vec<Warning>,
    /// Each registry asked, and whether every request to it answered.
    pub asked: BTreeMap<&'static str, bool>,
    /// The jurisdiction a registry states, as one ISO 3166 code (EDGAR's state of
    /// incorporation, `US-CA`).
    pub jurisdiction: Option<String>,
}

impl Findings {
    fn warn(&mut self, w: &Warning) {
        if !matches!(w.kind, WarningKind::Disabled | WarningKind::NotConfigured) {
            warn!(registry = %w.registry, warning = %w.kind, "registry warning");
            self.warnings.push(w.clone());
            self.asked.insert(schedule_name(w.registry), false);
        }
    }

    fn asked(&mut self, r: Registry) {
        self.asked.entry(schedule_name(r)).or_insert(true);
    }

    fn found(&mut self, alias: String, evidence: &Value, by_name: Option<f64>) {
        if !self.found.iter().any(|f| f.alias == alias) {
            self.found.push(Found {
                alias,
                evidence: evidence.clone(),
                by_name,
            });
        }
    }

    fn name(&mut self, name: &str) {
        let name = name.trim();
        if !name.is_empty() && !self.names.iter().any(|n| n == name) {
            self.names.push(name.to_owned());
        }
    }

    /// The freshness schedule: each registry asked, at its refresh, or sooner after a
    /// failure.
    pub fn schedule(&self, retry: Duration, edgar_fed: bool) -> Vec<(String, Duration)> {
        let interval = if edgar_fed {
            fed_refresh_interval
        } else {
            refresh_interval
        };
        self.asked
            .iter()
            .map(|(r, ok)| {
                let every = if *ok {
                    registry_named(r).map_or(retry, interval)
                } else {
                    retry
                };
                ((*r).to_owned(), every)
            })
            .collect()
    }
}

fn registry_named(name: &str) -> Option<Registry> {
    [
        Registry::Gleif,
        Registry::Vies,
        Registry::Wikidata,
        Registry::Edgar,
        Registry::Apr,
        Registry::CompaniesHouse,
    ]
    .into_iter()
    .find(|r| r.as_str() == name)
}

/// The evidence of a registry record.
pub(crate) fn evidence(r: Registry, record: &str, fetched: OffsetDateTime) -> Value {
    json!({
        "registry": r.as_str(),
        "tier": r.tier().as_str(),
        "record": record,
        "fetched_at": fetched.date().to_string(),
    })
}

fn day(d: Option<Date>) -> Value {
    d.map_or(Value::Null, |d| Value::String(d.to_string()))
}

/// A parent edge pair: `parent_of` from the parent, `subsidiary_of` to it.
fn parent_pair(
    parent: &Other,
    from: Option<Date>,
    to: Option<Date>,
    state: EdgeState,
    confidence: f32,
    evidence: &Value,
    props: &Value,
) -> [Planned; 2] {
    let make = |kind, outgoing| Planned {
        kind,
        outgoing,
        other: parent.clone(),
        valid_from: from,
        valid_to: to,
        state,
        confidence,
        evidence: evidence.clone(),
        props: props.clone(),
    };
    [
        make(EdgeKind::ParentOf, false),
        make(EdgeKind::SubsidiaryOf, true),
    ]
}

/// A subsidiary edge pair: `parent_of` to the subsidiary, `subsidiary_of` from it.
fn subsidiary_pair(
    sub: &Other,
    from: Option<Date>,
    state: EdgeState,
    confidence: f32,
    evidence: &Value,
    props: &Value,
) -> [Planned; 2] {
    let make = |kind, outgoing| Planned {
        kind,
        outgoing,
        other: sub.clone(),
        valid_from: from,
        valid_to: None,
        state,
        confidence,
        evidence: evidence.clone(),
        props: props.clone(),
    };
    [
        make(EdgeKind::ParentOf, true),
        make(EdgeKind::SubsidiaryOf, false),
    ]
}

fn single(
    kind: EdgeKind,
    other: Other,
    from: Option<Date>,
    state: EdgeState,
    confidence: f32,
    evidence: &Value,
    props: Value,
) -> Planned {
    Planned {
        kind,
        outgoing: true,
        other,
        valid_from: from,
        valid_to: None,
        state,
        confidence,
        evidence: evidence.clone(),
        props,
    }
}

/// One registry query of a pass.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Step {
    Apr(Identifier),
    AprSearch,
    Ch(CompanyNumber),
    ChSearch,
    Edgar(Cik),
    EdgarSearch,
    Vies(Identifier),
    Gleif(Identifier),
    GleifSearch,
    Wikidata,
}

/// What [`Job::lease_other`] got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OtherLease {
    /// The lease is this job's now.
    Taken,
    /// Another task of this instance works on that company.
    Here,
    /// Another instance holds its lease.
    Elsewhere,
}

/// One company's enrichment, under its lease.
pub(crate) struct Job<'a> {
    pub backend: Arc<dyn Backend>,
    pub clients: &'a Clients,
    pub owner: &'a InstanceId,
    busy: &'a super::Busy,
    pub lease: Duration,
    pub retry: Duration,
    leased: Mutex<Vec<Uuid>>,
}

impl<'a> Job<'a> {
    pub fn new(
        backend: Arc<dyn Backend>,
        clients: &'a Clients,
        owner: &'a InstanceId,
        busy: &'a super::Busy,
        lease: Duration,
        retry: Duration,
    ) -> Self {
        Self {
            backend,
            clients,
            owner,
            busy,
            lease,
            retry,
            leased: Mutex::new(Vec::new()),
        }
    }

    /// The other companies leased for a merge.
    pub fn leased(&self) -> Vec<Uuid> {
        self.leased
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Takes the lease on another company (for a merge).
    pub async fn lease_other(&self, id: Uuid) -> Result<OtherLease> {
        if !super::take(self.busy, id) {
            return Ok(OtherLease::Here);
        }
        let ok = match self.backend.lease_company(id, self.owner, self.lease).await {
            Ok(ok) => ok,
            Err(e) => {
                super::give_back(self.busy, &[id]);
                return Err(e);
            }
        };
        if ok {
            self.leased
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(id);
            Ok(OtherLease::Taken)
        } else {
            super::give_back(self.busy, &[id]);
            Ok(OtherLease::Elsewhere)
        }
    }

    async fn renew(&self, id: Uuid) {
        if let Err(e) = self.backend.lease_company(id, self.owner, self.lease).await {
            warn!(company = %id, error = %e, "company lease not renewed");
        }
    }

    /// Enriches the company: registries, gate, writes, website, freshness.
    pub async fn run(&self, due: &DueCompany) -> Result<Outcome> {
        let b = self.backend.as_ref();
        let Some(mut subject) = Subject::load(b, due.company).await? else {
            return Ok(Outcome::default());
        };
        if subject.id() != due.company {
            // Merged away since it was listed: its survivor is enriched on its own.
            return Ok(Outcome::default());
        }
        let wanted = self.wanted(due).await?;
        let mut f = Findings::default();
        self.lookups(&mut subject, &mut f, wanted.as_ref()).await;
        let mut out = Outcome {
            warnings: f.warnings.len() as u64,
            ..Outcome::default()
        };
        self.renew(subject.id()).await;
        match self.gate_and_write(&mut subject, &f, &mut out).await {
            Ok(()) => Ok(out),
            // A person (or another instance) merged the company away while the job ran
            // (issue #62): its writes stop, nothing more lands on the husk, and the company
            // it was merged into is enriched again with what it has now.
            Err(Error::MergedAway { company, into }) => {
                let survivor = self.survivor_due(into).await?;
                info!(company = %company, survivor = %survivor, "company merged away during its enrichment; its survivor is due");
                out.merged_away = true;
                Ok(out)
            }
            Err(e) => Err(e),
        }
    }

    /// The gate, the writes, the website and the freshness of `subject`. The company's own
    /// writes fail with [`Error::MergedAway`] once it was merged away.
    async fn gate_and_write(
        &self,
        subject: &mut Subject,
        f: &Findings,
        out: &mut Outcome,
    ) -> Result<()> {
        let b = self.backend.as_ref();
        let gated = gate::run(self, subject, f).await?;
        out.merged = gated.merged;
        super::write::write(self, subject, f, &gated, out).await?;
        out.website = super::website::run(self, subject, f).await?;
        // A merge that waits for the other company's lease is done later (issue #60): the
        // pass runs the company again once its own tasks are done ([`OtherLease::Here`]),
        // and every registry is due again after the retry interval, as after a failure.
        let schedule = match gated.deferred {
            Some(lease) => {
                out.deferred = lease == OtherLease::Here;
                f.schedule(self.retry, self.clients.edgar_fed)
                    .into_iter()
                    .map(|(r, _)| (r, self.retry))
                    .collect()
            }
            None => f.schedule(self.retry, self.clients.edgar_fed),
        };
        b.record_enrichment(subject.id(), &schedule).await?;
        Ok(())
    }

    /// Makes the live company `into` stands for due for every enabled registry. Returns it.
    async fn survivor_due(&self, mut into: Uuid) -> Result<Uuid> {
        let b = self.backend.as_ref();
        let registries: Vec<String> = self
            .clients
            .enabled()
            .into_iter()
            .map(|r| r.as_str().to_owned())
            .collect();
        for _ in 0..MERGE_CHAIN_MAX {
            match b.mark_due(into, &registries).await {
                Ok(()) => return Ok(into),
                Err(Error::MergedAway { into: next, .. }) => into = next,
                Err(e) => return Err(e),
            }
        }
        Err(Error::Precondition(format!(
            "company {into} has more than {MERGE_CHAIN_MAX} merges in a chain"
        )))
    }

    /// Which registries to ask: all for a new or doubled company; else the due ones and any
    /// never asked before (switched on since, say).
    async fn wanted(&self, due: &DueCompany) -> Result<Option<BTreeSet<String>>> {
        let Some(mut set) = due_registries(due) else {
            return Ok(None);
        };
        let before = self.backend.enrichment(due.company).await?;
        for r in self.clients.enabled() {
            if before
                .as_ref()
                .is_none_or(|e| !e.next_due.contains_key(r.as_str()))
            {
                set.insert(r.as_str().to_owned());
            }
        }
        Ok(Some(set))
    }

    async fn lookups(&self, s: &mut Subject, f: &mut Findings, wanted: Option<&BTreeSet<String>>) {
        let mut done: Vec<Registry> = Vec::new();
        while let Some((r, step)) = self.next_step(s, &done, wanted) {
            done.push(r);
            f.asked(r);
            self.step(s, f, step).await;
        }
    }

    fn next_step(
        &self,
        s: &Subject,
        done: &[Registry],
        wanted: Option<&BTreeSet<String>>,
    ) -> Option<(Registry, Step)> {
        let mut order = Vec::new();
        match s.country.as_deref() {
            Some("RS") => order.push(Registry::Apr),
            Some("GB") => order.push(Registry::CompaniesHouse),
            Some("US") => order.push(Registry::Edgar),
            Some(c) if VIES_PREFIXES.contains(&c) || c == "GR" => order.push(Registry::Vies),
            _ => {}
        }
        for r in [
            Registry::Apr,
            Registry::CompaniesHouse,
            Registry::Edgar,
            Registry::Vies,
            Registry::Gleif,
            Registry::Wikidata,
        ] {
            if !order.contains(&r) {
                order.push(r);
            }
        }
        let country = s.country.as_deref();
        let named = s.name.is_some();
        order.into_iter().find_map(|r| {
            if done.contains(&r)
                || !self.clients.has(r)
                || wanted.is_some_and(|w| !w.contains(r.as_str()))
            {
                return None;
            }
            let step = match r {
                Registry::Apr => match s.id_of(IdKind::RsMb) {
                    Some(mb) => Some(Step::Apr(mb.clone())),
                    None => (country == Some("RS") && named).then_some(Step::AprSearch),
                },
                Registry::CompaniesHouse => match &s.ch {
                    Some(n) => Some(Step::Ch(n.clone())),
                    None => (country == Some("GB") && named).then_some(Step::ChSearch),
                },
                Registry::Edgar => match s.cik {
                    Some(c) => Some(Step::Edgar(c)),
                    None => (country == Some("US") && named).then_some(Step::EdgarSearch),
                },
                Registry::Vies => s
                    .ids
                    .iter()
                    .find(|i| {
                        i.kind == IdKind::Vat
                            && i.value
                                .get(..2)
                                .is_some_and(|p| VIES_PREFIXES.contains(&p) || p == "GR")
                    })
                    .map(|v| Step::Vies(v.clone())),
                Registry::Gleif => match s.id_of(IdKind::Lei) {
                    Some(l) => Some(Step::Gleif(l.clone())),
                    None => named.then_some(Step::GleifSearch),
                },
                Registry::Wikidata => {
                    (s.id_of(IdKind::Lei).is_some() || s.qid.is_some()).then_some(Step::Wikidata)
                }
            }?;
            Some((r, step))
        })
    }

    async fn step(&self, s: &mut Subject, f: &mut Findings, step: Step) {
        let result = match step {
            Step::Apr(mb) => self.apr(s, f, &mb).await,
            Step::AprSearch => self.apr_search(s, f).await,
            Step::Ch(n) => self.companies_house(s, f, &n, None).await,
            Step::ChSearch => self.ch_search(s, f).await,
            Step::Edgar(cik) => self.edgar(s, f, cik, None).await,
            Step::EdgarSearch => self.edgar_search(s, f).await,
            Step::Vies(vat) => self.vies(f, &vat).await,
            Step::Gleif(lei) => self.gleif(s, f, &lei).await,
            Step::GleifSearch => self.gleif_search(s, f).await,
            Step::Wikidata => self.wikidata(s, f).await,
        };
        if let Err(w) = result {
            f.warn(&w);
        }
    }

    // --- APR ------------------------------------------------------------------------------

    async fn apr(&self, s: &mut Subject, f: &mut Findings, mb: &Identifier) -> Result<(), Warning> {
        let Some(apr) = &self.clients.apr else {
            return Ok(());
        };
        let got = apr.company(&mb.value).await?;
        match got.value {
            Some(c) => Self::apr_record(s, f, &c, got.fetched_at, None),
            None => {
                f.facts
                    .insert("apr", json!({"found": false, "mb": mb.value}));
            }
        }
        Ok(())
    }

    async fn apr_search(&self, s: &mut Subject, f: &mut Findings) -> Result<(), Warning> {
        let (Some(apr), Some(name)) = (&self.clients.apr, s.name.clone()) else {
            return Ok(());
        };
        let got = apr.search(&name.original).await?;
        let candidates: Vec<NameCandidate> = got
            .value
            .iter()
            .map(|c| NameCandidate {
                name: c.name.clone(),
                country: Some("RS".to_owned()),
                record: format!("companies/{}", c.mb.value),
            })
            .collect();
        if let Some((i, p)) = gate::pick(s, &candidates, Registry::Apr, f) {
            let c = &got.value[i];
            Self::apr_record(s, f, c, got.fetched_at, Some(p));
        }
        Ok(())
    }

    fn apr_record(
        s: &mut Subject,
        f: &mut Findings,
        c: &AprCompany,
        fetched: OffsetDateTime,
        by_name: Option<f64>,
    ) {
        let ev = evidence(Registry::Apr, &format!("companies/{}", c.mb.value), fetched);
        f.found(c.mb.alias(), &ev, by_name);
        s.ids.insert(c.mb.clone());
        s.country.get_or_insert_with(|| "RS".to_owned());
        f.name(&c.name);
        f.facts.insert(
            "apr",
            json!({
                "found": true,
                "mb": c.mb.value,
                "name": c.name,
                "status": c.status.as_str(),
                "legal_form": c.legal_form.canonical(),
                "founded": day(c.founded),
                "as_of": day(c.as_of),
                "fetched_at": fetched.date().to_string(),
            }),
        );
        if c.status.is_winding_up() {
            // The open data gives no date of the status change.
            f.edges.push(single(
                EdgeKind::Liquidated,
                Other::Itself,
                None,
                EdgeState::Active,
                OFFICIAL,
                &ev,
                json!({"status": c.status.as_str(), "as_of": day(c.as_of)}),
            ));
        }
    }

    // --- Companies House ------------------------------------------------------------------

    async fn ch_search(&self, s: &mut Subject, f: &mut Findings) -> Result<(), Warning> {
        let (Some(ch), Some(name)) = (&self.clients.companies_house, s.name.clone()) else {
            return Ok(());
        };
        let got = ch.search(&name.original).await?;
        let candidates: Vec<NameCandidate> = got
            .value
            .iter()
            .map(|h| NameCandidate {
                name: h.name.clone(),
                country: Some("GB".to_owned()),
                record: format!("company/{}", h.number.as_str()),
            })
            .collect();
        if let Some((i, p)) = gate::pick(s, &candidates, Registry::CompaniesHouse, f) {
            let n = got.value[i].number.clone();
            return self.companies_house(s, f, &n, Some(p)).await;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn companies_house(
        &self,
        s: &mut Subject,
        f: &mut Findings,
        number: &CompanyNumber,
        by_name: Option<f64>,
    ) -> Result<(), Warning> {
        let Some(ch) = &self.clients.companies_house else {
            return Ok(());
        };
        let n = number.as_str();
        let got = ch.company(n).await?;
        let record = format!("company/{n}");
        let Some(c) = got.value else {
            f.facts
                .insert("companies_house", json!({"found": false, "number": n}));
            return Ok(());
        };
        let ev = evidence(Registry::CompaniesHouse, &record, got.fetched_at);
        f.found(super::subject::ch_alias(number), &ev, by_name);
        s.ch = Some(number.clone());
        s.country.get_or_insert_with(|| "GB".to_owned());
        f.name(&c.name);
        for p in &c.previous_names {
            f.former.push(FormerName {
                name: p.name.clone(),
                until: p.to,
                evidence: ev.clone(),
            });
        }
        let mut facts = json!({
            "found": true,
            "number": n,
            "name": c.name,
            "status": format!("{:?}", c.status).to_lowercase(),
            "status_detail": c.status_detail,
            "type": c.company_type,
            "created": day(c.created),
            "ceased": day(c.ceased),
            "conversion": c.is_conversion(),
            "fetched_at": got.fetched_at.date().to_string(),
        });
        match c.status {
            ChStatus::Dissolved | ChStatus::Liquidation => f.edges.push(single(
                EdgeKind::Liquidated,
                Other::Itself,
                c.ceased,
                EdgeState::Active,
                OFFICIAL,
                &ev,
                json!({"status": facts["status"]}),
            )),
            _ => {}
        }
        // Corporate PSCs with majority control are parents (natural persons never are).
        match ch.significant_control(n).await {
            Ok(list) => {
                let ev = evidence(
                    Registry::CompaniesHouse,
                    &format!("{record}/persons-with-significant-control"),
                    list.fetched_at,
                );
                for p in &list.value.items {
                    if !matches!(p.kind, PscKind::CorporateEntity | PscKind::LegalPerson)
                        || !majority(&p.natures_of_control)
                    {
                        continue;
                    }
                    let uk = p.registration.as_ref().and_then(|r| {
                        let country = r.country.as_deref().unwrap_or("").to_ascii_lowercase();
                        let kind = r.kind.as_deref().unwrap_or("").to_ascii_lowercase();
                        let british = [
                            "england",
                            "wales",
                            "scotland",
                            "northern ireland",
                            "united kingdom",
                            "uk",
                            "great britain",
                        ]
                        .iter()
                        .any(|c| country.contains(c))
                            || kind.starts_with("uk-");
                        british
                            .then(|| r.number.as_deref().and_then(CompanyNumber::parse))
                            .flatten()
                    });
                    let other = match (uk, &p.name) {
                        (Some(number), name) => Other::Ch {
                            number,
                            name: name.clone(),
                        },
                        (None, Some(name)) => Other::Name { name: name.clone() },
                        (None, None) => continue,
                    };
                    f.edges.extend(parent_pair(
                        &other,
                        p.notified,
                        p.ceased,
                        EdgeState::Active,
                        OFFICIAL,
                        &ev,
                        &json!({"level": "direct", "natures_of_control": p.natures_of_control}),
                    ));
                }
            }
            Err(w) => f.warn(&w),
        }
        match ch.filing_events(n).await {
            Ok(list) => {
                let events: Vec<Value> = list
                    .value
                    .items
                    .iter()
                    .filter(|e| {
                        !matches!(
                            e.event,
                            doewood_registry::companies_house::FilingEvent::NameChange
                        )
                    })
                    .take(20)
                    .map(|e| {
                        json!({
                            "event": format!("{:?}", e.event).to_lowercase(),
                            "date": day(e.action_date.or(e.date)),
                            "form": e.form,
                            "transaction": e.transaction_id,
                        })
                    })
                    .collect();
                facts["events"] = Value::Array(events);
            }
            Err(w) => f.warn(&w),
        }
        f.facts.insert("companies_house", facts);
        Ok(())
    }

    // --- EDGAR ----------------------------------------------------------------------------

    async fn edgar_search(&self, s: &mut Subject, f: &mut Findings) -> Result<(), Warning> {
        let (Some(edgar), Some(name)) = (&self.clients.edgar, s.name.clone()) else {
            return Ok(());
        };
        let got = edgar.search(&name.original).await?;
        let candidates: Vec<NameCandidate> = got
            .value
            .iter()
            .map(|t| NameCandidate {
                name: t.title.clone(),
                country: Some("US".to_owned()),
                record: format!("submissions/CIK{}", t.cik),
            })
            .collect();
        if let Some((i, p)) = gate::pick(s, &candidates, Registry::Edgar, f) {
            let cik = got.value[i].cik;
            return self.edgar(s, f, cik, Some(p)).await;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn edgar(
        &self,
        s: &mut Subject,
        f: &mut Findings,
        cik: Cik,
        by_name: Option<f64>,
    ) -> Result<(), Warning> {
        let Some(edgar) = &self.clients.edgar else {
            return Ok(());
        };
        let got = edgar.company(&cik.to_string()).await?;
        let record = format!("submissions/CIK{cik}");
        let Some(c) = got.value else {
            f.facts
                .insert("edgar", json!({"found": false, "cik": cik.to_string()}));
            return Ok(());
        };
        let ev = evidence(Registry::Edgar, &record, got.fetched_at);
        f.found(super::subject::cik_alias(cik), &ev, by_name);
        s.cik = Some(cik);
        f.name(&c.name);
        if let Some(lei) = &c.lei {
            f.found(lei.alias(), &ev, by_name);
            s.ids.insert(lei.clone());
        }
        for p in &c.former_names {
            f.former.push(FormerName {
                name: p.name.clone(),
                until: p.to,
                evidence: ev.clone(),
            });
        }
        let acquisitions: Vec<&doewood_registry::edgar::Filing> = c.acquisitions().collect();
        // `CA` here is California (`US-CA`), never Canada (issue #201).
        let jurisdiction = c
            .state_of_incorporation
            .as_deref()
            .and_then(jurisdiction::from_sec);
        if f.jurisdiction.is_none() {
            f.jurisdiction.clone_from(&jurisdiction);
        }
        f.facts.insert(
            "edgar",
            json!({
                "found": true,
                "cik": cik.to_string(),
                "name": c.name,
                "tickers": c.tickers,
                "state_of_incorporation": c.state_of_incorporation,
                "jurisdiction": jurisdiction,
                "sic": c.sic,
                "acquisitions": acquisitions.iter().take(20).map(|a| json!({
                    "accession": a.accession.plain(),
                    "filed": day(a.filing_date),
                })).collect::<Vec<_>>(),
                "fetched_at": got.fetched_at.date().to_string(),
            }),
        );
        // Exhibit 21 rows are best-effort: proposed edges to companies the graph knows.
        let subs = match edgar.subsidiaries(&cik.to_string()).await {
            Ok(x) => x.value,
            Err(w) => {
                f.warn(&w);
                None
            }
        };
        let mut rows: Vec<CompanyName> = Vec::new();
        if let Some(x) = &subs {
            let ev = evidence(
                Registry::Edgar,
                &format!("{}/{}", x.accession.plain(), x.document),
                got.fetched_at,
            );
            for row in x.subsidiaries.iter().take(MAX_ROWS) {
                let Some(parsed) = CompanyName::parse(&row.name) else {
                    continue;
                };
                f.edges.extend(subsidiary_pair(
                    &Other::Name {
                        name: row.name.clone(),
                    },
                    x.filing_date,
                    EdgeState::Proposed,
                    BEST_EFFORT,
                    &ev,
                    &json!({
                        "exhibit": x.exhibit_type,
                        "jurisdiction": row.jurisdiction.as_deref().and_then(jurisdiction::from_sec),
                        "jurisdiction_as_written": row.jurisdiction,
                    }),
                ));
                rows.push(parsed);
            }
        }
        // 8-K Item 2.01: an acquisition of a company that is now a subsidiary, when the
        // item's text names it (best-effort, proposed).
        for a in acquisitions.iter().take(MAX_ACQUISITIONS) {
            if rows.is_empty() {
                break;
            }
            let text = match edgar.acquisition_text(cik, a).await {
                Ok(t) => t.value,
                Err(w) => {
                    f.warn(&w);
                    continue;
                }
            };
            let Some(text) = text else { continue };
            let hay = format!(" {} ", doewood_enrich::normalize(&text));
            let ev = evidence(Registry::Edgar, a.accession.plain(), got.fetched_at);
            for row in &rows {
                if row.key.len() >= 6 && hay.contains(&format!(" {} ", row.key)) {
                    f.edges.push(single(
                        EdgeKind::Acquired,
                        Other::Name {
                            name: row.original.clone(),
                        },
                        a.filing_date,
                        EdgeState::Proposed,
                        BEST_EFFORT,
                        &ev,
                        json!({"item": "2.01", "form": a.form}),
                    ));
                }
            }
        }
        Ok(())
    }

    // --- VIES -----------------------------------------------------------------------------

    async fn vies(&self, f: &mut Findings, vat: &Identifier) -> Result<(), Warning> {
        let Some(vies) = &self.clients.vies else {
            return Ok(());
        };
        let (country, number) = vat.value.split_at(2);
        let got = vies.check(country, number).await?;
        let r = got.value;
        if r.valid
            && let Some(name) = &r.name
        {
            f.name(name);
        }
        f.facts.insert(
            "vies",
            json!({
                "vat": vat.value,
                "valid": r.valid,
                "name": r.name,
                "checked": day(r.request_date),
                "fetched_at": got.fetched_at.date().to_string(),
            }),
        );
        Ok(())
    }

    // --- GLEIF ----------------------------------------------------------------------------

    async fn gleif_search(&self, s: &mut Subject, f: &mut Findings) -> Result<(), Warning> {
        let (Some(gleif), Some(name)) = (&self.clients.gleif, s.name.clone()) else {
            return Ok(());
        };
        let got = gleif.search(&name.original, s.country.as_deref()).await?;
        let candidates: Vec<NameCandidate> = got
            .value
            .iter()
            .map(|r| NameCandidate {
                name: r.legal_name.clone(),
                country: r.legal_country.clone().or_else(|| {
                    r.jurisdiction
                        .as_deref()
                        .and_then(super::subject::country_code)
                }),
                record: format!("lei-records/{}", r.lei.value),
            })
            .collect();
        if let Some((i, p)) = gate::pick(s, &candidates, Registry::Gleif, f) {
            let rec = got.value[i].clone();
            return self.gleif_record(s, f, &rec, got.fetched_at, Some(p)).await;
        }
        Ok(())
    }

    async fn gleif(
        &self,
        s: &mut Subject,
        f: &mut Findings,
        lei: &Identifier,
    ) -> Result<(), Warning> {
        let Some(gleif) = &self.clients.gleif else {
            return Ok(());
        };
        let got = gleif.lookup(&lei.value).await?;
        if let Some(rec) = got.value {
            return self.gleif_record(s, f, &rec, got.fetched_at, None).await;
        }
        f.facts
            .insert("gleif", json!({"found": false, "lei": lei.value}));
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn gleif_record(
        &self,
        s: &mut Subject,
        f: &mut Findings,
        rec: &LeiRecord,
        fetched: OffsetDateTime,
        by_name: Option<f64>,
    ) -> Result<(), Warning> {
        let Some(gleif) = &self.clients.gleif else {
            return Ok(());
        };
        let lei = &rec.lei.value;
        let record = format!("lei-records/{lei}");
        let ev = evidence(Registry::Gleif, &record, fetched);
        f.found(rec.lei.alias(), &ev, by_name);
        s.ids.insert(rec.lei.clone());
        f.name(&rec.legal_name);
        let country = rec
            .jurisdiction
            .as_deref()
            .and_then(super::subject::country_code)
            .or_else(|| rec.legal_country.clone());
        if s.country.is_none() {
            s.country.clone_from(&country);
        }
        // `registeredAs`: the national registry's id, when it is one doewood can check.
        if let Some(reg) = rec.registered_as.as_deref() {
            match country.as_deref() {
                Some("GB") => {
                    if let Some(n) = CompanyNumber::parse(reg) {
                        f.found(super::subject::ch_alias(&n), &ev, by_name);
                        s.ch.get_or_insert(n);
                    }
                }
                Some("RS") => {
                    if let Some(mb) = Identifier::parse(IdKind::RsMb, reg) {
                        f.found(mb.alias(), &ev, by_name);
                        s.ids.insert(mb);
                    }
                }
                _ => {}
            }
        }
        // Former names; a single completed rename gives the date.
        let renames: Vec<_> = rec.renames().collect();
        let previous: Vec<&str> = rec
            .other_names
            .iter()
            .filter(|n| n.kind.as_deref() == Some("PREVIOUS_LEGAL_NAME"))
            .map(|n| n.name.as_str())
            .collect();
        for name in &previous {
            f.former.push(FormerName {
                name: (*name).to_owned(),
                until: if previous.len() == 1 && renames.len() == 1 {
                    renames[0].effective
                } else {
                    None
                },
                evidence: ev.clone(),
            });
        }
        // Successions: merged into the successor.
        for succ in rec.successions() {
            let other = match (&succ.successor.lei, &succ.successor.name) {
                (Some(l), name) => Other::Lei {
                    lei: l.clone(),
                    name: name.clone(),
                },
                (None, Some(name)) => Other::Name { name: name.clone() },
                (None, None) => continue,
            };
            if matches!(&other, Other::Lei { lei: l, .. } if l == &rec.lei) {
                continue;
            }
            f.edges.push(single(
                EdgeKind::MergedInto,
                other,
                succ.date,
                EdgeState::Active,
                OFFICIAL,
                &ev,
                json!({"event": succ.event.map(|e| format!("{e:?}"))}),
            ));
        }
        // Closure: a completed liquidation-type event, or an entity dissolved.
        let closure = rec
            .events
            .iter()
            .filter(|e| e.kind == EventKind::Closure && e.is_completed())
            .max_by_key(|e| e.effective);
        let dissolved = rec.entity_status == EntityStatus::Inactive
            && rec
                .expiration
                .as_ref()
                .is_some_and(|x| x.reason.as_deref() == Some("DISSOLVED"));
        if closure.is_some() || dissolved {
            let date = closure
                .and_then(|e| e.effective)
                .or_else(|| rec.expiration.as_ref().and_then(|x| x.date));
            f.edges.push(single(
                EdgeKind::Liquidated,
                Other::Itself,
                date,
                EdgeState::Active,
                OFFICIAL,
                &ev,
                json!({"event": closure.map(|e| e.code.clone()), "dissolved": dissolved}),
            ));
        }
        let mut facts = json!({
            "found": true,
            "lei": lei,
            "name": rec.legal_name,
            "jurisdiction": rec.jurisdiction,
            "registered_as": rec.registered_as,
            "category": rec.category,
            "entity_status": format!("{:?}", rec.entity_status).to_lowercase(),
            "registration_status": rec.registration_status,
            "merged": rec.is_merged(),
            "fetched_at": fetched.date().to_string(),
        });
        // Level 2: the direct parent, and the ultimate one when it is another entity.
        let mut direct: Option<Identifier> = None;
        for kind in [ParentKind::Direct, ParentKind::Ultimate] {
            let got = match gleif.parent(lei, kind).await {
                Ok(g) => g,
                Err(w) => {
                    f.warn(&w);
                    continue;
                }
            };
            let key = format!("{}_parent", kind.as_str());
            match got.value {
                Parent::Reported(rel) => {
                    if kind == ParentKind::Ultimate && direct.as_ref() == Some(&rel.parent) {
                        facts[&key] = json!({"lei": rel.parent.value, "same_as_direct": true});
                        continue;
                    }
                    if kind == ParentKind::Direct {
                        direct = Some(rel.parent.clone());
                    }
                    let name = match gleif.lookup(&rel.parent.value).await {
                        Ok(p) => p.value.map(|r| r.legal_name),
                        Err(w) => {
                            f.warn(&w);
                            None
                        }
                    };
                    let ev = evidence(
                        Registry::Gleif,
                        &format!("{record}/{}-parent-relationship", kind.as_str()),
                        got.fetched_at,
                    );
                    let state = match rel.status {
                        RelationshipStatus::Inactive => EdgeState::Superseded,
                        _ => EdgeState::Active,
                    };
                    f.edges.extend(parent_pair(
                        &Other::Lei {
                            lei: rel.parent.clone(),
                            name,
                        },
                        rel.valid_from,
                        rel.valid_to,
                        state,
                        OFFICIAL,
                        &ev,
                        &json!({"level": kind.as_str(), "registration_status": rel.registration_status}),
                    ));
                    facts[&key] = json!({"lei": rel.parent.value, "from": day(rel.valid_from)});
                }
                Parent::Exception(x) => {
                    facts[&key] = json!({
                        "exception": x.code,
                        "parent_exists": x.reason.parent_exists(),
                    });
                }
                Parent::NotReported => {
                    facts[&key] = Value::Null;
                }
            }
        }
        f.facts.insert("gleif", facts);
        Ok(())
    }

    // --- Wikidata -------------------------------------------------------------------------

    async fn wikidata(&self, s: &mut Subject, f: &mut Findings) -> Result<(), Warning> {
        let Some(wd) = &self.clients.wikidata else {
            return Ok(());
        };
        let (item, fetched) = if let Some(lei) = s.id_of(IdKind::Lei).cloned() {
            let got = wd.by_lei(&lei.value).await?;
            (got.value.into_iter().next(), got.fetched_at)
        } else if let Some(q) = s.qid {
            let got = wd.item(&q.to_string()).await?;
            (got.value, got.fetched_at)
        } else {
            return Ok(());
        };
        let Some(item) = item else {
            f.facts.insert("wikidata", json!({"found": false}));
            return Ok(());
        };
        Self::wikidata_item(s, f, &item, fetched);
        Ok(())
    }

    fn wikidata_item(
        s: &mut Subject,
        f: &mut Findings,
        item: &WikidataItem,
        fetched: OffsetDateTime,
    ) {
        let ev = evidence(Registry::Wikidata, &item.qid.to_string(), fetched);
        // The item is found by an identifier the company owns (its LEI) or by its own id:
        // still the lowest tier, so it never counts as a registry identifier (the gate's
        // evidence is the LEI it was found by).
        f.found(super::subject::qid_alias(item.qid), &ev, None);
        s.qid = Some(item.qid);
        let props = |r: &Relation, what: &str| json!({"level": "wikidata", "property": what, "label": r.label, "preferred": r.preferred, "tier": "community"});
        let mut n = 0;
        for (what, list) in [
            ("parent_organization", &item.parent_organizations),
            ("owned_by", &item.owned_by),
        ] {
            for r in list.iter().filter(|r| !r.human) {
                if n == MAX_RELATIONS || r.target == item.qid {
                    continue;
                }
                n += 1;
                let (from, to) = period(&r.period);
                f.edges.extend(parent_pair(
                    &Other::Qid { qid: r.target },
                    from,
                    to,
                    EdgeState::Proposed,
                    COMMUNITY,
                    &ev,
                    &props(r, what),
                ));
            }
        }
        for (what, list) in [
            ("replaced_by", &item.replaced_by),
            ("followed_by", &item.followed_by),
        ] {
            for r in list.iter().take(MAX_RELATIONS) {
                if r.human || r.target == item.qid {
                    continue;
                }
                let (from, _) = period(&r.period);
                f.edges.push(single(
                    EdgeKind::MergedInto,
                    Other::Qid { qid: r.target },
                    from,
                    EdgeState::Proposed,
                    COMMUNITY,
                    &ev,
                    props(r, what),
                ));
            }
        }
        for w in &item.websites {
            if let Some(host) = w.url.host_str() {
                let host = host.trim_start_matches("www.").to_ascii_lowercase();
                if !f.websites.contains(&host) {
                    f.websites.push(host);
                }
            }
        }
        f.facts.insert(
            "wikidata",
            json!({
                "found": true,
                "qid": item.qid.to_string(),
                "label": item.label,
                "websites": item.websites.iter().map(|w| w.url.as_str()).collect::<Vec<_>>(),
                "tier": "community",
                "fetched_at": fetched.date().to_string(),
            }),
        );
    }
}

fn period(p: &Period) -> (Option<Date>, Option<Date>) {
    (p.start.map(|d| d.date), p.end.map(|d| d.date))
}

/// Whether a PSC's natures of control make it a parent: more than half the shares or votes,
/// or the right to appoint and remove the directors.
fn majority(natures: &[String]) -> bool {
    natures.iter().any(|n| {
        n.contains("50-to-75-percent")
            || n.contains("75-to-100-percent")
            || n.starts_with("right-to-appoint-and-remove-directors")
    })
}
