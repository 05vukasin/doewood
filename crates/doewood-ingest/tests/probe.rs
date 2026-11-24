//! Prints what the analyzer and the fact reader make of the generated inputs. Run with
//! `cargo test -p doewood-ingest --test probe -- --ignored --nocapture`.

mod common;

use doewood_ingest::facts::DocFacts;
use doewood_ingest::{Analyzer, AnalyzerConfig};

#[test]
#[ignore = "prints, asserts nothing"]
fn probe() {
    let a = Analyzer::new(&AnalyzerConfig::default()).unwrap();
    let p1 = common::pib("10123456");
    let contract = [
        "UGOVOR O ISPORUCI br. 114/23".to_owned(),
        format!("Dobavljac: ACME Trade d.o.o. Beograd, PIB {p1}, MB 10000009"),
        "Narucilac: Beta Gradnja a.d. Novi Sad".to_owned(),
        "Ugovor stupa na snagu 01.03.2024. Ukupno: 120.000,00 RSD.".to_owned(),
    ];
    let lines: Vec<&str> = contract.iter().map(String::as_str).collect();
    let amendment = [
        "ANEKS br. 2 uz Ugovor o isporuci br. 114/23",
        "Dobavljac: АЦМЕ Трејд д.о.о. Београд",
        "Narucilac: Beta Gradnja a.d.",
    ];
    let inputs: Vec<(&str, Vec<u8>)> = vec![
        ("contract.pdf", common::pdf(&lines, "Ugovor o isporuci")),
        ("aneks.docx", common::docx("Aneks br. 2", &amendment)),
        (
            "invoice.txt",
            format!("INVOICE No. INV-7/2026\nSupplier: Alpha Widgets LLC\nCustomer: Beta Components GmbH\nPIB {p1}\nTotal: EUR 4,200.50\n").into_bytes(),
        ),
        (
            "prices.xlsx",
            common::xlsx("Prices", &[&["Item", "Qty", "Price"], &["Bolts", "10", "2.5"], &["Nuts", "20", "1.25"]]),
        ),
        ("a.txt", common::prose(7, 400).into_bytes()),
    ];
    for (name, bytes) in inputs {
        let r = a.analyze(bytes, name);
        let d = DocFacts::from_record(&r.body);
        println!("== {name}: route {:?} outcome {:?}", r.route, r.outcome);
        println!("   status {}", r.body["status"]);
        println!("   versions {:?}", r.versions);
        println!(
            "   title {:?} kind {:?} date {:?} amount {:?}",
            d.title, d.kind, d.date, d.amount
        );
        println!("   numbers {:?} cites {:?}", d.numbers, d.cites);
        for p in &d.parties {
            println!(
                "   party {:?} form {:?} roles {:?} ids {:?} score {}",
                p.surface, p.legal_form, p.roles, p.ids, p.score
            );
        }
        println!("   minhash {}", d.minhash.as_ref().map_or(0, |m| m.1.len()));
        let keys: Vec<&str> = r.body["facts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|f| f["key"].as_str())
            .collect();
        println!("   keys {keys:?}");
    }
}
