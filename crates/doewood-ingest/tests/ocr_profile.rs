//! OCR per scanned page (issue #116): where a page's time goes, how many cores OCR keeps busy,
//! and whether the output changes with the engine settings doewood could pass to quail.
//!
//! Every test here is ignored: each needs the OCR model files (`DOEWOOD_OCR_MODELS`, as the
//! server finds them; it skips without them) and minutes of CPU. Run one test, one variant at a
//! time, in release, through the build lock, and read the printed report:
//!
//! ```sh
//! DOEWOOD_OCR_MODELS=/path/to/ocr-models OCR_THREADS=4 serial timeout 600 \
//!   cargo test --release -p doewood-ingest --test ocr_profile -- --ignored --nocapture --exact stages
//! ```
//!
//! - `stages`: one engine, page by page: decode of the embedded scan, the engine call, and its
//!   parts (page orientation, detection, line orientation, recognition), found by switching a
//!   part off or reading a blank page of the same size. `OCR_THREADS` (default: what doewood
//!   plans for one worker), `OCR_PAGES` (default 4).
//! - `analyzer_workers`: doewood's own [`Analyzer`] with `OCR_WORKERS` workers (default 1) on
//!   `OCR_FILES` files (default 8) of `OCR_FILE_PAGES` pages (default 1), `OCR_JOBS` as
//!   `DOEWOOD_OCR_JOBS` (default unset): pages/s, CPU use, peak RSS, and quail's step timings
//!   from the records.
//! - `engines`: `OCR_JOBS` engines (default 1) with `OCR_THREADS` threads each (default
//!   `cpus / jobs`) reading `OCR_PAGES` pages (default 8) side by side: what other OCR
//!   job and thread counts would give.
//! - `same_output`: the engine's output for each page (text, confidences, boxes, as JSON) is
//!   byte-identical for 1, 2 and 4 threads, and the analyzer's records (without timings) for
//!   1 and 4 workers and `DOEWOOD_OCR_JOBS` unset, 2 and 4. Any setting doewood changes must
//!   pass it. `OCR_PAGES` (default 3).
//!
//! The scans are invented invoice text (Helvetica 11 pt, about 1500 characters a page)
//! rendered at 300 DPI to 8-bit gray, US Letter (2550 x 3300), Flate, as image-only PDFs, as in
//! the #63 measurement (docs/deploy.md, Sizing).
//!
//! CPU time is the process's `utime + stime` from `/proc/self/stat` (in 1/100 s, the Linux
//! default `USER_HZ`), sampled every 250 ms; "busy threads" are threads whose CPU time grew
//! by at least 5 % of the wall time, among those alive at the end (the worker threads the
//! test spawns have exited by then; ONNX Runtime's and rayon's pools have not); peak RSS is
//! `VmHWM`, reset through `/proc/self/clear_refs` before each measured part.
#![cfg(feature = "ocr")]
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

mod common;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use doewood_ingest::{Analyzer, AnalyzerConfig, OcrState};
use doewood_model::canonical::to_canonical;
use lopdf::content::{Content, Operation};
use lopdf::{Document, Object, Stream, dictionary};
use quail_core::{Blob, Image, Limits, PageRaster};
use quail_ocr::paddle::{PaddleConfig, PaddleFactory};
use quail_ocr::{OcrEngine, OcrHints, OcrPage, OcrResources, ScriptHint};
use quail_pdf::PdfRaster;

/// US Letter, points.
const W: f32 = 612.0;
const H: f32 = 792.0;
/// Render resolution of the scans.
const DPI: f32 = 300.0;
/// Linux `USER_HZ`: `/proc` CPU times are in these ticks.
const TICKS_PER_S: f64 = 100.0;

/// The model directory when every model file is there.
fn models() -> Option<PathBuf> {
    let Some(dir) = std::env::var_os("DOEWOOD_OCR_MODELS").map(PathBuf::from) else {
        eprintln!("DOEWOOD_OCR_MODELS not set; skipping");
        return None;
    };
    let missing = quail_ocr::models::missing(&dir);
    if missing.is_empty() {
        Some(dir)
    } else {
        eprintln!(
            "{} OCR model file(s) missing in {}; skipping",
            missing.len(),
            dir.display()
        );
        None
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn cpus() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

// ---------------------------------------------------------------------------------------------
// Inputs

/// Invented invoice text of one page: a header and about 24 lines of words, numbers and
/// amounts, about 1500 characters.
fn page_lines(seed: u64) -> Vec<String> {
    let mut x = seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let prose = common::prose(seed, 400);
    let mut words = prose.split_whitespace();
    let mut lines = vec![
        format!("INVOICE No. {}-{:04}", 2026, next() % 10_000),
        format!(
            "Date: 2026-{:02}-{:02}   Customer ID: {:06}",
            next() % 12 + 1,
            next() % 28 + 1,
            next() % 1_000_000
        ),
        String::new(),
    ];
    for i in 0..24 {
        let mut line = String::new();
        while line.len() < 52 {
            let Some(w) = words.next() else { break };
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(w);
        }
        if i % 3 == 0 {
            line = format!("{:>2}. {line}", i / 3 + 1);
        }
        if i % 2 == 0 {
            let (units, cents) = (next() % 9000 + 10, next() % 100);
            line = format!("{line}  {units}.{cents:02} EUR");
        }
        lines.push(line);
    }
    lines
}

/// The page of `seed` as an 8-bit gray image at [`DPI`]: a text PDF rendered by quail's own
/// rasterizer.
fn scan_image(seed: u64) -> Image {
    let lines = page_lines(seed);
    let refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let text = common::pdf(&refs, "scan source");
    let img = PdfRaster::new(Blob::from(text))
        .render(0, DPI, &Limits::default())
        .expect("render the text page");
    assert_eq!(img.channels, 1);
    img
}

/// A white page of the same size as a scan.
fn blank_image() -> Image {
    let (w, h) = ((W / 72.0 * DPI) as u32, (H / 72.0 * DPI) as u32);
    Image {
        width: w,
        height: h,
        channels: 1,
        data: vec![255; (w * h) as usize],
    }
}

/// An image-only PDF, one page per image (8-bit gray, Flate), each image covering its page.
fn scan_pdf(images: &[Image]) -> Vec<u8> {
    let mut doc = Document::with_version("1.7");
    let pages_id = doc.new_object_id();
    let mut kids: Vec<Object> = Vec::new();
    for img in images {
        let mut stream = Stream::new(
            dictionary! {
                "Type" => "XObject",
                "Subtype" => "Image",
                "Width" => i64::from(img.width),
                "Height" => i64::from(img.height),
                "ColorSpace" => "DeviceGray",
                "BitsPerComponent" => 8,
            },
            img.data.clone(),
        );
        stream.compress().expect("deflate the scan");
        let image_id = doc.add_object(stream);
        let ops = vec![
            Operation::new("q", vec![]),
            Operation::new(
                "cm",
                vec![W.into(), 0.into(), 0.into(), H.into(), 0.into(), 0.into()],
            ),
            Operation::new("Do", vec!["Im1".into()]),
            Operation::new("Q", vec![]),
        ];
        let content = Content { operations: ops };
        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
        let leaf = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
            "Resources" => dictionary! { "XObject" => dictionary! { "Im1" => image_id } },
        });
        kids.push(leaf.into());
    }
    let count = i64::try_from(kids.len()).unwrap();
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => kids,
            "Count" => count,
            "MediaBox" => vec![0.into(), 0.into(), W.into(), H.into()],
        }),
    );
    let catalog_id = doc.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
    doc.trailer.set("Root", catalog_id);
    let mut out = Vec::new();
    doc.save_to(&mut out).unwrap();
    out
}

/// `files` scanned PDFs of `pages` pages each; page seeds are distinct across files.
fn scan_files(files: usize, pages: usize) -> Vec<Vec<u8>> {
    (0..files)
        .map(|f| {
            let imgs: Vec<Image> = (0..pages)
                .map(|p| scan_image((f * pages + p) as u64 + 1))
                .collect();
            scan_pdf(&imgs)
        })
        .collect()
}

fn hints() -> OcrHints {
    OcrHints {
        script: ScriptHint::Auto,
        dpi: DPI,
    }
}

// ---------------------------------------------------------------------------------------------
// Process measurements

/// `utime + stime` of a `/proc/.../stat` line, in ticks.
fn stat_ticks(stat: &str) -> Option<u64> {
    // Fields after the command name (which may hold spaces): state is the first.
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    Some(f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?)
}

fn process_ticks() -> u64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| stat_ticks(&s))
        .unwrap_or(0)
}

/// CPU ticks of every thread of the process.
fn thread_ticks() -> HashMap<u64, u64> {
    let mut out = HashMap::new();
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else {
        return out;
    };
    for e in dir.flatten() {
        let Some(tid) = e.file_name().to_str().and_then(|s| s.parse().ok()) else {
            continue;
        };
        if let Some(t) = std::fs::read_to_string(e.path().join("stat"))
            .ok()
            .and_then(|s| stat_ticks(&s))
        {
            out.insert(tid, t);
        }
    }
    out
}

/// A `kB` field of `/proc/self/status`, in MiB.
fn status_mib(field: &str) -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<f64>().ok())
        .map_or(0.0, |kib| kib / 1024.0)
}

fn threads_now() -> usize {
    std::fs::read_dir("/proc/self/task").map_or(0, Iterator::count)
}

/// Resets `VmHWM` to the current RSS.
fn reset_peak() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// What a measured part used.
struct Usage {
    wall: Duration,
    /// CPU seconds of the process.
    cpu: f64,
    /// Cores busy, sampled every 250 ms.
    busy: Vec<f64>,
    /// Most threads alive.
    threads_max: usize,
    /// Threads whose CPU time grew by at least 5 % of the wall time.
    busy_threads: usize,
    /// RSS before, and the peak during.
    rss_before: f64,
    rss_peak: f64,
}

impl Usage {
    /// Mean cores busy over the part.
    fn cores(&self) -> f64 {
        self.cpu / self.wall.as_secs_f64().max(1e-9)
    }

    fn busy_quantile(&self, q: f64) -> f64 {
        let mut b = self.busy.clone();
        if b.is_empty() {
            return self.cores();
        }
        b.sort_by(f64::total_cmp);
        b[((b.len() - 1) as f64 * q).round() as usize]
    }

    fn report(&self, what: &str) {
        println!(
            "{what}: wall {:.2} s, CPU {:.2} s, cores busy mean {:.2} of {} (p10 {:.2}, p50 {:.2}, \
             p90 {:.2}), threads {} at most, {} busy, RSS {:.0} MiB before, peak {:.0} MiB (+{:.0})",
            self.wall.as_secs_f64(),
            self.cpu,
            self.cores(),
            cpus(),
            self.busy_quantile(0.1),
            self.busy_quantile(0.5),
            self.busy_quantile(0.9),
            self.threads_max,
            self.busy_threads,
            self.rss_before,
            self.rss_peak,
            self.rss_peak - self.rss_before,
        );
    }
}

/// Runs `f` and measures it.
fn measure<R>(f: impl FnOnce() -> R) -> (R, Usage) {
    reset_peak();
    let rss_before = status_mib("VmRSS");
    let threads_before = thread_ticks();
    let stop = AtomicBool::new(false);
    let samples = Mutex::new((Vec::new(), 0usize));
    let start = Instant::now();
    let t0 = process_ticks();
    let r = std::thread::scope(|s| {
        s.spawn(|| {
            let mut last = (Instant::now(), process_ticks());
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(250));
                let now = (Instant::now(), process_ticks());
                let dt = now.0.duration_since(last.0).as_secs_f64();
                let mut g = samples.lock().unwrap();
                g.0.push((now.1 - last.1) as f64 / TICKS_PER_S / dt.max(1e-9));
                g.1 = g.1.max(threads_now());
                last = now;
            }
        });
        let r = f();
        stop.store(true, Ordering::Relaxed);
        r
    });
    let wall = start.elapsed();
    let cpu = (process_ticks() - t0) as f64 / TICKS_PER_S;
    let threshold = wall.as_secs_f64() * 0.05 * TICKS_PER_S;
    let busy_threads = thread_ticks()
        .iter()
        .filter(|(tid, t)| {
            let before = threads_before.get(tid).copied().unwrap_or(0);
            (**t - before.min(**t)) as f64 >= threshold
        })
        .count();
    let (busy, threads_max) = samples.into_inner().unwrap();
    let usage = Usage {
        wall,
        cpu,
        busy,
        threads_max: threads_max.max(threads_now()),
        busy_threads,
        rss_before,
        rss_peak: status_mib("VmHWM"),
    };
    (r, usage)
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

// ---------------------------------------------------------------------------------------------
// Engine and analyzer

fn paddle(threads: usize) -> PaddleConfig {
    PaddleConfig {
        intra_threads: threads,
        ..PaddleConfig::default()
    }
}

/// The engine's output as JSON text: the bytes compared between settings.
fn page_json(p: &OcrPage) -> String {
    serde_json::to_string(p).unwrap()
}

/// Decodes page 0 of each scanned PDF the way OCR gets it (the embedded scan).
fn decode(files: &[Vec<u8>]) -> Vec<(Image, Duration)> {
    files
        .iter()
        .map(|f| {
            let t = Instant::now();
            let img = PdfRaster::new(Blob::from(f.clone()))
                .embedded_scan(0, &Limits::default())
                .expect("a scan")
                .expect("decoded");
            (img, t.elapsed())
        })
        .collect()
}

fn analyzer(dir: PathBuf, workers: usize, ocr_jobs: Option<usize>) -> Analyzer {
    let a = Analyzer::new(&AnalyzerConfig {
        workers,
        cpus: workers.min(cpus()),
        ocr_models: dir,
        ocr_jobs,
        ..AnalyzerConfig::default()
    })
    .unwrap();
    assert!(
        matches!(a.ocr(), OcrState::Ready { .. }),
        "OCR not ready: {:?}",
        a.ocr()
    );
    a
}

/// A record without its timings, as stored text: what must not change.
fn record_text(a: &Analyzer, bytes: Vec<u8>) -> (String, BTreeMap<String, u64>, i64) {
    let mut parts = a.analyze_record(bytes, "scan.pdf");
    let mut steps = BTreeMap::new();
    for t in &parts.record.timings {
        *steps.entry(t.src.to_string()).or_default() += t.micros;
    }
    let ocr_pages = parts
        .record
        .facts_for("ocr.pages")
        .find_map(|f| match f.value {
            quail_core::Value::Int(i) => Some(i),
            _ => None,
        })
        .unwrap_or(0);
    parts.record.timings.clear();
    (
        to_canonical(&parts.record).unwrap().get().to_owned(),
        steps,
        ocr_pages,
    )
}

// ---------------------------------------------------------------------------------------------
// Tests

/// One engine, page by page, by stage.
#[test]
#[ignore = "measurement (issue #116): needs DOEWOOD_OCR_MODELS, minutes of CPU"]
fn stages() {
    let Some(dir) = models() else { return };
    let threads = env_usize("OCR_THREADS").unwrap_or_else(|| OcrResources::auto(1).threads);
    let n = env_usize("OCR_PAGES").unwrap_or(4).max(1);
    let files = scan_files(n, 1);
    println!(
        "{n} single-page scans, {:.0} KiB each on average; {threads} ONNX Runtime thread(s), {} CPUs",
        files.iter().map(Vec::len).sum::<usize>() as f64 / n as f64 / 1024.0,
        cpus()
    );
    let (images, decode_usage) = measure(|| decode(&files));
    let decode_ms: Vec<f64> = images.iter().map(|(_, d)| d.as_secs_f64() * 1e3).collect();
    decode_usage.report("decode (parse + inflate the embedded scan)");
    println!("  decode per page: median {:.0} ms", median(decode_ms));
    let blank = blank_image();

    let variants: [(&str, PaddleConfig); 3] = [
        ("default", paddle(threads)),
        (
            "no page orientation",
            PaddleConfig {
                doc_orientation: false,
                ..paddle(threads)
            },
        ),
        (
            "no line orientation",
            PaddleConfig {
                line_orientation: false,
                ..paddle(threads)
            },
        ),
    ];
    let mut medians = BTreeMap::new();
    for (name, cfg) in variants {
        let factory = PaddleFactory::new(&dir, cfg).unwrap();
        let (mut engine, load) = measure(|| factory.engine().unwrap());
        load.report(&format!("[{name}] engine load"));
        // The first page grows the ONNX Runtime arenas.
        let (_, first) = measure(|| engine.recognize(&images[0].0, &hints()).unwrap());
        first.report(&format!("[{name}] first page (cold)"));
        let mut walls = Vec::new();
        let mut cpu = Vec::new();
        let mut chars = 0;
        let ((), all) = measure(|| {
            for (img, _) in &images {
                let t = Instant::now();
                let c = process_ticks();
                let p = engine.recognize(img, &hints()).unwrap();
                walls.push(t.elapsed().as_secs_f64());
                cpu.push((process_ticks() - c) as f64 / TICKS_PER_S);
                chars += p.lines.iter().map(|l| l.text.len()).sum::<usize>();
            }
        });
        all.report(&format!("[{name}] {n} pages (warm)"));
        let (_, b) = measure(|| engine.recognize(&blank, &hints()).unwrap());
        b.report(&format!("[{name}] blank page"));
        let (w, c) = (median(walls), median(cpu));
        println!(
            "  [{name}] per page: median {w:.2} s wall, {c:.2} CPU s ({:.2} cores); blank page \
             {:.2} s; {:.0} characters a page",
            c / w.max(1e-9),
            b.wall.as_secs_f64(),
            chars as f64 / n as f64
        );
        medians.insert(name, (w, b.wall.as_secs_f64()));
    }
    let (full, blank_full) = medians["default"];
    let (no_doc, _) = medians["no page orientation"];
    let (_, blank_no_doc) = medians["no page orientation"];
    let (no_line, _) = medians["no line orientation"];
    let doc_ori = full - no_doc;
    let line_ori = full - no_line;
    let detection = blank_no_doc;
    println!(
        "breakdown per page ({threads} thread(s)): engine {full:.2} s = page orientation \
         {doc_ori:.2} (blank page with and without: {blank_full:.2} / {blank_no_doc:.2}) + \
         detection and image copies {detection:.2} + line orientation {line_ori:.2} + \
         recognition and the rest {:.2}",
        full - doc_ori - detection - line_ori
    );
}

/// doewood's analyzer with `OCR_WORKERS` workers (and `OCR_JOBS` as `DOEWOOD_OCR_JOBS`).
#[test]
#[ignore = "measurement (issue #116): needs DOEWOOD_OCR_MODELS, minutes of CPU"]
fn analyzer_workers() {
    let Some(dir) = models() else { return };
    let workers = env_usize("OCR_WORKERS").unwrap_or(1).max(1);
    let n = env_usize("OCR_FILES").unwrap_or(8).max(1);
    let pages = env_usize("OCR_FILE_PAGES").unwrap_or(1).max(1);
    let files = scan_files(n, pages);
    let (analyzer, load) = measure(|| analyzer(dir, workers, env_usize("OCR_JOBS")));
    load.report("analyzer built");
    println!("{workers} worker(s), {:?}", analyzer.ocr());
    let next = AtomicUsize::new(0);
    let results = Mutex::new(Vec::new());
    let ((), usage) = measure(|| {
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(f) = files.get(i) else { break };
                        let t = Instant::now();
                        let (_, steps, ocr) = record_text(&analyzer, f.clone());
                        results.lock().unwrap().push((t.elapsed(), steps, ocr));
                    }
                });
            }
        });
    });
    let results = results.into_inner().unwrap();
    let total = n * pages;
    let read: i64 = results.iter().map(|r| r.2).sum();
    assert_eq!(read, i64::try_from(total).unwrap(), "every page OCR'd");
    usage.report(&format!("{n} files x {pages} page(s), {workers} worker(s)"));
    println!(
        "  {:.3} pages/s, {:.2} s wall a page, {:.2} CPU s a page; file time median {:.2} s",
        total as f64 / usage.wall.as_secs_f64(),
        usage.wall.as_secs_f64() / total as f64,
        usage.cpu / total as f64,
        median(results.iter().map(|r| r.0.as_secs_f64()).collect())
    );
    let mut steps: BTreeMap<String, u64> = BTreeMap::new();
    for r in &results {
        for (k, v) in &r.1 {
            *steps.entry(k.clone()).or_default() += v;
        }
    }
    let sum: u64 = steps.values().sum();
    let mut by_time: Vec<_> = steps.into_iter().collect();
    by_time.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    for (k, v) in by_time.iter().take(8) {
        println!(
            "  step {k}: {:.3} s a page ({:.1} %)",
            *v as f64 / 1e6 / total as f64,
            *v as f64 * 100.0 / sum.max(1) as f64
        );
    }
}

/// `OCR_JOBS` engines side by side, `OCR_THREADS` threads each.
#[test]
#[ignore = "measurement (issue #116): needs DOEWOOD_OCR_MODELS, minutes of CPU"]
fn engines() {
    let Some(dir) = models() else { return };
    let jobs = env_usize("OCR_JOBS").unwrap_or(1).max(1);
    let threads = env_usize("OCR_THREADS").unwrap_or((cpus() / jobs).max(1));
    let n = env_usize("OCR_PAGES").unwrap_or(8).max(1);
    let images: Vec<Image> = decode(&scan_files(n, 1)).into_iter().map(|i| i.0).collect();
    let factory = PaddleFactory::new(&dir, paddle(threads)).unwrap();
    let (mut engines, load) = measure(|| {
        (0..jobs)
            .map(|_| factory.engine().unwrap())
            .collect::<Vec<_>>()
    });
    load.report(&format!("{jobs} engine(s) loaded"));
    // Warm every engine's arenas once, as a running instance has them.
    let ((), warm) = measure(|| {
        std::thread::scope(|s| {
            for e in &mut engines {
                let img = &images[0];
                s.spawn(move || e.recognize(img, &hints()).unwrap());
            }
        });
    });
    warm.report("first page on every engine (cold)");
    let next = AtomicUsize::new(0);
    let outputs = Mutex::new(BTreeMap::new());
    let ((), u) = measure(|| {
        std::thread::scope(|s| {
            for e in &mut engines {
                let (next, outputs, images) = (&next, &outputs, &images);
                s.spawn(move || {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(img) = images.get(i) else { break };
                        let p = e.recognize(img, &hints()).unwrap();
                        outputs.lock().unwrap().insert(i, page_json(&p));
                    }
                });
            }
        });
    });
    u.report(&format!("{n} pages, {jobs} job(s) x {threads} thread(s)"));
    let outputs = outputs.into_inner().unwrap();
    let digest = blake3::hash(outputs.values().cloned().collect::<String>().as_bytes());
    println!(
        "  {:.3} pages/s, {:.2} CPU s a page; output digest {}",
        n as f64 / u.wall.as_secs_f64(),
        u.cpu / n as f64,
        &digest.to_hex()[..16]
    );
}

/// Ingest workers and `DOEWOOD_OCR_JOBS`.
type Setting = (usize, Option<usize>);

/// The settings measured give the same bytes: the engine's output for every thread count, and
/// the analyzer's records (without timings) for 1 and 4 workers and 1, 2 and 4 OCR jobs.
#[test]
#[ignore = "needs DOEWOOD_OCR_MODELS (issue #116), minutes of CPU"]
fn same_output() {
    let Some(dir) = models() else { return };
    let n = env_usize("OCR_PAGES").unwrap_or(3).max(1);
    let files = scan_files(n, 1);
    let images: Vec<Image> = decode(&files).into_iter().map(|i| i.0).collect();
    let mut by_threads: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for threads in [4, 2, 1] {
        let mut e = PaddleFactory::new(&dir, paddle(threads))
            .unwrap()
            .engine()
            .unwrap();
        let out: Vec<String> = images
            .iter()
            .map(|i| page_json(&e.recognize(i, &hints()).unwrap()))
            .collect();
        assert!(out.iter().all(|o| o.len() > 1000), "pages were read");
        by_threads.insert(threads, out);
    }
    let base = &by_threads[&4];
    for (t, out) in &by_threads {
        for (i, (a, b)) in base.iter().zip(out).enumerate() {
            assert!(a == b, "page {i}: 4 threads and {t} differ");
        }
    }
    println!("engine output identical for 1, 2 and 4 threads on {n} pages");

    // The analyzer as configured by `DOEWOOD_INGEST_JOBS` and `DOEWOOD_OCR_JOBS`; with 4
    // workers and several OCR jobs the files are read side by side.
    let mut runs: Vec<(Setting, Vec<String>)> = Vec::new();
    for (workers, jobs) in [(1, None), (4, None), (4, Some(2)), (4, Some(4))] {
        let a = analyzer(dir.clone(), workers, jobs);
        let next = AtomicUsize::new(0);
        let out = Mutex::new(BTreeMap::new());
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(f) = files.get(i) else { break };
                        let (text, _, ocr) = record_text(&a, f.clone());
                        assert_eq!(ocr, 1);
                        out.lock().unwrap().insert(i, text);
                    }
                });
            }
        });
        runs.push((
            (workers, jobs),
            out.into_inner().unwrap().into_values().collect(),
        ));
    }
    let (base, records) = &runs[0];
    for (setting, other) in &runs[1..] {
        for (i, (a, b)) in records.iter().zip(other).enumerate() {
            assert!(
                a == b,
                "file {i}: records of {base:?} and {setting:?} differ"
            );
        }
    }
    println!(
        "records identical for (workers, OCR jobs) {:?} on {n} files",
        runs.iter().map(|r| r.0).collect::<Vec<_>>()
    );
}
