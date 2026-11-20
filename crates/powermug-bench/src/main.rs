//! Load bench of the telemetry path (SPECIFICATIONS §13.1, NFR-PERF-002).
//!
//! It runs one partition lane on the single writer of a cell, over a telemetry
//! file in a temporary directory, and measures each `ingest` call from the
//! reception of the batch to the commit to the journal. It reports the quantiles
//! that §13.3 asks for.
//!
//! The numbers are only meaningful on the qualified hardware (decision 2). On any
//! other host they are a development measurement, and the output says so.
//!
//! Usage: `powermug-bench [--batches N] [--events E] [--points P] [--budget-ms B]`.
//! With `--budget-ms`, the process exits with status 1 when the p99 exceeds it.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, GroupJournal};

/// Quantiles of a set of durations, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quantiles {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

/// The quantiles of `samples`, by nearest rank. `None` for an empty set.
#[must_use]
pub fn quantiles(samples: &[Duration]) -> Option<Quantiles> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1_000.0).collect();
    sorted.sort_by(f64::total_cmp);
    // `per_mille` is the quantile in thousandths: 990 is the 99th percentile.
    let rank = |per_mille: usize| {
        let index = (per_mille * sorted.len())
            .div_ceil(1_000)
            .clamp(1, sorted.len());
        sorted[index - 1]
    };
    Some(Quantiles {
        p50: rank(500),
        p95: rank(950),
        p99: rank(990),
        p999: rank(999),
        max: sorted[sorted.len() - 1],
    })
}

/// A `UUIDv7` with a 64-bit counter in its last bytes.
fn event_id(counter: u64) -> EventId {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[8..16].copy_from_slice(&(0x8000_0000_0000_0000u64 | counter).to_be_bytes());
    EventId::from_bytes(bytes).expect("v7 with the RFC variant")
}

struct Options {
    batches: u64,
    events: u64,
    points: u32,
    budget_ms: Option<f64>,
}

fn parse(arguments: &[String]) -> Result<Options, String> {
    let mut options = Options {
        batches: 2_000,
        events: 100,
        points: 1_000,
        budget_ms: None,
    };
    let mut args = arguments.iter();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--batches" => options.batches = value.parse().map_err(|_| "--batches".to_owned())?,
            "--events" => options.events = value.parse().map_err(|_| "--events".to_owned())?,
            "--points" => options.points = value.parse().map_err(|_| "--points".to_owned())?,
            "--budget-ms" => {
                options.budget_ms = Some(value.parse().map_err(|_| "--budget-ms".to_owned())?);
            }
            other => return Err(format!("unknown option {other}")),
        }
    }
    if options.points == 0 || options.events == 0 || options.batches == 0 {
        return Err("batches, events and points must be positive".to_owned());
    }
    Ok(options)
}

fn run(options: &Options) -> Result<(Vec<Duration>, Duration), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let (journal, writer) =
        GroupJournal::spawn(dir.path().join("telemetry.db")).map_err(|e| e.to_string())?;
    let specs = (0..options.points)
        .map(|i| PointSpec {
            site_id: "S3".into(),
            point_id: format!("S3/TM_{i:05}"),
            class: EventClass::A,
            session: Some(SessionId(1)),
            supervision: None,
        })
        .collect();
    let table = PointTable::new(1, specs).map_err(|e| e.to_string())?;
    let mut lane = Lane::new(table, journal, 0);

    let mut samples = Vec::with_capacity(usize::try_from(options.batches).unwrap_or(0));
    let mut counter = 0u64;
    let started = Instant::now();
    for batch in 0..options.batches {
        let events: Vec<PointEvent> = (0..options.events)
            .map(|i| {
                counter += 1;
                PointEvent {
                    point: PointIndex(
                        u32::try_from((batch * options.events + i) % u64::from(options.points))
                            .unwrap_or(0),
                    ),
                    value: Value::Int(i64::try_from(counter).unwrap_or(i64::MAX)),
                    quality: Quality::GOOD,
                    source_time_ns: 1,
                    receive_time_ns: 1,
                    process_time_ns: 1,
                    event_id: event_id(counter),
                    revision_index: 1,
                }
            })
            .collect();
        let at = Times {
            monotonic_ns: 0,
            wall_ns: 0,
        };
        let begin = Instant::now();
        lane.ingest(&events, at).map_err(|e| e.to_string())?;
        samples.push(begin.elapsed());
    }
    let total = started.elapsed();
    drop(lane);
    writer.join();
    Ok((samples, total))
}

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let options = match parse(&arguments) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("powermug-bench: {message}");
            return ExitCode::from(2);
        }
    };
    let (samples, total) = match run(&options) {
        Ok(result) => result,
        Err(message) => {
            eprintln!("powermug-bench: {message}");
            return ExitCode::from(2);
        }
    };
    let Some(quantiles) = quantiles(&samples) else {
        eprintln!("powermug-bench: no sample");
        return ExitCode::from(2);
    };
    let events = options.batches * options.events;
    println!("powermug-bench: telemetry reception to journal commit (NFR-PERF-002)");
    println!("host: development host, not the qualified hardware; figures are not NFR results");
    println!(
        "batches={} events_per_batch={} points={} events={}",
        options.batches, options.events, options.points, events
    );
    #[allow(clippy::cast_precision_loss, reason = "a rate for display")]
    let rate = events as f64 / total.as_secs_f64();
    println!("throughput_events_per_s={rate:.0}");
    println!(
        "latency_ms p50={:.3} p95={:.3} p99={:.3} p99.9={:.3} max={:.3}",
        quantiles.p50, quantiles.p95, quantiles.p99, quantiles.p999, quantiles.max
    );
    if let Some(budget) = options.budget_ms {
        if quantiles.p99 > budget {
            println!(
                "FAIL: p99 {:.3} ms exceeds the budget of {budget} ms",
                quantiles.p99
            );
            return ExitCode::from(1);
        }
        println!(
            "PASS: p99 {:.3} ms within the budget of {budget} ms",
            quantiles.p99
        );
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_nearest_rank() {
        let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let q = quantiles(&samples).expect("some samples");
        assert_eq!(q.p50, 50.0);
        assert_eq!(q.p95, 95.0);
        assert_eq!(q.p99, 99.0);
        assert_eq!(q.max, 100.0);
    }

    #[test]
    fn no_sample_gives_no_quantiles() {
        assert_eq!(quantiles(&[]), None);
    }

    #[test]
    fn options_are_checked() {
        assert!(parse(&["--batches".into(), "0".into()]).is_err());
        assert!(parse(&["--unknown".into(), "1".into()]).is_err());
        assert!(parse(&["--budget-ms".into()]).is_err());
        assert!(parse(&["--batches".into(), "3".into()]).is_ok());
    }
}
