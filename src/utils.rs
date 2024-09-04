/********************************************************************************
* Copyright (c) 2024 Contributors to the Eclipse Foundation
*
* See the NOTICE file(s) distributed with this work for additional
* information regarding copyright ownership.
*
* This program and the accompanying materials are made available under the
* terms of the Apache License 2.0 which is available at
* http://www.apache.org/licenses/LICENSE-2.0
*
* SPDX-License-Identifier: Apache-2.0
********************************************************************************/

use anyhow::{Context, Result};
use console::Term;
use hdrhistogram::Histogram;
use log::debug;
use serde_json::from_reader;
use std::{cmp::max, fs::OpenOptions, io::Write, time::SystemTime};

use crate::{
    config::{Config, Signal},
    measure::Api,
};

pub fn read_config(config_file: Option<&String>) -> Result<Vec<Signal>> {
    match config_file {
        Some(filename) => {
            let file = OpenOptions::new()
                .read(true)
                .open(filename)
                .with_context(|| format!("Failed to open configuration file '{}'", filename))?;
            let config: Config = from_reader(file)
                .with_context(|| format!("Failed to parse configuration file '{}'", filename))?;
            Ok::<Vec<Signal>, anyhow::Error>(config.signals)
        }
        None => {
            // Default set of signals
            Ok(vec![Signal {
                path: "Vehicle.Cabin.Infotainment.Media.Played.Track".to_owned(),
                id: None,
            }])
        }
    }
}

pub async fn write_output(
    start_time: SystemTime,
    interval: u16,
    iterations: u64,
    number_of_signals: u64,
    skipped: u64,
    hist: Histogram<u64>,
    api: Api,
) -> Result<()> {
    let mut stdout = Term::stdout();
    let end_time = SystemTime::now();
    let total_duration = end_time.duration_since(start_time)?;
    writeln!(stdout, "\n\nSummary:")?;
    writeln!(stdout, "  API: {}", api)?;
    writeln!(
        stdout,
        "  Elapsed time: {:.2} s",
        total_duration.as_millis() as f64 / 1000.0
    )?;
    let rate_limit = match interval {
        0 => "None".into(),
        ms => format!("{} ms between iterations", ms),
    };
    writeln!(stdout, "  Rate limit: {}", rate_limit)?;
    writeln!(
        stdout,
        "  Sent: {} iterations * {} signals = {} updates",
        iterations,
        number_of_signals,
        iterations * number_of_signals
    )?;
    writeln!(stdout, "  Skipped: {} updates", skipped)?;
    writeln!(stdout, "  Received: {} updates", hist.len())?;
    writeln!(stdout, "  Fastest: {:>7.3} ms", hist.min() as f64 / 1000.0)?;
    writeln!(stdout, "  Slowest: {:>7.3} ms", hist.max() as f64 / 1000.0)?;
    writeln!(stdout, "  Average: {:>7.3} ms", hist.mean() / 1000.0)?;
    writeln!(stdout, "\nLatency histogram:")?;

    let step_size = max(1, (hist.max() - hist.min()) / 11);

    let buckets = hist.iter_linear(step_size);

    // skip initial empty buckets
    let buckets = buckets.skip_while(|v| v.count_since_last_iteration() == 0);

    let mut histogram = Vec::with_capacity(11);

    for v in buckets {
        let mean = v.value_iterated_to() + 1 - step_size / 2; // +1 to make range inclusive
        let count = v.count_since_last_iteration();
        histogram.push((mean, count));
    }

    let (_, cols) = stdout.size();
    debug!("Number of columns: {cols}");

    for (mean, count) in histogram {
        let bars = count as f64 / (hist.len() * number_of_signals) as f64 * (cols - 22) as f64;
        let bar = "∎".repeat(bars as usize);
        writeln!(
            stdout,
            "  {:>7.3} ms [{:<5}] |{}",
            mean as f64 / 1000.0,
            count,
            bar
        )?;
    }

    writeln!(stdout, "\nLatency distribution:")?;

    for q in &[10, 25, 50, 75, 90, 95, 99] {
        writeln!(
            stdout,
            "  {q}% in under {:.3} ms",
            hist.value_at_quantile(*q as f64 / 100.0) as f64 / 1000.0
        )?;
    }
    Ok(())
}
