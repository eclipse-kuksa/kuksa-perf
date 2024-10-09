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

use anyhow::{Context, Ok, Result};
use console::Term;
use hdrhistogram::Histogram;
use log::debug;
use serde_json::from_reader;
use std::{
    cmp::max,
    fs::OpenOptions,
    io::Write,
    time::{Duration, SystemTime},
};

use crate::{
    config::{Config, Group, Signal},
    measure::{MeasurementConfig, MeasurementResult},
};

pub fn read_config(config_file: Option<&String>) -> Result<Vec<Group>> {
    match config_file {
        Some(filename) => {
            let file = OpenOptions::new()
                .read(true)
                .open(filename)
                .with_context(|| format!("Failed to open configuration file '{}'", filename))?;
            let config: Config = from_reader(file)
                .with_context(|| format!("Failed to parse configuration file '{}'", filename))?;

            Ok(config.groups)
        }
        None => {
            // Return a default set of groups or handle the None case appropriately
            Ok(vec![
                Group {
                    group_name: String::from("Group A"),
                    cycle_time_ms: 0,
                    signals: vec![Signal {
                        path: String::from("Vehicle.Speed"),
                    }],
                },
                Group {
                    group_name: String::from("Group B"),
                    cycle_time_ms: 0,
                    signals: vec![Signal {
                        path: String::from("Vehicle.IsBrokenDown"),
                    }],
                },
                Group {
                    group_name: String::from("Group C"),
                    cycle_time_ms: 0,
                    signals: vec![
                        Signal {
                            path: String::from("Vehicle.Body.Windshield.Front.Wiping.Intensity"),
                        },
                        Signal {
                            path: String::from("Vehicle.Body.Windshield.Front.Wiping.Mode"),
                        },
                        Signal {
                            path: String::from("Vehicle.Body.Windshield.Front.Wiping.WiperWear"),
                        },
                    ],
                },
            ])
        }
    }
}

fn print_latency_histogram(stdout: &mut Term, histogram: &Histogram<u64>) -> Result<()> {
    let step_size = max(1, (histogram.max() - histogram.min()) / 11);

    let buckets = histogram.iter_linear(step_size);

    // skip initial empty buckets
    let buckets = buckets.skip_while(|v| v.count_since_last_iteration() == 0);

    let mut histogram_percentile = Vec::with_capacity(11);

    for v in buckets {
        let mean = v.value_iterated_to() + 1 - step_size / 2; // +1 to make range inclusive
        let count = v.count_since_last_iteration();
        histogram_percentile.push((mean, count));
    }

    let (_, cols) = stdout.size();
    debug!("Number of columns: {cols}");

    for (mean, count) in &histogram_percentile {
        let bars = (*count as f64 / histogram.len() as f64) * (cols - 22) as f64;
        let bar = "∎".repeat(bars as usize);
        writeln!(
            stdout,
            "  {:>7.3} ms [{:<5}] |{}",
            *mean as f64 / 1000.0,
            count,
            bar
        )?;
    }
    Ok(())
}

fn print_latency_distribution(stdout: &mut Term, histogram: &Histogram<u64>) -> Result<()> {
    for q in &[10, 25, 50, 75, 90, 95, 99] {
        writeln!(
            stdout,
            "  {q}% in under {:.3} ms",
            histogram.value_at_quantile(*q as f64 / 100.0) as f64 / 1000.0
        )?;
    }
    Ok(())
}

pub fn write_global_output(
    measurement_config: &MeasurementConfig,
    measurement_results: &Vec<MeasurementResult>,
) -> Result<()> {
    let mut stdout = Term::stdout();
    let end_time = SystemTime::now();

    let mut global_end_time = Duration::default();

    let mut global_hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;

    let mut global_signals_len = 0;
    let mut global_signals_sent = 0;
    let mut global_signals_skipped = 0;

    for result in measurement_results {
        global_end_time += end_time.duration_since(result.start_time)?;

        global_hist += result.measurement_context.hist.clone();

        global_signals_len += result.measurement_context.signals.len();
        global_signals_sent +=
            result.iterations_executed * result.measurement_context.signals.len() as u64;
        global_signals_skipped += result.signals_skipped;
    }

    writeln!(stdout, "\n\nGlobal Summary:")?;
    writeln!(stdout, "  API: {}", measurement_config.api)?;

    let total_elapsed_seconds = match measurement_config.duration {
        Some(duration) => duration,
        None => {
            global_end_time /= measurement_results.len() as u32; // Update global_end_time
            global_end_time.as_secs() // Return the adjusted seconds
        }
    };
    writeln!(stdout, "  Total elapsed seconds: {}", total_elapsed_seconds)?;

    let skip_seconds = match measurement_config.skip_seconds {
        Some(seconds) => {
            writeln!(stdout, "  Skipped test seconds: {}", seconds)?;
            seconds
        }
        None => 0,
    };

    writeln!(stdout, "  Total signals: {} signals", global_signals_len,)?;
    writeln!(stdout, "  Sent: {} signal updates", global_signals_sent,)?;
    writeln!(
        stdout,
        "  Skipped: {} signal updates",
        global_signals_skipped
    )?;
    writeln!(stdout, "  Received: {} signal updates", global_hist.len())?;

    let elapsed_seconds = match measurement_config.duration {
        Some(duration) => duration - skip_seconds,
        None => global_end_time.as_secs() - skip_seconds,
    };

    writeln!(
        stdout,
        "  Throughput: {} signal/second",
        global_hist.len() / elapsed_seconds
    )?;

    writeln!(
        stdout,
        "  95% in under: {:.3} ms",
        global_hist.value_at_quantile(95.0 / 100.0) as f64 / 1000.0
    )?;

    writeln!(
        stdout,
        "  Fastest: {:>7.3} ms",
        global_hist.min() as f64 / 1000.0
    )?;
    writeln!(
        stdout,
        "  Slowest: {:>7.3} ms",
        global_hist.max() as f64 / 1000.0
    )?;
    writeln!(stdout, "  Average: {:>7.3} ms", global_hist.mean() / 1000.0)?;

    writeln!(stdout, "\nLatency histogram:")?;
    print_latency_histogram(stdout.by_ref(), &global_hist).unwrap();

    writeln!(stdout, "\nLatency distribution:")?;
    print_latency_distribution(stdout.by_ref(), &global_hist).unwrap();
    Ok(())
}

pub fn write_output(measurement_result: &MeasurementResult) -> Result<()> {
    let mut stdout = Term::stdout();
    let end_time = SystemTime::now();
    let total_duration = end_time.duration_since(measurement_result.start_time)?;
    let measurement_context = &measurement_result.measurement_context;
    let measurement_config = &measurement_context.measurement_config;

    writeln!(
        stdout,
        "\n\nGroup: {} | Cycle time(ms): {}",
        measurement_context.group_name, measurement_config.interval
    )?;
    writeln!(
        stdout,
        "  API: {}",
        measurement_context.measurement_config.api
    )?;
    writeln!(
        stdout,
        "  Elapsed time: {:.2} s",
        total_duration.as_millis() as f64 / 1000.0
    )?;
    let rate_limit = match measurement_config.interval {
        0 => "None".into(),
        ms => format!("{} ms between iterations", ms),
    };
    writeln!(stdout, "  Rate limit: {}", rate_limit)?;
    writeln!(
        stdout,
        "  Sent: {} iterations * {} signals = {} updates",
        measurement_result.iterations_executed,
        measurement_context.signals.len(),
        measurement_result.iterations_executed * measurement_context.signals.len() as u64
    )?;
    writeln!(
        stdout,
        "  Skipped: {} updates",
        measurement_result.signals_skipped
    )?;
    writeln!(
        stdout,
        "  Received: {} updates",
        measurement_context.hist.len()
    )?;

    let throughput = match measurement_config.duration {
        Some(duration) => {
            measurement_context.hist.len() / (duration - measurement_config.skip_seconds.unwrap())
        }
        None => {
            measurement_context.hist.len()
                / (total_duration.as_secs() - measurement_config.skip_seconds.unwrap())
        }
    };

    writeln!(stdout, "  Throughput: {} signal/second", throughput)?;

    writeln!(
        stdout,
        "  95% in under: {:.3} ms",
        measurement_context.hist.value_at_quantile(95.0 / 100.0) as f64 / 1000.0
    )?;
    writeln!(
        stdout,
        "  Fastest: {:>7.3} ms",
        measurement_context.hist.min() as f64 / 1000.0
    )?;
    writeln!(
        stdout,
        "  Slowest: {:>7.3} ms",
        measurement_context.hist.max() as f64 / 1000.0
    )?;
    writeln!(
        stdout,
        "  Average: {:>7.3} ms",
        measurement_context.hist.mean() / 1000.0
    )?;

    writeln!(stdout, "\nLatency histogram:")?;

    print_latency_histogram(stdout.by_ref(), &measurement_context.hist).unwrap();

    writeln!(stdout, "\nLatency distribution:")?;
    print_latency_distribution(stdout.by_ref(), &measurement_context.hist).unwrap();
    Ok(())
}
