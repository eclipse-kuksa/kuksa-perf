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

use anyhow::Result;
use clap::Parser;
use config::Group;
use measure::{perform_measurement, Api, MeasurementConfig};
use shutdown::setup_shutdown_handler;
use std::{
    cmp::{max, min},
    collections::HashSet,
};

use utils::read_config;

mod config;
mod measure;
mod providers;
mod shutdown;
mod subscriber;
mod types;
mod utils;

#[derive(Parser)]
#[clap(author, version, about)]
struct Args {
    /// Number of seconds to run.
    #[clap(
        long,
        short,
        display_order = 1,
        default_value_t = 8,
        conflicts_with = "run_forever"
    )]
    duration: u64,

    /// Api of databroker.
    #[clap(long, display_order = 2, default_value = "kuksa.val.v1", value_parser = clap::builder::PossibleValuesParser::new(["kuksa.val.v1", "kuksa.val.v2", "sdv.databroker.v1"]))]
    api: String,

    /// Host address of databroker.
    #[clap(long, display_order = 3, default_value = "http://127.0.0.1")]
    host: String,

    /// Port of databroker.
    #[clap(long, display_order = 4, default_value_t = 55555)]
    port: u64,

    /// Seconds to run (skip) before measuring the latency.
    #[clap(long, display_order = 5, value_name = "DURATION", default_value_t = 4)]
    skip_seconds: u64,

    /// Print more details in the summary result
    #[clap(
        long,
        display_order = 6,
        value_name = "Detailed ouput result",
        default_value_t = false
    )]
    detailed_output: bool,

    /// Path to test data file
    #[clap(long = "test-data-file", display_order = 7, value_name = "FILE")]
    test_data_file: Option<String>,

    /// Run the measurements forever (until receiving a shutdown signal).
    #[clap(
        long,
        action = clap::ArgAction::SetTrue,
        display_order = 8,
        conflicts_with = "duration",
        default_value_t = false
    )]
    run_forever: bool,

    /// Verbosity level. Can be one of ERROR, WARN, INFO, DEBUG, TRACE.
    #[clap(
        long = "verbosity",
        short,
        display_order = 10,
        value_name = "LEVEL",
        default_value_t = log::Level::Warn
    )]
    verbosity_level: log::Level,
}

fn setup_logging(verbosity_level: log::Level) -> Result<()> {
    stderrlog::new()
        .module(module_path!())
        .verbosity(verbosity_level)
        .init()?;
    Ok(())
}

fn check_duplicate_paths(groups: &Vec<Group>) {
    let mut seen_paths: HashSet<String> = HashSet::new();

    for group in groups {
        for signal in &group.signals {
            if !seen_paths.insert(signal.path.clone()) {
                // If the insert fails, it means the path is a duplicate
                eprintln!("Error: Duplicate path found: {}", signal.path);
                std::process::exit(1); // Exit the program with status code 1
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    setup_logging(args.verbosity_level)?;

    let shutdown_handler = setup_shutdown_handler();

    if args.duration <= args.skip_seconds {
        eprintln!(
            "Error: `duration` ({}) cannot be equal or smaller than `skip_seconds` ({}).",
            args.duration, args.skip_seconds
        );
        std::process::exit(1);
    }

    let mut api = Api::KuksaValV1;
    if args.api.contains("sdv.databroker.v1") {
        api = Api::SdvDatabrokerV1;
    } else if args.api.contains("kuksa.val.v2") {
        api = Api::KuksaValV2;
    }

    let config_groups = read_config(args.test_data_file.as_ref())?;

    for group in &config_groups {
        if group.cycle_time_ms as u64 >= args.duration * 1000 {
            eprintln!(
                "Error: Group name: {} contain a higher or equal cycle_time_ms: {} seconds than the specified test --duration: {} seconds.",
                group.group_name, group.cycle_time_ms / 1000, args.duration
            );
            std::process::exit(1);
        }
    }

    check_duplicate_paths(&config_groups);
    // Skip at most _iterations_ number of iterations
    let skip_seconds = max(0, min(args.duration, args.skip_seconds));

    let measurement_config = MeasurementConfig {
        host: args.host,
        port: args.port,
        duration: args.duration,
        interval: 0,
        skip_seconds,
        api,
        run_forever: args.run_forever,
        detailed_output: args.detailed_output,
    };

    perform_measurement(measurement_config, config_groups, shutdown_handler).await?;
    Ok(())
}
