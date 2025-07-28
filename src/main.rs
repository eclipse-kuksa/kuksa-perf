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
use measure::{perform_measurement, Api, MeasurementConfig, Operation};
use shutdown::setup_shutdown_handler;
use std::collections::HashSet;

use utils::read_config;

mod config;
mod measure;
mod receiving_ends;
mod shutdown;
mod triggering_ends;
mod types;
mod utils;

#[derive(Parser)]
#[clap(author, version, about)]
struct Args {
    /// Number of seconds to run.
    #[clap(long, short, display_order = 1, value_name = "SECONDS")]
    duration: Option<u64>,

    /// Api of databroker.
    #[clap(long, display_order = 2, default_value = "kuksa.val.v1", value_parser = clap::builder::PossibleValuesParser::new(["kuksa.val.v1", "kuksa.val.v2", "sdv.databroker.v1"]))]
    api: String,

    /// Operation that will be measured.
    #[clap(long, display_order = 2, default_value = "streaming_publish", value_parser = clap::builder::PossibleValuesParser::new(["streaming_publish", "actuate"]))]
    operation: String,

    /// Host address of databroker.
    #[clap(long, display_order = 3, default_value = "http://127.0.0.1")]
    host: String,

    /// Port of databroker.
    #[clap(long, display_order = 4, default_value_t = 55555)]
    port: u64,

    /// Unix socket path of databroker
    #[clap(long = "unix-socket", display_order = 5)]
    unix_socket_path: Option<String>,

    /// Seconds to run (skip) before measuring the latency.
    #[clap(long, display_order = 5, value_name = "SECONDS")]
    skip_seconds: Option<u64>,

    /// Print more details in the summary result
    #[clap(
        long,
        display_order = 6,
        value_name = "Detailed ouput result",
        default_value_t = false
    )]
    detailed_output: bool,

    /// kuksa.val.v2 subscription buffer_size
    #[clap(long, display_order = 7)]
    buffer_size: Option<u32>,

    /// Path to test data file
    #[clap(long = "test-data-file", display_order = 7, value_name = "FILE")]
    test_data_file: Option<String>,

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

fn check_if_duplicate_paths(groups: &Vec<Group>) -> bool {
    let mut seen_paths: HashSet<String> = HashSet::new();

    for group in groups {
        for signal in &group.signals {
            if !seen_paths.insert(signal.path.clone()) {
                println!("Error: Duplicate path found: {}", signal.path);
                return false;
            }
        }
    }
    true
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    setup_logging(args.verbosity_level)?;

    let shutdown_handler = setup_shutdown_handler();

    if let Some(duration) = args.duration {
        if duration == 0 {
            eprintln!("Error: `duration` cannot be less than `0` seconds.");
            std::process::exit(1);
        } else if let Some(skip_seconds) = args.skip_seconds {
            if duration <= skip_seconds {
                eprintln!(
                    "Error: `duration` ({duration}) cannot be smaller or equal than `skip_seconds` ({skip_seconds})."
                );
                std::process::exit(1);
            }
        }
    }

    let (operation, api) = if args.operation.contains("streaming_publish") {
        (
            Operation::StreamingPublish,
            if args.api.contains("sdv.databroker.v1") {
                Api::SdvDatabrokerV1
            } else if args.api.contains("kuksa.val.v2") {
                Api::KuksaValV2
            } else if args.api.contains("kuksa.val.v1") {
                Api::KuksaValV1
            } else {
                eprintln!("Error: No supported API of databroker given.");
                std::process::exit(1);
            },
        )
    } else if args.operation.contains("actuate") {
        if args.api.contains("sdv.databroker.v1") {
            eprintln!("Error: sdv.databroker.v1 is not supported for measuring actuate operation.");
            std::process::exit(1);
        } else if args.api.contains("kuksa.val.v2") {
            (Operation::Actuate, Api::KuksaValV2)
        } else if args.api.contains("kuksa.val.v1") {
            eprintln!("Error: sdv.databroker.v1 is not supported for measuring actuate operation.");
            std::process::exit(1);
        } else {
            eprintln!("Error: No supported API of databroker given.");
            std::process::exit(1);
        }
    } else {
        eprintln!("Error: No operation given.");
        std::process::exit(1);
    };

    if args.buffer_size.is_some() && matches!(api, Api::SdvDatabrokerV1 | Api::KuksaValV1) {
        println!("Warning: buffer_size will be ignored, only supported for kuksa.val.v2 API");
    }

    let config_groups = read_config(args.test_data_file.as_ref())?;

    if !check_if_duplicate_paths(&config_groups) {
        std::process::exit(1);
    }

    let measurement_config = MeasurementConfig {
        host: args.host,
        port: args.port,
        unix_socket_path: args.unix_socket_path,
        duration: args.duration,
        interval: 0,
        skip_seconds: args.skip_seconds,
        api,
        operation,
        detailed_output: args.detailed_output,
        buffer_size: args.buffer_size,
    };

    if cfg!(debug_assertions) {
        println!("Warning: You are running a debug build of kuksa-perf. This may affect performance measurements. If you want to run performance measurements, it is recommended you use a release build (cargo build --release).");
    }
    perform_measurement(measurement_config, config_groups, shutdown_handler).await?;
    Ok(())
}
