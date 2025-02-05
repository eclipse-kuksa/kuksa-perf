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

use crate::triggering_ends::kuksa_val_v1::triggering_end as p_kuksa_val_v1;
use crate::triggering_ends::kuksa_val_v2::triggering_end as p_kuksa_val_v2;
use crate::triggering_ends::sdv_databroker_v1::triggering_end as p_sdv_databroker_v1;

use crate::triggering_ends::triggering_end_trait::{TriggeringEndInterface, PublishError};

use crate::receiving_ends::kuksa_val_v1::receiving_end as s_kuksa_val_v1;
use crate::receiving_ends::kuksa_val_v2::receiving_end as s_kuksa_val_v2;
use crate::receiving_ends::sdv_databroker_v1::receiving_end as s_sdv_databroker_v1;

use crate::config::{Group, Signal};

use crate::shutdown::ShutdownHandler;
use crate::receiving_ends::receiving_end_trait::{Error, ReceivingEndInterface};
use crate::types::DataValue;
use crate::utils::{write_global_output, write_output};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::{
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
};
use tokio::net::UnixStream;
use tokio::sync::mpsc::Sender;
use tokio::task;
use tokio::{select, task::JoinSet, time::Instant};
use tonic::transport::Channel;
use tower::service_fn;

#[derive(Clone, PartialEq)]
pub enum Api {
    KuksaValV1,
    KuksaValV2,
    SdvDatabrokerV1,
}

#[derive(Clone, PartialEq)]
pub enum Direction {
    Read,
    Write,
}

pub struct TriggeringEnd {
    pub triggering_end_interface: Box<dyn TriggeringEndInterface>,
}

pub struct ReceivingEnd {
    pub receiving_end_interface: Box<dyn ReceivingEndInterface>,
}

#[derive(Clone)]
pub struct MeasurementConfig {
    pub host: String,
    pub port: u64,
    pub unix_socket_path: Option<String>,
    pub duration: Option<u64>,
    pub interval: u16,
    pub skip_seconds: Option<u64>,
    pub api: Api,
    pub direction: Direction,
    pub detailed_output: bool,
    pub buffer_size: Option<u32>,
}

pub struct MeasurementContext {
    pub measurement_config: MeasurementConfig,
    pub group_name: String,
    pub shutdown_handler: ShutdownHandler,
    pub triggering_end: TriggeringEnd,
    pub signals: Vec<Signal>,
    pub receiving_end: ReceivingEnd,
    pub hist: Histogram<u64>,
    pub running_hist: Histogram<u64>,
    pub latency_series: Vec<u64>,
}

pub struct MeasurementResult {
    pub measurement_context: MeasurementContext,
    pub iterations_executed: u64,
    pub signals_skipped: u64,
    pub start_time: SystemTime,
}

impl fmt::Display for Api {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Api::KuksaValV1 => write!(f, "kuksa.val.v1"),
            Api::KuksaValV2 => write!(f, "kuksa.val.v2"),
            Api::SdvDatabrokerV1 => write!(f, "sdv.databroker.v1"),
        }
    }
}

async fn create_receiving_end(
    channel: Channel,
    signals: Vec<Signal>,
    api: &Api,
    initial_values_sender: Sender<HashMap<Signal, DataValue>>,
    buffer_size: Option<u32>,
    direction: &Direction,
) -> Result<ReceivingEnd> {
    if *api == Api::KuksaValV2 {
        let receiving_end = s_kuksa_val_v2::ReceivingEnd::new(
            channel,
            signals,
            initial_values_sender,
            buffer_size.unwrap_or(1),
            direction,
        )
        .await
        .unwrap();
        Ok(ReceivingEnd {
            receiving_end_interface: Box::new(receiving_end),
        })
    } else if *api == Api::SdvDatabrokerV1 {
        let receiving_end = s_sdv_databroker_v1::ReceivingEnd::new(channel, signals)
            .await
            .unwrap();
        Ok(ReceivingEnd {
            receiving_end_interface: Box::new(receiving_end),
        })
    } else {
        let receiving_end = s_kuksa_val_v1::ReceivingEnd::new(channel, signals, initial_values_sender)
            .await
            .unwrap();
        Ok(ReceivingEnd {
            receiving_end_interface: Box::new(receiving_end),
        })
    }
}

async fn create_unix_socket_channel(path: impl AsRef<Path>) -> Result<Channel> {
    let path_buf = PathBuf::from(path.as_ref());
    tonic::transport::Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_| {
            let path = path_buf.clone();
            // Connect to a unix socket
            UnixStream::connect(path)
        }))
        .await
        .with_context(|| format!("Failed to connect to server {}", path.as_ref().display()))
}

async fn create_tcp_channel(host: String, port: u64) -> Result<Channel> {
    let databroker_address = format!("{}:{}", host, port);

    let endpoint = tonic::transport::Channel::from_shared(databroker_address.clone())
        .with_context(|| "Failed to parse server url")?;

    let endpoint = endpoint
        .initial_stream_window_size(100 * 1024 * 1024) // 100 MB stream window size
        .initial_connection_window_size(100 * 1024 * 1024) // 100 MB connection window size
        .keep_alive_timeout(Duration::from_secs(1))
        .keep_alive_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(1));

    let channel = endpoint.connect().await.with_context(|| {
        let host = endpoint.uri().host().unwrap_or("unknown host");
        let port = endpoint
            .uri()
            .port()
            .map_or("unknown port".to_string(), |p| p.to_string());
        format!("Failed to connect to server {}:{}", host, port)
    })?;

    Ok(channel)
}

fn create_triggering_end(channel: Channel, api: &Api, direction: &Direction) -> Result<TriggeringEnd> {
    if *api == Api::KuksaValV2 {
        let triggering_end =
            p_kuksa_val_v2::TriggeringEnd::new(channel, direction).with_context(|| "Failed to setup triggering_end")?;
        Ok(TriggeringEnd {
            triggering_end_interface: Box::new(triggering_end),
        })
    } else if *api == Api::SdvDatabrokerV1 {
        let triggering_end = p_sdv_databroker_v1::TriggeringEnd::new(channel)
            .with_context(|| "Failed to setup triggering_end")?;
        Ok(TriggeringEnd {
            triggering_end_interface: Box::new(triggering_end),
        })
    } else {
        let triggering_end =
            p_kuksa_val_v1::TriggeringEnd::new(channel).with_context(|| "Failed to setup triggering_end")?;
        Ok(TriggeringEnd {
            triggering_end_interface: Box::new(triggering_end),
        })
    }
}

pub async fn perform_measurement(
    measurement_config: MeasurementConfig,
    config_groups: Vec<Group>,
    shutdown_handler: ShutdownHandler,
) -> Result<()> {
    let triggering_end_channel = match measurement_config.unix_socket_path {
        Some(ref path) => create_unix_socket_channel(path).await?,
        None => {
            create_tcp_channel(measurement_config.host.clone(), measurement_config.port).await?
        }
    };

    let receiving_end_channel = match measurement_config.unix_socket_path {
        Some(ref path) => create_unix_socket_channel(path).await?,
        None => {
            create_tcp_channel(measurement_config.host.clone(), measurement_config.port).await?
        }
    };

    // Structure to collect tokio tasks of signals groups
    let mut tasks: JoinSet<Result<MeasurementResult>> = JoinSet::new();

    for group in config_groups.clone() {
        let triggering_end_channel = triggering_end_channel.clone();

        // Initialize triggering_end
        let mut triggering_end = create_triggering_end(triggering_end_channel, &measurement_config.api, &measurement_config.direction)?;
        // Validate metadata signals
        let ve = triggering_end
            .triggering_end_interface
            .as_mut()
            .validate_signals_metadata(group.signals.as_slice())
            .await;

        let mut signals = Vec::new();
        match ve {
            Ok(vec) => signals = vec,
            Err(e) => println!("Error: {}", e),
        }
        // Initilize receiving_end and initialize initial signal values.
        let (initial_values_sender, mut initial_values_reciever) =
            tokio::sync::mpsc::channel::<HashMap<Signal, DataValue>>(10);

        let receiving_end_channel = receiving_end_channel.clone();
        let receiving_end = create_receiving_end(
            receiving_end_channel,
            signals.clone(),
            &measurement_config.api,
            initial_values_sender,
            measurement_config.buffer_size,
            &measurement_config.direction,
        )
        .await?;

        // Receive the initial signal values
        if let Some(initial_signal_values) = initial_values_reciever.recv().await {
            let result = triggering_end
                .triggering_end_interface
                .as_mut()
                .set_initial_signals_values(initial_signal_values)
                .await;
            assert!(result.is_ok());
        }

        // Create MeasurmentContext for each group
        let mut measurement_config = measurement_config.clone();

        measurement_config.interval = group.cycle_time_ms;

        let hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;
        let running_hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;

        let group_name = group.group_name.clone();

        let mut measurement_context = MeasurementContext {
            measurement_config,
            group_name: group_name.clone(),
            shutdown_handler: shutdown_handler.clone(),
            triggering_end,
            signals,
            receiving_end,
            hist,
            running_hist,
            latency_series: Vec::new(),
        };

        // Spawn a task for each group
        let start_time = SystemTime::now();
        tasks.spawn(async move {
            let (iterations_executed, signals_skipped) =
                measurement_loop(&mut measurement_context).await.unwrap();

            Ok(MeasurementResult {
                measurement_context,
                iterations_executed,
                signals_skipped,
                start_time,
            })
        });
    }

    // Initialize progress bars
    let (progress_bar, duration) = match measurement_config.duration {
        Some(duration_value) => {
            let duration_in_ms = duration_value * 1000; // Convert to milliseconds
            let progress_bar = ProgressBar::new(duration_value).with_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} seconds",
                )
                .unwrap()
                .progress_chars("=> "),
            );
            (progress_bar, Duration::from_millis(duration_in_ms)) // Initialize Duration from milliseconds
        }
        None => {
            println!("Databroker-perf running... to cancel execution please press 'Ctrl+C'");
            let progress_bar = ProgressBar::new_spinner().with_style(
                ProgressStyle::with_template("[{elapsed_precise}] [{spinner}] ")
                    .unwrap()
                    .tick_chars("⠁⠁⠉⠙⠚⠒⠂⠂⠒⠲⠴⠤⠄⠄⠤⠠⠠⠤⠦⠖⠒⠐⠐⠒⠓⠋⠉⠈⠈ "),
            );
            (progress_bar, Duration::default()) // Default duration when no duration is provided
        }
    };

    let run_forever = measurement_config.duration.is_none();

    let start_run = Instant::now();
    let progress_bar_task = progress_bar.clone();

    let shutdown_handler_clone = shutdown_handler.clone();
    // Stop the execution of tasks when the test duration is exceeded.
    task::spawn(async move {
        while (run_forever || start_run.elapsed().as_millis() < duration.as_millis())
            && shutdown_handler_clone.state.running.load(Ordering::SeqCst)
        {
            if !run_forever {
                progress_bar_task.set_position(start_run.elapsed().as_secs());
            } else if start_run.elapsed().as_millis() % 100 == 0 {
                progress_bar_task.tick();
            }
        }
        shutdown_handler_clone
            .state
            .running
            .store(false, Ordering::SeqCst);
        if shutdown_handler_clone.trigger.send(()).is_err() {
            println!("failed to trigger shutdown");
        }
    });

    // Collect measurements results from each group
    let mut measurements_results = Vec::<MeasurementResult>::new();
    while let Some(received) = tasks.join_next().await {
        match received {
            Ok(Ok(measurement_result)) => {
                measurements_results.push(measurement_result);
            }
            Ok(Err(err)) => {
                error!("{}", err.to_string());
                break;
            }
            Err(err) => {
                error!("{}", err.to_string());
                break;
            }
        }
    }

    progress_bar.finish();

    // Output results
    write_global_output(&measurement_config, &measurements_results).unwrap();

    if measurement_config.detailed_output {
        for group in config_groups {
            let measurement_result = measurements_results
                .iter()
                .find(|result| result.measurement_context.group_name == group.group_name)
                .unwrap();

            write_output(measurement_result).unwrap();
        }
    }
    Ok(())
}

async fn measurement_loop(ctx: &mut MeasurementContext) -> Result<(u64, u64)> {
    let mut iterations = 0;
    let mut skipped = 0;
    let start_run = Instant::now();

    let skip_milliseconds = ctx
        .measurement_config
        .skip_seconds
        .map(|skip_seconds| skip_seconds * 1000)
        .unwrap_or_else(|| 0);

    let mut interval_to_run = if ctx.measurement_config.interval == 0 {
        None
    } else {
        Some(tokio::time::interval(Duration::from_millis(
            ctx.measurement_config.interval.into(),
        )))
    };

    loop {
        if !ctx.shutdown_handler.state.running.load(Ordering::SeqCst) {
            break;
        }

        if let Some(interval_to_run) = interval_to_run.as_mut() {
            let mut shutdown_triggered = ctx.shutdown_handler.trigger.subscribe();
            tokio::select! {
                _ = interval_to_run.tick() => {
                }
                _ = shutdown_triggered.recv() => {
                    break;
                }
            }
        }

        let triggering_end = ctx.triggering_end.triggering_end_interface.as_ref();
        let publish_task = triggering_end.publish(&ctx.signals, iterations);

        let mut meassure_tasks: JoinSet<Result<Instant, Error>> = JoinSet::new();

        for signal in &ctx.signals {
            // TODO: return an awaitable thingie (wrapping the Receiver<Instant>)
            let receiving_end = ctx.receiving_end.receiving_end_interface.as_ref();
            let mut receiver = receiving_end.get_receiver(signal).await.unwrap();
            let mut shutdown_triggered = ctx.shutdown_handler.trigger.subscribe();

            meassure_tasks.spawn(async move {
                // Wait for notification or shutdown
                select! {
                    instant = receiver.recv() => {
                        instant.map_err(|err| Error::RecvFailed(err.to_string()))
                    }
                    _ = shutdown_triggered.recv() => {
                        Err(Error::Shutdown)
                    }
                }
            });
        }

        let published = {
            let mut shutdown_triggered = ctx.shutdown_handler.trigger.subscribe();
            select! {
                published = publish_task => published,
                _ = shutdown_triggered.recv() => {
                    Err(PublishError::Shutdown)
                }
            }
        }?;

        while let Some(received) = meassure_tasks.join_next().await {
            match received {
                Ok(Ok(received)) => {
                    if start_run.elapsed().as_millis() < skip_milliseconds.into() {
                        skipped += 1;
                        continue;
                    }
                    let latency = received
                        .duration_since(published)
                        .as_micros()
                        .try_into()
                        .unwrap();
                    ctx.hist.record(latency)?;
                    ctx.latency_series.push(latency);
                    ctx.running_hist.record(latency)?;
                }
                Ok(Err(Error::Shutdown)) => {
                    break;
                }
                Ok(Err(err)) => {
                    error!("{}", err.to_string());
                    break;
                }
                Err(err) => {
                    error!("{}", err.to_string());
                    break;
                }
            }
        }

        if ctx.shutdown_handler.state.running.load(Ordering::SeqCst) {
            iterations += 1;
        }
    }
    Ok((iterations, skipped))
}
