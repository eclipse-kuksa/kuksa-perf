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

use crate::providers::kuksa_val_v1::provider as kuksa_val_v1;
use crate::providers::kuksa_val_v2::provider as kuksa_val_v2;
use crate::providers::provider_trait::{ProviderInterface, PublishError};
use crate::providers::sdv_databroker_v1::provider as sdv_databroker_v1;

use crate::config::{Group, Signal};

use crate::shutdown::ShutdownHandler;
use crate::subscriber::{self, Subscriber};
use crate::types::DataValue;
use crate::utils::{write_global_output, write_output};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::{
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
};
use tokio::sync::{mpsc::Sender, RwLock};
use tokio::task;
use tokio::{select, task::JoinSet, time::Instant};
use tonic::transport::Endpoint;

#[derive(Clone, PartialEq)]
pub enum Api {
    KuksaValV1,
    KuksaValV2,
    SdvDatabrokerV1,
}

pub struct Provider {
    pub provider_interface: Box<dyn ProviderInterface>,
}

#[derive(Clone)]
pub struct MeasurementConfig {
    pub host: String,
    pub port: u64,
    pub duration: Option<u64>,
    pub interval: u32,
    pub skip_seconds: Option<u64>,
    pub api: Api,
    pub detailed_output: bool,
}

pub struct MeasurementContext {
    pub measurement_config: MeasurementConfig,
    pub group_name: String,
    pub shutdown_handler: Arc<RwLock<ShutdownHandler>>,
    pub provider: Provider,
    pub signals: Vec<Signal>,
    pub subscriber: Subscriber,
    pub hist: Histogram<u64>,
    pub running_hist: Histogram<u64>,
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

async fn setup_subscriber(
    endpoint: &Endpoint,
    signals: Vec<Signal>,
    api: &Api,
    initial_values_sender: Sender<HashMap<String, DataValue>>,
) -> Result<Subscriber> {
    let subscriber_channel = endpoint.connect().await.with_context(|| {
        let host = endpoint.uri().host().unwrap_or("unknown host");
        let port = endpoint
            .uri()
            .port()
            .map_or("unknown port".to_string(), |p| p.to_string());
        format!("Failed to connect to server {}:{}", host, port)
    })?;

    let subscriber =
        subscriber::Subscriber::new(subscriber_channel, signals, api, initial_values_sender)
            .await?;

    Ok(subscriber)
}

fn create_databroker_endpoint(host: String, port: u64) -> Result<Endpoint> {
    let databroker_address = format!("{}:{}", host, port);

    let endpoint = tonic::transport::Channel::from_shared(databroker_address.clone())
        .with_context(|| "Failed to parse server url")?;

    let endpoint = endpoint
        .initial_stream_window_size(100 * 1024 * 1024) // 100 MB stream window size
        .initial_connection_window_size(100 * 1024 * 1024) // 100 MB connection window size
        .keep_alive_timeout(Duration::from_secs(1))
        .keep_alive_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(1));

    Ok(endpoint)
}

async fn create_provider(endpoint: &Endpoint, api: &Api) -> Result<Provider> {
    let channel = endpoint.connect().await.with_context(|| {
        let host = endpoint.uri().host().unwrap_or("unknown host");
        let port = endpoint
            .uri()
            .port()
            .map_or("unknown port".to_string(), |p| p.to_string());
        format!("Failed to connect to server {}:{}", host, port)
    })?;

    if *api == Api::KuksaValV2 {
        let provider =
            kuksa_val_v2::Provider::new(channel).with_context(|| "Failed to setup provider")?;
        Ok(Provider {
            provider_interface: Box::new(provider),
        })
    } else if *api == Api::SdvDatabrokerV1 {
        let provider = sdv_databroker_v1::Provider::new(channel)
            .with_context(|| "Failed to setup provider")?;
        Ok(Provider {
            provider_interface: Box::new(provider),
        })
    } else {
        let provider =
            kuksa_val_v1::Provider::new(channel).with_context(|| "Failed to setup provider")?;
        Ok(Provider {
            provider_interface: Box::new(provider),
        })
    }
}

pub async fn perform_measurement(
    measurement_config: MeasurementConfig,
    config_groups: Vec<Group>,
    shutdown_handler: ShutdownHandler,
) -> Result<()> {
    let provider_endpoint =
        create_databroker_endpoint(measurement_config.host.clone(), measurement_config.port)?;

    let subscriber_endpoint =
        create_databroker_endpoint(measurement_config.host.clone(), measurement_config.port)?;

    // Create references to be used among tokio::tasks
    let shutdown_handler_ref = Arc::new(RwLock::new(shutdown_handler));

    // Structure to collect tokio tasks of signals groups
    let mut tasks: JoinSet<Result<MeasurementResult>> = JoinSet::new();

    for group in config_groups.clone() {
        // Initialize provider
        let mut provider = create_provider(&provider_endpoint, &measurement_config.api).await?;

        // Validate metadata signals
        let signals = provider
            .provider_interface
            .as_mut()
            .validate_signals_metadata(group.signals.as_slice())
            .await
            .unwrap();

        // Initilize subscriber and initialize initial signal values.
        let (initial_values_sender, mut initial_values_reciever) =
            tokio::sync::mpsc::channel::<HashMap<String, DataValue>>(10);

        let subscriber = setup_subscriber(
            &subscriber_endpoint,
            signals,
            &measurement_config.api,
            initial_values_sender,
        )
        .await?;

        // Receive the initial signal values
        if let Some(initial_signal_values) = initial_values_reciever.recv().await {
            let result = provider
                .provider_interface
                .as_mut()
                .set_initial_signals_values(initial_signal_values)
                .await;
            assert!(result.is_ok());
        }

        // Create MeasurmentContext for each group
        let mut measurement_config = measurement_config.clone();

        measurement_config.interval = group.cycle_time_microseconds;

        let hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;
        let running_hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;

        let group_name = group.group_name.clone();

        let mut measurement_context = MeasurementContext {
            measurement_config,
            group_name: group_name.clone(),
            shutdown_handler: Arc::clone(&shutdown_handler_ref),
            provider,
            signals: group.signals,
            subscriber,
            hist,
            running_hist,
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

    // Stop the execution of tasks when the test duration is exceeded.
    task::spawn(async move {
        while (run_forever || start_run.elapsed().as_millis() < duration.as_millis())
            && shutdown_handler_ref
                .read()
                .await
                .state
                .running
                .load(Ordering::SeqCst)
        {
            if !run_forever {
                progress_bar_task.set_position(start_run.elapsed().as_secs());
            } else if start_run.elapsed().as_millis() % 100 == 0 {
                progress_bar_task.tick();
            }
        }
        shutdown_handler_ref
            .write()
            .await
            .state
            .running
            .store(false, Ordering::SeqCst);
        if shutdown_handler_ref.write().await.trigger.send(()).is_err() {
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
        Some(tokio::time::interval(Duration::from_micros(
            ctx.measurement_config.interval.into(),
        )))
    };

    loop {
        if !ctx
            .shutdown_handler
            .read()
            .await
            .state
            .running
            .load(Ordering::SeqCst)
        {
            break;
        }

        if let Some(interval_to_run) = interval_to_run.as_mut() {
            let mut shutdown_triggered = ctx.shutdown_handler.write().await.trigger.subscribe();
            tokio::select! {
                _ = interval_to_run.tick() => {
                }
                _ = shutdown_triggered.recv() => {
                    break;
                }
            }
        }

        let provider = ctx.provider.provider_interface.as_ref();
        let publish_task = provider.publish(&ctx.signals, iterations);

        let mut subscriber_tasks: JoinSet<Result<Instant, subscriber::Error>> = JoinSet::new();

        for signal in &ctx.signals {
            // TODO: return an awaitable thingie (wrapping the Receiver<Instant>)
            let mut sub = ctx.subscriber.wait_for(&signal.path)?;
            let mut shutdown_triggered = ctx.shutdown_handler.write().await.trigger.subscribe();

            subscriber_tasks.spawn(async move {
                // Wait for notification or shutdown
                select! {
                    instant = sub.recv() => {
                        instant.map_err(|err| subscriber::Error::RecvFailed(err.to_string()))
                    }
                    _ = shutdown_triggered.recv() => {
                        Err(subscriber::Error::Shutdown)
                    }
                }
            });
        }

        let published = {
            let mut shutdown_triggered = ctx.shutdown_handler.write().await.trigger.subscribe();
            select! {
                published = publish_task => published,
                _ = shutdown_triggered.recv() => {
                    Err(PublishError::Shutdown)
                }
            }
        }?;

        while let Some(received) = subscriber_tasks.join_next().await {
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
                    ctx.running_hist.record(latency)?;
                }
                Ok(Err(subscriber::Error::Shutdown)) => {
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

        iterations += 1;
    }
    Ok((iterations, skipped))
}
