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

use crate::config::Signal;

use crate::shutdown::ShutdownHandler;
use crate::subscriber::{self, Subscriber};
use crate::utils::write_output;
use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use std::fmt;
use std::{
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
};
use tokio::{select, task::JoinSet, time::Instant};
use tonic::transport::Endpoint;

#[derive(Clone, PartialEq)]
pub enum Api {
    KuksaValV1,
    KuksaValV2,
    SdvDatabrokerV1,
}

pub struct MeasurementConfig {
    pub endpoint: Endpoint,
    pub config_signals: Vec<Signal>,
    pub iterations: u64,
    pub interval: u16,
    pub skip: u64,
    pub api: Api,
    pub run_forever: bool,
}

impl fmt::Display for Api {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Api::KuksaValV1 => write!(f, "KuksaValV1"),
            Api::KuksaValV2 => write!(f, "KuksaValV2"),
            Api::SdvDatabrokerV1 => write!(f, "SdvDatabrokerV1"),
        }
    }
}

async fn setup_subscriber(
    endpoint: &Endpoint,
    signals: Vec<Signal>,
    api: &Api,
) -> Result<Subscriber> {
    let subscriber_channel = endpoint.connect().await.with_context(|| {
        let host = endpoint.uri().host().unwrap_or("unknown host");
        let port = endpoint
            .uri()
            .port()
            .map_or("unknown port".to_string(), |p| p.to_string());
        format!("Failed to connect to server {}:{}", host, port)
    })?;

    let subscriber = subscriber::Subscriber::new(subscriber_channel, signals, api).await?;

    Ok(subscriber)
}

struct Provider {
    pub provider_interface: Box<dyn ProviderInterface>,
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
    shutdown_handler: ShutdownHandler,
) -> Result<()> {
    let mut provider =
        create_provider(&measurement_config.endpoint, &measurement_config.api).await?;

    let signals = provider
        .provider_interface
        .as_mut()
        .validate_signals_metadata(&measurement_config.config_signals)
        .await
        .unwrap();

    let subscriber = setup_subscriber(
        &measurement_config.endpoint,
        signals.clone(),
        &measurement_config.api,
    )
    .await?;

    let hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;
    let running_hist = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000 * 1000, 3)?;

    let start_time = SystemTime::now();

    let progress = if measurement_config.run_forever {
        ProgressBar::new_spinner().with_style(
            // TODO: Add average latency etc...
            ProgressStyle::with_template("[{elapsed_precise}] {wide_msg} {pos:>7} iterations")
                .unwrap(),
        )
    } else {
        ProgressBar::new(measurement_config.iterations).with_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {msg} [{wide_bar}] {pos:>7}/{len:7} iterations",
            )
            .unwrap()
            .progress_chars("=> "),
        )
    };

    let api = measurement_config.api.clone();

    let mut measurement_context = MeasurementContext {
        measurement_config,
        shutdown_handler,
        provider,
        signals,
        subscriber,
        progress,
        hist,
        running_hist,
    };

    let (iterations, skipped) = measurement_loop(&mut measurement_context).await?;

    measurement_context.progress.finish();

    write_output(
        start_time,
        measurement_context.measurement_config.interval,
        iterations,
        measurement_context.signals.len() as u64,
        skipped,
        measurement_context.hist,
        api,
    )
    .await?;

    Ok(())
}

struct MeasurementContext {
    measurement_config: MeasurementConfig,
    shutdown_handler: ShutdownHandler,
    provider: Provider,
    signals: Vec<Signal>,
    subscriber: Subscriber,
    progress: ProgressBar,
    hist: Histogram<u64>,
    running_hist: Histogram<u64>,
}

async fn measurement_loop(ctx: &mut MeasurementContext) -> Result<(u64, u64)> {
    let mut iterations = 0;
    let mut skipped = 0;
    let mut last_running_hist = Instant::now();
    let mut interval_to_run = if ctx.measurement_config.interval == 0 {
        None
    } else {
        Some(tokio::time::interval(Duration::from_millis(
            ctx.measurement_config.interval.into(),
        )))
    };

    loop {
        if !ctx.measurement_config.run_forever && iterations >= ctx.measurement_config.iterations
            || !ctx.shutdown_handler.state.running.load(Ordering::SeqCst)
        {
            break;
        }

        if last_running_hist.elapsed().as_millis() >= 500 {
            ctx.progress.set_message(format!(
                "Current latency: {:.3} ms",
                ctx.running_hist.mean() / 1000.
            ));
            ctx.running_hist.reset();
            last_running_hist = Instant::now();
        }

        if let Some(interval_to_run) = interval_to_run.as_mut() {
            interval_to_run.tick().await;
        }

        let provider = ctx.provider.provider_interface.as_ref();
        let publish_task = provider.publish(&ctx.signals, iterations);

        let mut subscriber_tasks: JoinSet<Result<Instant, subscriber::Error>> = JoinSet::new();

        for signal in &ctx.signals {
            // TODO: return an awaitable thingie (wrapping the Receiver<Instant>)
            let mut sub = ctx.subscriber.wait_for(&signal.path)?;
            let mut shutdown_triggered = ctx.shutdown_handler.trigger.subscribe();

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
            let mut shutdown_triggered = ctx.shutdown_handler.trigger.subscribe();
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
                    if iterations < ctx.measurement_config.skip {
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
        ctx.progress.set_position(iterations);
    }
    Ok((iterations, skipped))
}
