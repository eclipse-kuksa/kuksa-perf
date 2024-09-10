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

use databroker_proto::kuksa::val::v1 as kuksa_val_v1;
use databroker_proto::kuksa::val::v2 as kuksa_val_v2;
use databroker_proto::sdv::databroker::v1 as sdv_databroker_v1;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

use log::{error, info};
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    time::Instant,
};
use tonic::transport::Channel;

use crate::{config::Signal, measure::Api};

pub struct Subscriber {
    signals: Arc<HashMap<String, Sender<Instant>>>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("subscription failed: {0}")]
    SubscriptionFailed(String),

    #[error("waiting for a signal update failed: {0}")]
    RecvFailed(String),

    #[error("signal '{0}' not found")]
    SignalNotFound(String),

    #[error("Received shutdown signal")]
    Shutdown,
}

impl Subscriber {
    pub async fn new(channel: Channel, signals: Vec<Signal>, api: &Api) -> Result<Self, Error> {
        let signals_c = Arc::new(HashMap::from_iter(signals.clone().into_iter().map(
            |signal| {
                let (sender, _) = broadcast::channel(32);
                (signal.path, sender)
            },
        )));

        Subscriber::start(channel, signals, signals_c.clone(), api).await?;

        Ok(Self { signals: signals_c })
    }

    fn query_from_signals(signals: impl Iterator<Item = Signal>) -> String {
        format!(
            "SELECT {}",
            signals
                .map(|signal| signal.path)
                .collect::<Vec<String>>()
                .as_slice()
                .join(",")
        )
    }

    // pub async fn wait_for(&self, path: &str) -> Result<Instant, Error> {
    //     let mut subscription = match self.signals.get(path) {
    //         Some(sender) => Ok(sender.subscribe()),
    //         None => Err(Error::SignalNotFound(path.to_string())),
    //     }?;

    //     Ok(subscription
    //         .recv()
    //         .await
    //         .map_err(|err| Error::RecvError(err.to_string()))?)
    // }

    pub fn wait_for(&self, path: &str) -> Result<Receiver<Instant>, Error> {
        match self.signals.get(path) {
            Some(sender) => Ok(sender.subscribe()),
            None => Err(Error::SignalNotFound(path.to_string())),
        }
    }

    async fn start(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
        api: &Api,
    ) -> Result<(), Error> {
        if *api == Api::SdvDatabrokerV1 {
            Self::handle_sdv_databroker_v1(channel, signals, signals_map).await
        } else if *api == Api::KuksaValV1 {
            Self::handle_kuksa_val_v1(channel, signals, signals_map).await
        } else {
            Self::handle_kuksa_val_v2(channel, signals, signals_map).await
        }
    }

    async fn handle_sdv_databroker_v1(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
    ) -> Result<(), Error> {
        let query = Self::query_from_signals(signals.clone().into_iter());

        let mut client = sdv_databroker_v1::broker_client::BrokerClient::new(channel);

        let args = tonic::Request::new(sdv_databroker_v1::SubscribeRequest { query });

        match client.subscribe(args).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                // Ignore first message as the first notification is not triggered by a provider
                // but instead contains the current value.
                let _ = stream
                    .message()
                    .await
                    .map_err(|err| Error::SubscriptionFailed(err.to_string()))?;

                tokio::spawn(async move {
                    loop {
                        if let Some(update) = stream
                            .message()
                            .await
                            .map_err(|err| Error::SubscriptionFailed(err.to_string()))?
                        {
                            let now = Instant::now();
                            update
                                .fields
                                .iter()
                                .filter_map(|(path, _datapoint)| {
                                    signals_map.get(path).map(|sender| (path, sender))
                                })
                                .for_each(|(_, sender)| {
                                    // If no one is subscribed, the send fails, which is fine.
                                    let _ = sender.send(now);
                                });
                        } else {
                            info!("Server gone. Subscription stopped");
                            break;
                        }
                    }
                    Ok::<(), Error>(())
                });
                Ok(())
            }
            Err(err) => Err(Error::SubscriptionFailed(err.to_string())),
        }
    }

    async fn handle_kuksa_val_v1(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
    ) -> Result<(), Error> {
        let entries: Vec<kuksa_val_v1::SubscribeEntry> = signals
            .iter()
            .map(|signal| kuksa_val_v1::SubscribeEntry {
                path: signal.path.clone(),
                view: kuksa_val_v1::View::CurrentValue as i32,
                fields: vec![kuksa_val_v1::Field::Value as i32],
            })
            .collect();

        let mut client = kuksa_val_v1::val_client::ValClient::new(channel);

        let args = tonic::Request::new(kuksa_val_v1::SubscribeRequest { entries });

        match client.subscribe(args).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                // Ignore first message as the first notification is not triggered by a provider
                // but instead contains the current value.
                let _ = stream
                    .message()
                    .await
                    .map_err(|err| Error::SubscriptionFailed(err.to_string()))?;

                tokio::spawn(async move {
                    loop {
                        if let Some(update) = stream
                            .message()
                            .await
                            .map_err(|err| Error::SubscriptionFailed(err.to_string()))?
                        {
                            let now = Instant::now();
                            update
                                .updates
                                .iter()
                                .filter_map(|update| {
                                    update
                                        .entry
                                        .as_ref()
                                        .and_then(|entry| signals_map.get(&entry.path))
                                })
                                .for_each(|sender| {
                                    // If no one is subscribed, the send fails, which is fine.
                                    let _ = sender.send(now);
                                });
                        } else {
                            info!("Server gone. Subscription stopped");
                            break;
                        }
                    }
                    Ok::<(), Error>(())
                });
                Ok(())
            }
            Err(err) => Err(Error::SubscriptionFailed(err.to_string())),
        }
    }

    async fn handle_kuksa_val_v2(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
    ) -> Result<(), Error> {
        let mut paths = Vec::with_capacity(signals.len());
        for signal in signals {
            paths.push(signal.path);
        }

        let mut client = kuksa_val_v2::val_client::ValClient::new(channel);

        let args = tonic::Request::new(kuksa_val_v2::SubscribeRequest {
            signal_paths: paths,
        });

        match client.subscribe(args).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                // Ignore first message as the first notification is not triggered by a provider
                // but instead contains the current value.
                let _ = stream
                    .message()
                    .await
                    .map_err(|err| Error::SubscriptionFailed(err.to_string()))?;

                tokio::spawn(async move {
                    loop {
                        if let Some(update) = stream
                            .message()
                            .await
                            .map_err(|err| Error::SubscriptionFailed(err.to_string()))?
                        {
                            let now = Instant::now();
                            update
                                .entries
                                .iter()
                                .filter_map(|(path, _datapoint)| {
                                    signals_map.get(path).map(|sender| (path, sender))
                                })
                                .for_each(|(_, sender)| {
                                    // If no one is subscribed, the send fails, which is fine.
                                    let _ = sender.send(now);
                                });
                        } else {
                            info!("Server gone. Subscription stopped");
                            break;
                        }
                    }
                    Ok::<(), Error>(())
                });
                Ok(())
            }
            Err(err) => Err(Error::SubscriptionFailed(err.to_string())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_query_from_signal() {
        let query = Subscriber::query_from_signals(
            vec![Signal {
                path: "Vehicle.Speed".to_owned(),
            }]
            .into_iter(),
        );
        assert_eq!(query, "SELECT Vehicle.Speed")
    }

    #[test]
    fn test_query_from_signals() {
        let query = Subscriber::query_from_signals(
            vec![
                Signal {
                    path: "Vehicle.Speed".to_owned(),
                },
                Signal {
                    path: "Vehicle.Width".to_owned(),
                },
            ]
            .into_iter(),
        );
        assert_eq!(query, "SELECT Vehicle.Speed,Vehicle.Width")
    }
}
