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

use databroker_proto::sdv::databroker::v1 as sdv_databroker_v1;
use std::{collections::HashMap, sync::Arc};

use crate::receiving_ends::receiving_end_trait::{Error, ReceivingEndInterface};

use log::info;
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    time::Instant,
};
use tonic::{async_trait, transport::Channel};

use crate::config::Signal;

pub struct ReceivingEnd {
    signals: Arc<HashMap<String, Sender<Instant>>>,
}

impl ReceivingEnd {
    pub async fn new(channel: Channel, signals: Vec<Signal>) -> Result<Self, Error> {
        let signals_c = Arc::new(HashMap::from_iter(signals.clone().into_iter().map(
            |signal| {
                let (sender, _) = broadcast::channel(32);
                (signal.path, sender)
            },
        )));

        ReceivingEnd::start(channel, signals, signals_c.clone()).await?;

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

    async fn start(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
    ) -> Result<(), Error> {
        Self::handle_sdv_databroker_v1(channel, signals, signals_map).await
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
}

#[async_trait]
impl ReceivingEndInterface for ReceivingEnd {
    async fn get_receiver(&self, signal: &Signal) -> Result<Receiver<Instant>, Error> {
        match self.signals.get(&signal.path) {
            Some(sender) => Ok(sender.subscribe()),
            None => Err(Error::SignalNotFound(signal.path.to_string())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_query_from_signal() {
        let query = ReceivingEnd::query_from_signals(
            vec![Signal {
                path: "Vehicle.Speed".to_owned(),
                id: None,
            }]
            .into_iter(),
        );
        assert_eq!(query, "SELECT Vehicle.Speed")
    }

    #[test]
    fn test_query_from_signals() {
        let query = ReceivingEnd::query_from_signals(
            vec![
                Signal {
                    path: "Vehicle.Speed".to_owned(),
                    id: None,
                },
                Signal {
                    path: "Vehicle.Width".to_owned(),
                    id: None,
                },
            ]
            .into_iter(),
        );
        assert_eq!(query, "SELECT Vehicle.Speed,Vehicle.Width")
    }
}
