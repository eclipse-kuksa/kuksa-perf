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
use std::{collections::HashMap, sync::Arc};

use crate::receiving_ends::receiving_end_trait::{Error, ReceivingEndInterface};

use log::info;
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    time::Instant,
};
use tonic::{async_trait, transport::Channel};

use crate::config::Signal;

use crate::types::DataValue;

pub struct ReceivingEnd {
    signals: Arc<HashMap<String, Sender<Instant>>>,
}

impl ReceivingEnd {
    pub async fn new(
        channel: Channel,
        signals: Vec<Signal>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
    ) -> Result<Self, Error> {
        let signals_c = Arc::new(HashMap::from_iter(signals.clone().into_iter().map(
            |signal| {
                let (sender, _) = broadcast::channel(32);
                (signal.path, sender)
            },
        )));

        ReceivingEnd::start(channel, signals, signals_c.clone(), initial_values_sender).await?;

        Ok(Self { signals: signals_c })
    }

    async fn start(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
    ) -> Result<(), Error> {
        Self::handle_kuksa_val_v1(channel, signals, signals_map, initial_values_sender).await
    }

    async fn handle_kuksa_val_v1(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<String, Sender<Instant>>>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
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
                let first_current_value = stream
                    .message()
                    .await
                    .map_err(|err| Error::SubscriptionFailed(err.to_string()))?;

                let mut initial_signals_value: HashMap<Signal, DataValue> = HashMap::new();
                for entry in first_current_value.unwrap().updates {
                    if let Some(value) = entry.entry.clone().unwrap().value {
                        initial_signals_value.insert(
                            Signal {
                                path: entry.entry.unwrap().path.clone(),
                                id: None,
                            },
                            DataValue::from(&value.value),
                        );
                    }
                }

                let result = initial_values_sender.send(initial_signals_value).await;
                assert!(result.is_ok());

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
