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

use databroker_proto::kuksa::val::v2 as kuksa_val_v2;
use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    time::Instant,
};
use tonic::{async_trait, transport::Channel};

use crate::subscribers::subscriber_trait::{Error, SubscriberInterface};

use crate::config::Signal;

use crate::types::DataValue;

pub struct Subscriber {
    signals: Arc<HashMap<i32, Sender<Instant>>>,
}

impl Subscriber {
    pub async fn new(
        channel: Channel,
        signals: Vec<Signal>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
        buffer_size: u32,
    ) -> Result<Self, Error> {
        let signals_c = Arc::new(HashMap::from_iter(signals.clone().into_iter().map(
            |signal| {
                let (sender, _) = broadcast::channel(32);
                (signal.id.unwrap(), sender)
            },
        )));

        Subscriber::start(
            channel,
            signals,
            signals_c.clone(),
            initial_values_sender,
            buffer_size,
        )
        .await?;

        Ok(Self { signals: signals_c })
    }

    async fn start(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<i32, Sender<Instant>>>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
        buffer_size: u32,
    ) -> Result<(), Error> {
        Self::handle_kuksa_val_v2(
            channel,
            signals,
            signals_map,
            initial_values_sender,
            buffer_size,
        )
        .await
    }

    async fn handle_kuksa_val_v2(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<i32, Sender<Instant>>>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
        buffer_size: u32,
    ) -> Result<(), Error> {
        let mut ids = Vec::with_capacity(signals.len());
        for signal in signals {
            ids.push(signal.id.unwrap());
        }

        let mut client = kuksa_val_v2::val_client::ValClient::new(channel);

        let args = tonic::Request::new(kuksa_val_v2::SubscribeByIdRequest {
            buffer_size,
            signal_ids: ids,
        });

        match client.subscribe_by_id(args).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                // Ignore first message as the first notification is not triggered by a provider
                // but instead contains the current value.
                let first_current_value = stream
                    .message()
                    .await
                    .map_err(|err| Error::SubscriptionFailed(err.to_string()))?;

                let mut initial_signals_value: HashMap<Signal, DataValue> = HashMap::new();
                for (id, datapoint) in first_current_value.unwrap().entries {
                    initial_signals_value.insert(
                        Signal {
                            path: "".to_owned(),
                            id: Some(id),
                        },
                        DataValue::from(&datapoint.value),
                    );
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
                                .entries
                                .iter()
                                .filter_map(|(id, _datapoint)| {
                                    signals_map.get(id).map(|sender| (id, sender))
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
impl SubscriberInterface for Subscriber {
    async fn wait_for(&self, signal: &Signal) -> Result<Receiver<Instant>, Error> {
        match self.signals.get(&signal.id.unwrap()) {
            Some(sender) => Ok(sender.subscribe()),
            None => Err(Error::SignalNotFound(signal.id.unwrap().to_string())),
        }
    }
}
