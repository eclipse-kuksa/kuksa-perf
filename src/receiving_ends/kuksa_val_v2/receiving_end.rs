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

use databroker_proto::kuksa::val::v2::{
    self as kuksa_val_v2, open_provider_stream_response::Action::{
        BatchActuateStreamRequest, ProvideActuationResponse, PublishValuesResponse,
    }, SignalId
};
use std::{collections::HashMap, sync::Arc};

use log::{error, info};
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    time::Instant,
};
use tonic::{async_trait, transport::Channel};

use crate::{config::Signal, measure::Direction, receiving_ends::receiving_end_trait::{Error, ReceivingEndInterface}};

use crate::types::DataValue;

pub struct ReceivingEnd {
    signals: Arc<HashMap<i32, Sender<Instant>>>,
}

impl ReceivingEnd {
    pub async fn new(
        channel: Channel,
        signals: Vec<Signal>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
        buffer_size: u32,
        direction: &Direction,
    ) -> Result<Self, Error> {
        let signals_c = Arc::new(HashMap::from_iter(signals.clone().into_iter().map(
            |signal| {
                let (sender, _) = broadcast::channel(32);
                (signal.id.unwrap(), sender)
            },
        )));

        ReceivingEnd::start(
            channel,
            signals,
            signals_c.clone(),
            initial_values_sender,
            buffer_size,
            direction,
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
        direction: &Direction,
    ) -> Result<(), Error> {
        Self::handle_kuksa_val_v2(
            channel,
            signals,
            signals_map,
            initial_values_sender,
            buffer_size,
            direction,
        )
        .await
    }

    async fn handle_kuksa_val_v2(
        channel: Channel,
        signals: Vec<Signal>,
        signals_map: Arc<HashMap<i32, Sender<Instant>>>,
        initial_values_sender: tokio::sync::mpsc::Sender<HashMap<Signal, DataValue>>,
        buffer_size: u32,
        direction: &Direction
    ) -> Result<(), Error> {
        let mut client = kuksa_val_v2::val_client::ValClient::new(channel);

        let mut ids = Vec::with_capacity(signals.len());
        let mut ids_: Vec<SignalId> = Vec::with_capacity(signals.len());
        
        match direction{
            Direction::Read => {
                for signal in signals {
                    ids.push(signal.id.unwrap());
                }
        
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
            },
            Direction::Write => {
                for signal in signals {
                    ids_.push(SignalId {
                        signal: Some(kuksa_val_v2::signal_id::Signal::Id(signal.id.unwrap())),
                    });
                }
        
                let args = kuksa_val_v2::OpenProviderStreamRequest {
                    action: Some(
                        kuksa_val_v2::open_provider_stream_request::Action::ProvideActuationRequest(
                            kuksa_val_v2::ProvideActuationRequest {
                                actuator_identifiers: ids_,
                            },
                        ),
                    ),
                };
        
                // Wrap the stream in a tonic::Request
                let request = tonic::Request::new(tokio_stream::iter(vec![args]));
        
                // Open the stream on the server
                tokio::spawn(async move {
                    match client.open_provider_stream(request).await {
                        Ok(response) => {
                            let mut stream = response.into_inner();
            
                            let task = tokio::spawn(async move {
                                while let Ok(Some(resp)) = stream.message().await {
                                    match resp.action {
                                        Some(ProvideActuationResponse(_)) => {}
                                        Some(PublishValuesResponse(_)) => {}
                                        Some(BatchActuateStreamRequest(req)) => {
                                            let now = Instant::now();
                                            for actuate_request in req.actuate_requests{
                                                if let Some(signal_id) = actuate_request.signal_id{
                                                    if let Some(signal) = signal_id.signal{
                                                        let id_i32 = match signal{
                                                            kuksa_val_v2::signal_id::Signal::Id(id) => id,
                                                            kuksa_val_v2::signal_id::Signal::Path(_) => todo!(),
                                                        };
                                                        if let Some(sender) = signals_map.get(&id_i32){
                                                            let _ = sender.send(now);
                                                        }
                                                    }
                                                    
                                                } else {
                                                    error!("No SignalId found");
                                                }
                                            }
                                        }
                                        None => {}
                                    }
                                }
                                Ok::<(), Error>(())
                            });
                            task.await.unwrap()
                        }
                        Err(err) =>  Err(Error::SubscriptionFailed(err.to_string())),
                    }
                });
                Ok(())
            },
        }
    }
}

#[async_trait]
impl ReceivingEndInterface for ReceivingEnd {
    async fn get_receiver(&self, signal: &Signal) -> Result<Receiver<Instant>, Error> {
        match self.signals.get(&signal.id.unwrap()) {
            Some(sender) => Ok(sender.subscribe()),
            None => Err(Error::SignalNotFound(signal.id.unwrap().to_string())),
        }
    }
}
