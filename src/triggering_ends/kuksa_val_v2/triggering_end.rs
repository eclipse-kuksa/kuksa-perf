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

use crate::config::Signal;
use crate::measure::Operation;
use crate::triggering_ends::triggering_end_trait::{Error, TriggerError, TriggeringEndInterface};
use crate::types::DataValue;

use databroker_proto::kuksa::val::v2::{
    self as proto, open_provider_stream_request,
    open_provider_stream_response::Action::{
        BatchActuateStreamRequest, GetProviderValueRequest, ProvideActuationResponse,
        ProvideSignalResponse, PublishValuesResponse, UpdateFilterRequest,
    },
};
use databroker_proto::kuksa::val::v2::{OpenProviderStreamRequest, SampleInterval};

use tokio_stream::wrappers::ReceiverStream;

use tonic::async_trait;

use log::{debug, error};

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use tonic::transport::Channel;

use std::collections::HashMap;

pub struct TriggeringEnd {
    tx: Sender<proto::OpenProviderStreamRequest>,
    metadata: HashMap<String, proto::Metadata>,
    id_to_path: HashMap<i32, String>,
    channel: Channel,
    initial_signals_values: HashMap<Signal, DataValue>,
    operation: Operation,
}

fn build_registration_signal_request(signals: Vec<i32>) -> OpenProviderStreamRequest {
    proto::OpenProviderStreamRequest {
        action: Some(open_provider_stream_request::Action::ProvideSignalRequest(
            proto::ProvideSignalRequest {
                signals_sample_intervals: signals
                    .iter()
                    .map(|signal_id| (*signal_id, SampleInterval { interval_ms: 1 }))
                    .collect(),
            },
        )),
    }
}

impl TriggeringEnd {
    pub fn new(channel: Channel, operation: &Operation) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel(10);

        match operation {
            Operation::StreamingPublish => {
                tokio::spawn(TriggeringEnd::open_provider_stream(rx, channel.clone()));
            }
            Operation::Actuate => (),
        }
        Ok(TriggeringEnd {
            tx,
            metadata: HashMap::new(),
            id_to_path: HashMap::new(),
            channel,
            initial_signals_values: HashMap::new(),
            operation: operation.clone(),
        })
    }

    async fn open_provider_stream(
        rx: Receiver<proto::OpenProviderStreamRequest>,
        channel: Channel,
    ) -> Result<(), Error> {
        let mut client = proto::val_client::ValClient::new(channel);
        match client.open_provider_stream(ReceiverStream::new(rx)).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                let task = tokio::spawn(async move {
                    while let Ok(Some(resp)) = stream.message().await {
                        match resp.action {
                            Some(ProvideActuationResponse(_)) => {}
                            Some(PublishValuesResponse(response)) => {
                                if !response.status.is_empty() {
                                    if let Some((id, value)) = response.status.iter().next() {
                                        error!(
                                            "Singal id: {} | error setting datapoint {}",
                                            id, value.message
                                        );
                                        return Err::<(), Error>(Error::TriggerError(
                                            TriggerError::SendFailure(value.message.clone()),
                                        ));
                                    }
                                }
                            }
                            Some(BatchActuateStreamRequest(_)) => {}
                            Some(ProvideSignalResponse(_)) => {}
                            Some(UpdateFilterRequest(_)) => {}
                            Some(GetProviderValueRequest(_)) => {}
                            None => {}
                        }
                    }
                    Ok::<(), Error>(())
                });
                task.await.unwrap()?
            }
            Err(err) => {
                error!("failed to setup provider stream: {}", err.message());
            }
        }
        debug!("provider::run() exiting");
        Ok(())
    }
}

#[async_trait]
impl TriggeringEndInterface for TriggeringEnd {
    async fn trigger(
        &self,
        signal_data: &[Signal],
        iteration: u64,
    ) -> Result<Instant, TriggerError> {
        match self.operation {
            Operation::StreamingPublish => {
                let data_points = if iteration == 0 {
                    HashMap::from_iter(signal_data.iter().map(|signal: &Signal| {
                        let metadata = self.metadata.get(&signal.path).unwrap();
                        let mut new_value = n_to_value(metadata.clone(), iteration).unwrap();
                        if let Some(value) = self.initial_signals_values.get(signal) {
                            if DataValue::from(&Some(new_value.clone())) == *value {
                                new_value = n_to_value(metadata.clone(), iteration + 1).unwrap();
                            }
                        }
                        (
                            metadata.id,
                            proto::Datapoint {
                                timestamp: None,
                                value: Some(new_value),
                            },
                        )
                    }))
                } else {
                    HashMap::from_iter(signal_data.iter().map(|path: &Signal| {
                        let metadata = self.metadata.get(&path.path).unwrap();
                        (
                            metadata.id,
                            proto::Datapoint {
                                timestamp: None,
                                value: Some(n_to_value(metadata.clone(), iteration + 1).unwrap()),
                            },
                        )
                    }))
                };

                let payload = proto::OpenProviderStreamRequest {
                    action: Some(open_provider_stream_request::Action::PublishValuesRequest(
                        proto::PublishValuesRequest {
                            request_id: 1_u32,
                            data_points,
                        },
                    )),
                };

                let now = Instant::now();
                self.tx
                    .send(payload)
                    .await
                    .map_err(|err| TriggerError::SendFailure(err.to_string()))?;
                Ok(now)
            }
            Operation::Actuate => {
                let actuate_requests = if iteration == 0 {
                    Vec::from_iter(signal_data.iter().map(|signal: &Signal| {
                        let metadata = self.metadata.get(&signal.path).unwrap();
                        let mut new_value = n_to_value(metadata.clone(), iteration).unwrap();
                        if let Some(value) = self.initial_signals_values.get(signal) {
                            if DataValue::from(&Some(new_value.clone())) == *value {
                                new_value = n_to_value(metadata.clone(), iteration + 1).unwrap();
                            }
                        }
                        proto::ActuateRequest {
                            signal_id: Some(proto::SignalId {
                                signal: Some(proto::signal_id::Signal::Id(metadata.id)),
                            }),
                            value: Some(new_value),
                        }
                    }))
                } else {
                    Vec::from_iter(signal_data.iter().map(|path: &Signal| {
                        let metadata = self.metadata.get(&path.path).unwrap();
                        proto::ActuateRequest {
                            signal_id: Some(proto::SignalId {
                                signal: Some(proto::signal_id::Signal::Id(metadata.id)),
                            }),
                            value: Some(n_to_value(metadata.clone(), iteration + 1).unwrap()),
                        }
                    }))
                };

                let mut client = proto::val_client::ValClient::new(self.channel.clone());
                let message = proto::BatchActuateRequest { actuate_requests };

                let now = Instant::now();
                match client.batch_actuate(tonic::Request::new(message)).await {
                    Ok(_) => Ok(now),
                    Err(err) => Err(TriggerError::SendFailure(err.to_string())),
                }
            }
        }
    }

    async fn validate_signals_metadata(
        &mut self,
        signals: &[Signal],
        operation: &Operation,
    ) -> Result<Vec<Signal>, Error> {
        let signals_vec: Vec<String> = signals.iter().map(|signal| signal.path.clone()).collect();
        let number_of_signals = signals.len();

        let mut client: proto::val_client::ValClient<Channel> =
            proto::val_client::ValClient::new(self.channel.clone())
                .max_decoding_message_size(64 * 1024 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024);

        let mut signals_response = Vec::with_capacity(signals_vec.len());

        let requests_entries: Vec<proto::ListMetadataRequest> = signals_vec
            .iter()
            .map(|path| proto::ListMetadataRequest {
                root: path.to_string(),
                filter: "".to_string(),
            })
            .collect();

        for request in requests_entries {
            let signal_path = request.root.clone();
            let response = client.list_metadata(request).await;
            match response {
                Ok(entries) => {
                    for metadata in entries.into_inner().metadata.iter() {
                        if metadata.entry_type == proto::EntryType::Sensor as i32
                            && *operation == Operation::Actuate
                        {
                            return Err(Error::TriggerError(TriggerError::NoActuator(
                                signal_path.clone(),
                            )));
                        }
                        self.metadata.insert(signal_path.clone(), metadata.clone());
                        self.id_to_path.insert(metadata.id, signal_path.clone());
                        signals_response.push(Signal {
                            path: signal_path.clone(),
                            id: Some(metadata.id),
                        });
                    }
                }
                Err(status) => {
                    // Handle the Err case without returning a value
                    eprintln!("gRPC call failed with status: {status:?}");
                }
            }
        }

        debug!(
            "received {} number of signals in metadata",
            self.metadata.len()
        );
        if self.metadata.len() < number_of_signals {
            let missing_signals: Vec<_> = signals_vec
                .iter()
                .filter(|signal| !self.metadata.contains_key(signal.as_str()))
                .collect();

            Err(Error::MetadataError(format!(
                "The following signals are missing in the databroker: {missing_signals:?}"
            )))
        } else {
            //Register signals after validation
            let _ = self
                .tx
                .send(build_registration_signal_request(
                    self.id_to_path.keys().cloned().collect(),
                ))
                .await;

            Ok(signals_response)
        }
    }

    async fn set_initial_signals_values(
        &mut self,
        initial_signals_values: HashMap<Signal, DataValue>,
    ) -> Result<(), Error> {
        self.initial_signals_values = initial_signals_values;
        Ok(())
    }
}

pub fn n_to_value(metadata: proto::Metadata, n: u64) -> Result<proto::Value, TriggerError> {
    match proto::DataType::try_from(metadata.data_type) {
        Ok(proto::DataType::String) => {
            if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::StringArray(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize].clone();
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::String(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::String(n.to_string())),
                })
            }
        }
        Ok(proto::DataType::Unspecified) => Err(TriggerError::Shutdown),
        Ok(proto::DataType::Boolean) => match n % 2 {
            0 => Ok(proto::Value {
                typed_value: Some(proto::value::TypedValue::Bool(true)),
            }),
            _ => Ok(proto::Value {
                typed_value: Some(proto::value::TypedValue::Bool(false)),
            }),
        },
        Ok(proto::DataType::Int8) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i8::MIN.into());

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i8::MAX.into());

                let mut value = min + (n as i32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int32(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Int32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Int32(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int32((n % 128) as i32)),
                })
            }
        }
        Ok(proto::DataType::Int16) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i16::MIN.into());

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i16::MAX.into());

                let mut value = min + (n as i32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int32(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Int32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Int32(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int32((n % 128) as i32)),
                })
            }
        }
        Ok(proto::DataType::Int32) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i32::MIN);

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i32::MAX);

                let mut value = min + (n as i32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int32(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Int32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Int32(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int32((n % 128) as i32)),
                })
            }
        }
        Ok(proto::DataType::Int64) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int64(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i64::MIN);

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Int64(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(i64::MAX);

                let mut value = min + (n as i64);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int64(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Int64Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Int64(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Int64((n % 128) as i64)),
                })
            }
        }
        Ok(proto::DataType::Uint8) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u8::MIN.into());

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u8::MAX.into());

                let mut value = min + (n as u32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Uint32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Uint32(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32((n % 128) as u32)),
                })
            }
        }
        Ok(proto::DataType::Uint16) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u16::MIN.into());

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u16::MAX.into());

                let mut value = min + (n as u32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Uint32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Uint32(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32((n % 128) as u32)),
                })
            }
        }
        Ok(proto::DataType::Uint32) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u32::MIN);

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint32(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u32::MAX);

                let mut value = min + (n as u32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Uint32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Uint32(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32((n % 128) as u32)),
                })
            }
        }
        Ok(proto::DataType::Uint64) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint64(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u64::MIN);

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Uint64(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(u64::MAX);

                let mut value = min + n;
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint64(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Uint64Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Uint64(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint64(n % 128)),
                })
            }
        }
        Ok(proto::DataType::Float) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Float(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(f32::MIN);

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Float(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(f32::MAX);

                let mut value = min + (n as f32);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Float(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::FloatArray(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Float(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Float((n % 128) as f32)),
                })
            }
        }
        Ok(proto::DataType::Double) => {
            if metadata.min.is_some() || metadata.max.is_some() {
                let min = metadata
                    .min
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Double(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(f64::MIN);

                let max = metadata
                    .max
                    .and_then(|value| {
                        if let Some(proto::value::TypedValue::Double(s)) = value.typed_value {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(f64::MAX);

                let mut value = min + (n as f64);
                if value > max {
                    value %= max;
                    if value < min {
                        value = min;
                    }
                }
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Double(value)),
                })
            } else if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::DoubleArray(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Double(value)),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Double((n % 128) as f64)),
                })
            }
        }
        Ok(proto::DataType::StringArray) => {
            if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::StringArray(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize].clone();
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::StringArray(
                            proto::StringArray {
                                values: vec![value],
                            },
                        )),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::StringArray(proto::StringArray {
                        values: vec![n.to_string()],
                    })),
                })
            }
        }
        Ok(proto::DataType::Uint8Array) => {
            if metadata.allowed_values.is_some() {
                let allowed = metadata.allowed_values.unwrap();
                if let Some(proto::value::TypedValue::Uint32Array(values)) = allowed.typed_value {
                    let index = n % values.values.len() as u64;
                    let value = values.values[index as usize];
                    Ok(proto::Value {
                        typed_value: Some(proto::value::TypedValue::Uint32Array(
                            proto::Uint32Array {
                                values: vec![value],
                            },
                        )),
                    })
                } else {
                    Err(TriggerError::DataTypeError)
                }
            } else {
                Ok(proto::Value {
                    typed_value: Some(proto::value::TypedValue::Uint32Array(proto::Uint32Array {
                        values: vec![(n % 128) as u32],
                    })),
                })
            }
        }
        Ok(proto::DataType::BooleanArray) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Int8Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Int16Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Int32Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Int64Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Uint16Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Uint32Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Uint64Array) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::FloatArray) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::DoubleArray) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::Timestamp) => Err(TriggerError::DataTypeError),
        Ok(proto::DataType::TimestampArray) => Err(TriggerError::DataTypeError),
        Err(_) => Err(TriggerError::MetadataError),
    }
}
