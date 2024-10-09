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
use crate::providers::provider_trait::{Error, ProviderInterface, PublishError};
use crate::types::DataValue;
use databroker_proto::kuksa::val::v1::{self as proto, value_restriction};

use tokio_stream::wrappers::ReceiverStream;

use tonic::async_trait;

use log::{debug, error};

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use tonic::transport::Channel;

use std::collections::HashMap;

pub struct Provider {
    tx: Sender<proto::StreamedUpdateRequest>,
    metadata: HashMap<String, Metadata>,
    channel: Channel,
    initial_signals_values: HashMap<String, DataValue>,
}

pub struct Metadata {
    data_type: proto::DataType,
    allowed: Option<Vec<String>>,
}

impl Provider {
    pub fn new(channel: Channel) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(Provider::run(rx, channel.clone()));
        Ok(Provider {
            tx,
            metadata: HashMap::new(),
            channel,
            initial_signals_values: HashMap::new(),
        })
    }

    async fn run(
        rx: Receiver<proto::StreamedUpdateRequest>,
        channel: Channel,
    ) -> Result<(), Error> {
        let mut client = proto::val_client::ValClient::new(channel);

        match client.streamed_update(ReceiverStream::new(rx)).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                while let Ok(message) = stream.message().await {
                    match message {
                        Some(message) => {
                            for error in message.errors {
                                let path = error.path;
                                let error_code = error.error.unwrap().code;
                                error!("{}: error setting datapoint {}", error_code, path)
                            }
                        }
                        None => {
                            debug!("stream to databroker closed");
                            break;
                        }
                    }
                }
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
impl ProviderInterface for Provider {
    async fn publish(
        &self,
        signal_data: &[Signal],
        iteration: u64,
    ) -> Result<Instant, PublishError> {
        let updates = if iteration == 0 {
            Vec::from_iter(signal_data.iter().map(|signal| {
                let metadata = self.metadata.get(&signal.path).unwrap();
                let mut new_value = n_to_value(metadata, iteration).unwrap();
                if let Some(value) = self.initial_signals_values.get(&signal.path) {
                    if DataValue::from(&Some(new_value.clone())) == *value {
                        new_value = n_to_value(metadata, iteration + 1).unwrap();
                    }
                }
                proto::EntryUpdate {
                    entry: Some(proto::DataEntry {
                        path: signal.path.clone(),
                        value: Some(proto::Datapoint {
                            timestamp: None,
                            value: Some(new_value),
                        }),
                        actuator_target: None,
                        metadata: None,
                    }),
                    fields: vec![proto::Field::Value.into()],
                }
            }))
        } else {
            Vec::from_iter(signal_data.iter().map(|signal| {
                let metadata = self.metadata.get(&signal.path).unwrap();
                proto::EntryUpdate {
                    entry: Some(proto::DataEntry {
                        path: signal.path.clone(),
                        value: Some(proto::Datapoint {
                            timestamp: None,
                            value: Some(n_to_value(metadata, iteration + 1).unwrap()),
                        }),
                        actuator_target: None,
                        metadata: None,
                    }),
                    fields: vec![proto::Field::Value.into()],
                }
            }))
        };

        let payload = proto::StreamedUpdateRequest { updates };

        let now = Instant::now();
        self.tx
            .send(payload)
            .await
            .map_err(|err| PublishError::SendFailure(err.to_string()))?;
        Ok(now)
    }

    async fn validate_signals_metadata(
        &mut self,
        signals: &[Signal],
    ) -> Result<Vec<Signal>, Error> {
        let signals: Vec<String> = signals.iter().map(|signal| signal.path.clone()).collect();
        let number_of_signals = signals.len();

        let mut client: proto::val_client::ValClient<Channel> =
            proto::val_client::ValClient::new(self.channel.clone())
                .max_decoding_message_size(64 * 1024 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024);

        let mut signals_response = Vec::with_capacity(signals.len());

        let entries: Vec<proto::EntryRequest> = signals
            .iter()
            .map(|path| proto::EntryRequest {
                path: path.to_string(),
                view: proto::View::Metadata.into(),
                fields: vec![],
            })
            .collect();

        let response = client
            .get(proto::GetRequest { entries })
            .await
            .map_err(|err| Error::MetadataError(format!("failed to fetch metadata: {}", err)))?;

        for entry in response.into_inner().entries.iter() {
            if let Some(metadata) = &entry.metadata {
                let data_type = metadata.data_type();
                let allowed =
                    metadata
                        .value_restriction
                        .as_ref()
                        .and_then(|restriction| match &restriction.r#type {
                            Some(value_restriction::Type::String(allowed)) => {
                                Some(allowed.allowed_values.clone())
                            }
                            _ => None,
                        });
                self.metadata
                    .insert(entry.path.clone(), Metadata { data_type, allowed });
            }
            signals_response.push(Signal {
                path: entry.path.clone(),
            });
        }

        debug!(
            "received {} number of signals in metadata",
            self.metadata.len()
        );
        if self.metadata.len() < number_of_signals {
            let missing_signals: Vec<_> = signals
                .iter()
                .filter(|signal| !self.metadata.contains_key(signal.as_str()))
                .take(20)
                .collect();

            Err(Error::MetadataError(format!(
                "The following signals are missing in the databroker: {:?}",
                missing_signals
            )))
        } else {
            Ok(signals_response)
        }
    }

    async fn set_initial_signals_values(
        &mut self,
        initial_signals_values: HashMap<String, DataValue>,
    ) -> Result<(), Error> {
        self.initial_signals_values = initial_signals_values;
        Ok(())
    }
}

pub fn n_to_value(metadata: &Metadata, n: u64) -> Result<proto::datapoint::Value, PublishError> {
    match metadata.data_type {
        proto::DataType::Unspecified => Err(PublishError::Shutdown),
        proto::DataType::String => match &metadata.allowed {
            Some(allowed) => {
                let index = n % allowed.len() as u64;
                let value = allowed[index as usize].clone();
                Ok(proto::datapoint::Value::String(value))
            }
            None => Ok(proto::datapoint::Value::String(n.to_string())),
        },
        proto::DataType::Boolean => match n % 2 {
            0 => Ok(proto::datapoint::Value::Bool(true)),
            _ => Ok(proto::datapoint::Value::Bool(false)),
        },
        proto::DataType::Int8 => Ok(proto::datapoint::Value::Int32((n % 128) as i32)),
        proto::DataType::Int16 => Ok(proto::datapoint::Value::Int32((n % 128) as i32)),
        proto::DataType::Int32 => Ok(proto::datapoint::Value::Int32((n % 128) as i32)),
        proto::DataType::Int64 => Ok(proto::datapoint::Value::Int64((n % 128) as i64)),
        proto::DataType::Uint8 => Ok(proto::datapoint::Value::Uint32((n % 128) as u32)),
        proto::DataType::Uint16 => Ok(proto::datapoint::Value::Uint32((n % 128) as u32)),
        proto::DataType::Uint32 => Ok(proto::datapoint::Value::Uint32((n % 128) as u32)),
        proto::DataType::Uint64 => Ok(proto::datapoint::Value::Uint64(n % 128)),
        proto::DataType::Float => Ok(proto::datapoint::Value::Float(n as f32)),
        proto::DataType::Double => Ok(proto::datapoint::Value::Double(n as f64)),
        proto::DataType::StringArray => {
            let value = match &metadata.allowed {
                Some(allowed) => {
                    let index = n % allowed.len() as u64;
                    allowed[index as usize].clone()
                }
                None => n.to_string(),
            };
            Ok(proto::datapoint::Value::StringArray(proto::StringArray {
                values: vec![value],
            }))
        }
        proto::DataType::BooleanArray => Ok(proto::datapoint::Value::BoolArray(proto::BoolArray {
            values: vec![matches!(n % 2, 0)],
        })),
        proto::DataType::Int8Array => Ok(proto::datapoint::Value::Int32Array(proto::Int32Array {
            values: vec![(n % 128) as i32],
        })),
        proto::DataType::Int16Array => Ok(proto::datapoint::Value::Int32Array(proto::Int32Array {
            values: vec![(n % 128) as i32],
        })),
        proto::DataType::Int32Array => Ok(proto::datapoint::Value::Int32Array(proto::Int32Array {
            values: vec![(n % 128) as i32],
        })),
        proto::DataType::Int64Array => Ok(proto::datapoint::Value::Int64Array(proto::Int64Array {
            values: vec![(n % 128) as i64],
        })),
        proto::DataType::Uint8Array => {
            Ok(proto::datapoint::Value::Uint32Array(proto::Uint32Array {
                values: vec![(n % 128) as u32],
            }))
        }
        proto::DataType::Uint16Array => {
            Ok(proto::datapoint::Value::Uint32Array(proto::Uint32Array {
                values: vec![(n % 128) as u32],
            }))
        }
        proto::DataType::Uint32Array => {
            Ok(proto::datapoint::Value::Uint32Array(proto::Uint32Array {
                values: vec![(n % 128) as u32],
            }))
        }
        proto::DataType::Uint64Array => {
            Ok(proto::datapoint::Value::Uint64Array(proto::Uint64Array {
                values: vec![n % 128],
            }))
        }
        proto::DataType::FloatArray => Ok(proto::datapoint::Value::FloatArray(proto::FloatArray {
            values: vec![n as f32],
        })),
        proto::DataType::DoubleArray => {
            Ok(proto::datapoint::Value::DoubleArray(proto::DoubleArray {
                values: vec![n as f64],
            }))
        }
        proto::DataType::Timestamp => Err(PublishError::DataTypeError),
        proto::DataType::TimestampArray => Err(PublishError::DataTypeError),
    }
}
