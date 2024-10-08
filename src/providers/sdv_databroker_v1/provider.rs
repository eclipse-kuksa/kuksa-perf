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
use databroker_proto::sdv::databroker::v1 as proto;
use tokio_stream::wrappers::ReceiverStream;

use tonic::async_trait;

use log::{debug, error};

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use std::collections::HashMap;
use std::convert::TryFrom;
use tonic::transport::Channel;

pub struct Provider {
    tx: Sender<proto::StreamDatapointsRequest>,
    metadata: HashMap<String, Metadata>,
    id_to_path: HashMap<i32, String>,
    channel: Channel,
    initial_signals_values: HashMap<String, DataValue>,
}

pub struct Metadata {
    id: i32,
    data_type: proto::DataType,
    allowed_strings: Option<Vec<String>>,
}

impl Provider {
    pub fn new(channel: Channel) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(Provider::run(rx, channel.clone()));
        Ok(Provider {
            tx,
            metadata: HashMap::new(),
            id_to_path: HashMap::new(),
            channel,
            initial_signals_values: HashMap::new(),
        })
    }

    async fn run(
        rx: Receiver<proto::StreamDatapointsRequest>,
        channel: Channel,
    ) -> Result<(), Error> {
        let mut client = proto::collector_client::CollectorClient::new(channel);

        match client.stream_datapoints(ReceiverStream::new(rx)).await {
            Ok(response) => {
                let mut stream = response.into_inner();

                while let Ok(message) = stream.message().await {
                    match message {
                        Some(message) => {
                            for error in message.errors {
                                let error_code = proto::DatapointError::try_from(error.1)
                                    .unwrap()
                                    .as_str_name();
                                let id = error.0;
                                error!("{}: error setting datapoint {}", error_code, id)
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
        let datapoints = HashMap::from_iter(signal_data.iter().map(|path: &Signal| {
            let metadata = self.metadata.get(&path.path).unwrap();
            (
                metadata.id,
                proto::Datapoint {
                    timestamp: None,
                    value: Some(n_to_value(metadata, iteration).unwrap()),
                },
            )
        }));

        let payload = proto::StreamDatapointsRequest { datapoints };

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
        let mut client = proto::broker_client::BrokerClient::new(self.channel.clone())
            .max_decoding_message_size(64 * 1024 * 1024)
            .max_encoding_message_size(64 * 1024 * 1024);

        let response = client
            .get_metadata(tonic::Request::new(proto::GetMetadataRequest {
                names: signals.clone(),
            }))
            .await
            .map_err(|err| Error::MetadataError(format!("failed to fetch metadata: {}", err)))?;

        let signals_response: Vec<Signal> = response
            .into_inner()
            .list
            .into_iter()
            .map(|entry| {
                self.metadata.insert(
                    entry.name.clone(),
                    Metadata {
                        id: entry.id,
                        data_type: entry.data_type(),
                        allowed_strings: match entry.allowed {
                            Some(proto::Allowed { values }) => match values {
                                Some(proto::allowed::Values::StringValues(string_array)) => {
                                    Some(string_array.values)
                                }
                                Some(proto::allowed::Values::Int32Values(_)) => {
                                    todo!()
                                }
                                Some(proto::allowed::Values::Int64Values(_)) => {
                                    todo!()
                                }
                                Some(proto::allowed::Values::Uint32Values(_)) => {
                                    todo!()
                                }
                                Some(proto::allowed::Values::Uint64Values(_)) => {
                                    todo!()
                                }
                                Some(proto::allowed::Values::FloatValues(_)) => {
                                    todo!()
                                }
                                Some(proto::allowed::Values::DoubleValues(_)) => {
                                    todo!()
                                }
                                None => None,
                            },
                            None => None,
                        },
                    },
                );
                self.id_to_path.insert(entry.id, entry.name.clone());
                Signal { path: entry.name }
            })
            .collect();

        debug!(
            "received {} number of signals in metadata",
            self.metadata.len()
        );
        if self.metadata.len() < number_of_signals {
            let missing_signals: Vec<_> = signals
                .iter()
                .filter(|signal| !self.metadata.contains_key(signal.as_str()))
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
        proto::DataType::String => match &metadata.allowed_strings {
            Some(allowed) => {
                let index = n % allowed.len() as u64;
                let value = allowed[index as usize].clone();
                Ok(proto::datapoint::Value::StringValue(value))
            }
            None => Ok(proto::datapoint::Value::StringValue(n.to_string())),
        },
        proto::DataType::Bool => match n % 2 {
            0 => Ok(proto::datapoint::Value::BoolValue(true)),
            _ => Ok(proto::datapoint::Value::BoolValue(false)),
        },
        proto::DataType::Int8 => Ok(proto::datapoint::Value::Int32Value((n % 128) as i32)),
        proto::DataType::Int16 => Ok(proto::datapoint::Value::Int32Value((n % 128) as i32)),
        proto::DataType::Int32 => Ok(proto::datapoint::Value::Int32Value((n % 128) as i32)),
        proto::DataType::Int64 => Ok(proto::datapoint::Value::Int64Value((n % 128) as i64)),
        proto::DataType::Uint8 => Ok(proto::datapoint::Value::Uint32Value((n % 128) as u32)),
        proto::DataType::Uint16 => Ok(proto::datapoint::Value::Uint32Value((n % 128) as u32)),
        proto::DataType::Uint32 => Ok(proto::datapoint::Value::Uint32Value((n % 128) as u32)),
        proto::DataType::Uint64 => Ok(proto::datapoint::Value::Uint64Value(n % 128)),
        proto::DataType::Float => Ok(proto::datapoint::Value::FloatValue(n as f32)),
        proto::DataType::Double => Ok(proto::datapoint::Value::DoubleValue(n as f64)),
        proto::DataType::StringArray => {
            let value = match &metadata.allowed_strings {
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
        proto::DataType::BoolArray => Ok(proto::datapoint::Value::BoolArray(proto::BoolArray {
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
    }
}
