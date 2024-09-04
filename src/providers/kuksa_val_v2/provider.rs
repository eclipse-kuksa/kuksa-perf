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

use databroker_proto::kuksa::val::v2::{self as proto, open_provider_stream_request};

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
    tx: Sender<proto::OpenProviderStreamRequest>,
    data_types_map: HashMap<String, (i32, proto::DataType)>,
    channel: Channel,
}

impl Provider {
    pub fn new(channel: Channel) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(Provider::run(rx, channel.clone()));
        Ok(Provider {
            tx,
            data_types_map: HashMap::new(),
            channel,
        })
    }

    async fn run(
        rx: Receiver<proto::OpenProviderStreamRequest>,
        channel: Channel,
    ) -> Result<(), Error> {
        let mut client = proto::val_client::ValClient::new(channel);
        match client.open_provider_stream(ReceiverStream::new(rx)).await {
            Ok(_response) => {
                // No reasson to keep track of the responses
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
            let (id, data_type) = *self.data_types_map.get(&path.path).unwrap();
            (
                id,
                proto::Datapoint {
                    timestamp: None,
                    value_state: Some(proto::datapoint::ValueState::Value(
                        n_to_value(&data_type, iteration).unwrap(),
                    )),
                },
            )
        }));

        let payload = proto::OpenProviderStreamRequest {
            action: Some(open_provider_stream_request::Action::PublishValuesRequest(
                proto::PublishValuesRequest {
                    request_id: iteration as i32,
                    datapoints,
                },
            )),
        };

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
            proto::val_client::ValClient::new(self.channel.clone());

        let mut signals_response = Vec::with_capacity(signals.len());

        let requests_entries: Vec<proto::ListMetadataRequest> = signals
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
                        let data_type = metadata.data_type();
                        self.data_types_map
                            .insert(signal_path.clone(), (metadata.id, data_type));
                        signals_response.push(Signal {
                            path: signal_path.clone(),
                            id: Some(metadata.id),
                        });
                    }
                }
                Err(status) => {
                    // Handle the Err case without returning a value
                    eprintln!("gRPC call failed with status: {:?}", status);
                }
            }
        }

        debug!(
            "received {} number of signals in metadata",
            self.data_types_map.len()
        );
        if self.data_types_map.len() < number_of_signals {
            let missing_signals: Vec<_> = signals
                .iter()
                .filter(|signal| !self.data_types_map.contains_key(signal.as_str()))
                .collect();

            Err(Error::MetadataError(format!(
                "The following signals are missing in the databroker: {:?}",
                missing_signals
            )))
        } else {
            Ok(signals_response)
        }
    }
}

pub fn n_to_value(data_type: &proto::DataType, n: u64) -> Result<proto::Value, PublishError> {
    match data_type {
        proto::DataType::Unspecified => Err(PublishError::Shutdown),
        proto::DataType::String => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::String(n.to_string())),
        }),
        proto::DataType::Boolean => match n % 2 {
            0 => Ok(proto::Value {
                typed_value: Some(proto::value::TypedValue::Bool(true)),
            }),
            _ => Ok(proto::Value {
                typed_value: Some(proto::value::TypedValue::Bool(false)),
            }),
        },
        proto::DataType::Int8 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int32((n % 128) as i32)),
        }),
        proto::DataType::Int16 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int32((n % 128) as i32)),
        }),
        proto::DataType::Int32 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int32((n % 128) as i32)),
        }),
        proto::DataType::Int64 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int64((n % 128) as i64)),
        }),
        proto::DataType::Uint8 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint32((n % 128) as u32)),
        }),
        proto::DataType::Uint16 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint32((n % 128) as u32)),
        }),
        proto::DataType::Uint32 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint32((n % 128) as u32)),
        }),
        proto::DataType::Uint64 => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint64(n % 128)),
        }),
        proto::DataType::Float => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Float(n as f32)),
        }),
        proto::DataType::Double => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Double(n as f64)),
        }),
        proto::DataType::StringArray => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::StringArray(proto::StringArray {
                values: vec![n.to_string()],
            })),
        }),
        proto::DataType::BooleanArray => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::BoolArray(proto::BoolArray {
                values: vec![matches!(n % 2, 0)],
            })),
        }),
        proto::DataType::Int8Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int32Array(proto::Int32Array {
                values: vec![(n % 128) as i32],
            })),
        }),
        proto::DataType::Int16Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int32Array(proto::Int32Array {
                values: vec![(n % 128) as i32],
            })),
        }),
        proto::DataType::Int32Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int32Array(proto::Int32Array {
                values: vec![(n % 128) as i32],
            })),
        }),
        proto::DataType::Int64Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Int64Array(proto::Int64Array {
                values: vec![(n % 128) as i64],
            })),
        }),
        proto::DataType::Uint8Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint32Array(proto::Uint32Array {
                values: vec![(n % 128) as u32],
            })),
        }),
        proto::DataType::Uint16Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint32Array(proto::Uint32Array {
                values: vec![(n % 128) as u32],
            })),
        }),
        proto::DataType::Uint32Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint32Array(proto::Uint32Array {
                values: vec![(n % 128) as u32],
            })),
        }),
        proto::DataType::Uint64Array => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::Uint64Array(proto::Uint64Array {
                values: vec![n % 128],
            })),
        }),
        proto::DataType::FloatArray => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::FloatArray(proto::FloatArray {
                values: vec![n as f32],
            })),
        }),
        proto::DataType::DoubleArray => Ok(proto::Value {
            typed_value: Some(proto::value::TypedValue::DoubleArray(proto::DoubleArray {
                values: vec![n as f64],
            })),
        }),
        proto::DataType::Timestamp => Err(PublishError::DataTypeError),
        proto::DataType::TimestampArray => Err(PublishError::DataTypeError),
    }
}
