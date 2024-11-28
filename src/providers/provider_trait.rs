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

use std::collections::HashMap;

use log::error;
use tonic::async_trait;

use thiserror::Error;
use tokio::time::Instant;

use crate::{config::Signal, types::DataValue};

#[async_trait]
pub trait ProviderInterface: Send + Sync {
    async fn publish(
        &self,
        signal_data: &[Signal],
        iteration: u64,
    ) -> Result<Instant, PublishError>;
    async fn validate_signals_metadata(&mut self, signals: &[Signal])
        -> Result<Vec<Signal>, Error>;
    async fn set_initial_signals_values(
        &mut self,
        initial_signals_values: HashMap<Signal, DataValue>,
    ) -> Result<(), Error>;
}

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("failed to fetch signal metadata: {0}")]
    MetadataError(String),
    #[error("publish error")]
    PublishError(#[from] PublishError),
}

#[derive(Error, Debug)]
pub enum PublishError {
    // #[error("the signal has not been registered")]
    // NotRegistered,
    #[error("failed to send new value: {0}")]
    SendFailure(String),

    #[error("Received shutdown signal")]
    Shutdown,

    #[error("DataType can not be mapped to datapoint value")]
    DataTypeError,

    #[error("Metadata error")]
    MetadataError,
}
