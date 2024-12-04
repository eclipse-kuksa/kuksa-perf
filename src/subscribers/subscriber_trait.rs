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

use tokio::sync::broadcast::Receiver;

use log::error;
use tonic::async_trait;

use thiserror::Error;
use tokio::time::Instant;

use crate::config::Signal;

#[derive(Error, Debug)]
pub enum Error {
    #[error("subscription failed: {0}")]
    SubscriptionFailed(String),

    #[error("waiting for a signal update failed: {0}")]
    RecvFailed(String),

    #[error("signal '{0}' not found")]
    SignalNotFound(String),
}

#[async_trait]
pub trait SubscriberInterface: Send + Sync {
    async fn wait_for(&self, signal: &Signal) -> Result<Receiver<Instant>, Error>;
}
