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

use log::info;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    select,
    signal::unix::{signal, SignalKind},
    sync::broadcast::{self, Sender},
};

pub struct ShutdownHandler {
    pub trigger: Sender<()>,
    pub state: Arc<State>,
}

pub struct State {
    pub running: AtomicBool,
}

async fn shutdown_handler() {
    let mut sigint =
        signal(SignalKind::interrupt()).expect("failed to setup SIGINT signal handler");
    let mut sigterm =
        signal(SignalKind::terminate()).expect("failed to setup SIGTERM signal handler");

    select! {
        _ = sigint.recv() => info!("received SIGINT"),
        _ = sigterm.recv() => info!("received SIGTERM"),
    };
}

pub fn setup_shutdown_handler() -> ShutdownHandler {
    let state = Arc::new(State {
        running: AtomicBool::new(true),
    });

    let (shutdown_trigger, _) = broadcast::channel::<()>(1);

    let shutdown_trigger_clone = shutdown_trigger.clone();
    let state_clone = state.clone();

    tokio::spawn(async move {
        shutdown_handler().await;
        info!("shutting down");

        state_clone.running.store(false, Ordering::SeqCst);

        if shutdown_trigger_clone.send(()).is_err() {
            info!("failed to trigger shutdown");
        }
    });

    ShutdownHandler {
        trigger: shutdown_trigger,
        state,
    }
}
