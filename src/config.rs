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

use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub groups: Vec<Group>,
}

#[derive(Deserialize, Clone)]
pub struct Group {
    pub group_name: String,
    pub cycle_time_microseconds: u32,
    pub signals: Vec<Signal>,
}
#[derive(Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Signal {
    pub path: String,
    pub id: Option<i32>,
}
