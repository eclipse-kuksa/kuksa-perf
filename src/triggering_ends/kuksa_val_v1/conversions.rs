// /********************************************************************************
// * Copyright (c) 2024 Contributors to the Eclipse Foundation
// *
// * See the NOTICE file(s) distributed with this work for additional
// * information regarding copyright ownership.
// *
// * This program and the accompanying materials are made available under the
// * terms of the Apache License 2.0 which is available at
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * SPDX-License-Identifier: Apache-2.0
// ********************************************************************************/
use databroker_proto::kuksa::val::v1 as proto;

use crate::types::DataValue;

impl From<&Option<proto::datapoint::Value>> for DataValue {
    fn from(from: &Option<proto::datapoint::Value>) -> DataValue {
        match from {
            Some(value) => match value {
                proto::datapoint::Value::String(value) => DataValue::String(value.to_string()),
                proto::datapoint::Value::Bool(value) => DataValue::Bool(*value),
                proto::datapoint::Value::Int32(value) => DataValue::Int32(*value),
                proto::datapoint::Value::Int64(value) => DataValue::Int64(*value),
                proto::datapoint::Value::Uint32(value) => DataValue::Uint32(*value),
                proto::datapoint::Value::Uint64(value) => DataValue::Uint64(*value),
                proto::datapoint::Value::Float(value) => DataValue::Float(*value),
                proto::datapoint::Value::Double(value) => DataValue::Double(*value),
                proto::datapoint::Value::StringArray(array) => {
                    DataValue::StringArray(array.values.clone())
                }
                proto::datapoint::Value::BoolArray(array) => {
                    DataValue::BoolArray(array.values.clone())
                }
                proto::datapoint::Value::Int32Array(array) => {
                    DataValue::Int32Array(array.values.clone())
                }
                proto::datapoint::Value::Int64Array(array) => {
                    DataValue::Int64Array(array.values.clone())
                }
                proto::datapoint::Value::Uint32Array(array) => {
                    DataValue::Uint32Array(array.values.clone())
                }
                proto::datapoint::Value::Uint64Array(array) => {
                    DataValue::Uint64Array(array.values.clone())
                }
                proto::datapoint::Value::FloatArray(array) => {
                    DataValue::FloatArray(array.values.clone())
                }
                proto::datapoint::Value::DoubleArray(array) => {
                    DataValue::DoubleArray(array.values.clone())
                }
            },
            None => DataValue::NotAvailable,
        }
    }
}
