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
use databroker_proto::kuksa::val::v2 as proto;

use crate::utils::DataValue;

impl From<proto::value::TypedValue> for DataValue {
    fn from(typed_value: proto::value::TypedValue) -> Self {
        match &typed_value {
            proto::value::TypedValue::String(value) => DataValue::String(value.to_owned()),
            proto::value::TypedValue::Bool(value) => DataValue::Bool(*value),
            proto::value::TypedValue::Int32(value) => DataValue::Int32(*value),
            proto::value::TypedValue::Int64(value) => DataValue::Int64(*value),
            proto::value::TypedValue::Uint32(value) => DataValue::Uint32(*value),
            proto::value::TypedValue::Uint64(value) => DataValue::Uint64(*value),
            proto::value::TypedValue::Float(value) => DataValue::Float(*value),
            proto::value::TypedValue::Double(value) => DataValue::Double(*value),
            proto::value::TypedValue::StringArray(array) => {
                DataValue::StringArray(array.values.clone())
            }
            proto::value::TypedValue::BoolArray(array) => {
                DataValue::BoolArray(array.values.clone())
            }
            proto::value::TypedValue::Int32Array(array) => {
                DataValue::Int32Array(array.values.clone())
            }
            proto::value::TypedValue::Int64Array(array) => {
                DataValue::Int64Array(array.values.clone())
            }
            proto::value::TypedValue::Uint32Array(array) => {
                DataValue::Uint32Array(array.values.clone())
            }
            proto::value::TypedValue::Uint64Array(array) => {
                DataValue::Uint64Array(array.values.clone())
            }
            proto::value::TypedValue::FloatArray(array) => {
                DataValue::FloatArray(array.values.clone())
            }
            proto::value::TypedValue::DoubleArray(array) => {
                DataValue::DoubleArray(array.values.clone())
            }
        }
    }
}
