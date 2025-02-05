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

use crate::types::DataValue;

impl From<&Option<proto::Value>> for DataValue {
    fn from(value: &Option<proto::Value>) -> Self {
        match &value {
            Some(value) => match &value.typed_value {
                Some(proto::value::TypedValue::String(value)) => {
                    DataValue::String(value.to_owned())
                }
                Some(proto::value::TypedValue::Bool(value)) => DataValue::Bool(*value),
                Some(proto::value::TypedValue::Int32(value)) => DataValue::Int32(*value),
                Some(proto::value::TypedValue::Int64(value)) => DataValue::Int64(*value),
                Some(proto::value::TypedValue::Uint32(value)) => DataValue::Uint32(*value),
                Some(proto::value::TypedValue::Uint64(value)) => DataValue::Uint64(*value),
                Some(proto::value::TypedValue::Float(value)) => DataValue::Float(*value),
                Some(proto::value::TypedValue::Double(value)) => DataValue::Double(*value),
                Some(proto::value::TypedValue::StringArray(array)) => {
                    DataValue::StringArray(array.values.clone())
                }
                Some(proto::value::TypedValue::BoolArray(array)) => {
                    DataValue::BoolArray(array.values.clone())
                }
                Some(proto::value::TypedValue::Int32Array(array)) => {
                    DataValue::Int32Array(array.values.clone())
                }
                Some(proto::value::TypedValue::Int64Array(array)) => {
                    DataValue::Int64Array(array.values.clone())
                }
                Some(proto::value::TypedValue::Uint32Array(array)) => {
                    DataValue::Uint32Array(array.values.clone())
                }
                Some(proto::value::TypedValue::Uint64Array(array)) => {
                    DataValue::Uint64Array(array.values.clone())
                }
                Some(proto::value::TypedValue::FloatArray(array)) => {
                    DataValue::FloatArray(array.values.clone())
                }
                Some(proto::value::TypedValue::DoubleArray(array)) => {
                    DataValue::DoubleArray(array.values.clone())
                }
                None => DataValue::NotAvailable,
            },
            None => DataValue::NotAvailable,
        }
    }
}
