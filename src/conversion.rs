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
use databroker_proto::kuksa::val::v1 as proto_v1;
use databroker_proto::kuksa::val::v2 as proto_v2;

use crate::utils::DataValue;

pub fn from_v1(typed_value: proto_v1::datapoint::Value) -> DataValue {
    match &typed_value {
        proto_v1::datapoint::Value::String(value) => DataValue::String(value.to_owned()),
        proto_v1::datapoint::Value::Bool(value) => DataValue::Bool(*value),
        proto_v1::datapoint::Value::Int32(value) => DataValue::Int32(*value),
        proto_v1::datapoint::Value::Int64(value) => DataValue::Int64(*value),
        proto_v1::datapoint::Value::Uint32(value) => DataValue::Uint32(*value),
        proto_v1::datapoint::Value::Uint64(value) => DataValue::Uint64(*value),
        proto_v1::datapoint::Value::Float(value) => DataValue::Float(*value),
        proto_v1::datapoint::Value::Double(value) => DataValue::Double(*value),
        proto_v1::datapoint::Value::StringArray(array) => {
            DataValue::StringArray(array.values.clone())
        }
        proto_v1::datapoint::Value::BoolArray(array) => DataValue::BoolArray(array.values.clone()),
        proto_v1::datapoint::Value::Int32Array(array) => {
            DataValue::Int32Array(array.values.clone())
        }
        proto_v1::datapoint::Value::Int64Array(array) => {
            DataValue::Int64Array(array.values.clone())
        }
        proto_v1::datapoint::Value::Uint32Array(array) => {
            DataValue::Uint32Array(array.values.clone())
        }
        proto_v1::datapoint::Value::Uint64Array(array) => {
            DataValue::Uint64Array(array.values.clone())
        }
        proto_v1::datapoint::Value::FloatArray(array) => {
            DataValue::FloatArray(array.values.clone())
        }
        proto_v1::datapoint::Value::DoubleArray(array) => {
            DataValue::DoubleArray(array.values.clone())
        }
    }
}

pub fn from_v2(typed_value: proto_v2::value::TypedValue) -> DataValue {
    match &typed_value {
        proto_v2::value::TypedValue::String(value) => DataValue::String(value.to_owned()),
        proto_v2::value::TypedValue::Bool(value) => DataValue::Bool(*value),
        proto_v2::value::TypedValue::Int32(value) => DataValue::Int32(*value),
        proto_v2::value::TypedValue::Int64(value) => DataValue::Int64(*value),
        proto_v2::value::TypedValue::Uint32(value) => DataValue::Uint32(*value),
        proto_v2::value::TypedValue::Uint64(value) => DataValue::Uint64(*value),
        proto_v2::value::TypedValue::Float(value) => DataValue::Float(*value),
        proto_v2::value::TypedValue::Double(value) => DataValue::Double(*value),
        proto_v2::value::TypedValue::StringArray(array) => {
            DataValue::StringArray(array.values.clone())
        }
        proto_v2::value::TypedValue::BoolArray(array) => DataValue::BoolArray(array.values.clone()),
        proto_v2::value::TypedValue::Int32Array(array) => {
            DataValue::Int32Array(array.values.clone())
        }
        proto_v2::value::TypedValue::Int64Array(array) => {
            DataValue::Int64Array(array.values.clone())
        }
        proto_v2::value::TypedValue::Uint32Array(array) => {
            DataValue::Uint32Array(array.values.clone())
        }
        proto_v2::value::TypedValue::Uint64Array(array) => {
            DataValue::Uint64Array(array.values.clone())
        }
        proto_v2::value::TypedValue::FloatArray(array) => {
            DataValue::FloatArray(array.values.clone())
        }
        proto_v2::value::TypedValue::DoubleArray(array) => {
            DataValue::DoubleArray(array.values.clone())
        }
    }
}

pub fn transform_v1(from: Option<proto_v1::datapoint::Value>) -> DataValue {
    match from {
        Some(value) => match value {
            proto_v1::datapoint::Value::String(value) => DataValue::String(value),
            proto_v1::datapoint::Value::Bool(value) => DataValue::Bool(value),
            proto_v1::datapoint::Value::Int32(value) => DataValue::Int32(value),
            proto_v1::datapoint::Value::Int64(value) => DataValue::Int64(value),
            proto_v1::datapoint::Value::Uint32(value) => DataValue::Uint32(value),
            proto_v1::datapoint::Value::Uint64(value) => DataValue::Uint64(value),
            proto_v1::datapoint::Value::Float(value) => DataValue::Float(value),
            proto_v1::datapoint::Value::Double(value) => DataValue::Double(value),
            proto_v1::datapoint::Value::StringArray(array) => DataValue::StringArray(array.values),
            proto_v1::datapoint::Value::BoolArray(array) => DataValue::BoolArray(array.values),
            proto_v1::datapoint::Value::Int32Array(array) => DataValue::Int32Array(array.values),
            proto_v1::datapoint::Value::Int64Array(array) => DataValue::Int64Array(array.values),
            proto_v1::datapoint::Value::Uint32Array(array) => DataValue::Uint32Array(array.values),
            proto_v1::datapoint::Value::Uint64Array(array) => DataValue::Uint64Array(array.values),
            proto_v1::datapoint::Value::FloatArray(array) => DataValue::FloatArray(array.values),
            proto_v1::datapoint::Value::DoubleArray(array) => DataValue::DoubleArray(array.values),
        },
        None => DataValue::NotAvailable,
    }
}

pub fn transform_v2(datapoint: &proto_v2::Datapoint) -> DataValue {
    match &datapoint.value {
        Some(value) => match &value.typed_value {
            Some(proto_v2::value::TypedValue::String(value)) => DataValue::String(value.to_owned()),
            Some(proto_v2::value::TypedValue::Bool(value)) => DataValue::Bool(*value),
            Some(proto_v2::value::TypedValue::Int32(value)) => DataValue::Int32(*value),
            Some(proto_v2::value::TypedValue::Int64(value)) => DataValue::Int64(*value),
            Some(proto_v2::value::TypedValue::Uint32(value)) => DataValue::Uint32(*value),
            Some(proto_v2::value::TypedValue::Uint64(value)) => DataValue::Uint64(*value),
            Some(proto_v2::value::TypedValue::Float(value)) => DataValue::Float(*value),
            Some(proto_v2::value::TypedValue::Double(value)) => DataValue::Double(*value),
            Some(proto_v2::value::TypedValue::StringArray(array)) => {
                DataValue::StringArray(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::BoolArray(array)) => {
                DataValue::BoolArray(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::Int32Array(array)) => {
                DataValue::Int32Array(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::Int64Array(array)) => {
                DataValue::Int64Array(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::Uint32Array(array)) => {
                DataValue::Uint32Array(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::Uint64Array(array)) => {
                DataValue::Uint64Array(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::FloatArray(array)) => {
                DataValue::FloatArray(array.values.clone())
            }
            Some(proto_v2::value::TypedValue::DoubleArray(array)) => {
                DataValue::DoubleArray(array.values.clone())
            }
            None => DataValue::NotAvailable,
        },
        None => DataValue::NotAvailable,
    }
}
