// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::borrow::Borrow;

use serde::{Serialize, Serializer, ser::Error};

use crate::{
    AvroResult, schema::{ResolvedNode},
    serde::ser_schema::SERIALIZING_SCHEMA_DEFAULT, types::Value,
    bigdecimal::big_decimal_as_bytes
};

pub struct AvroValueSerialize<'s, B : Borrow<Value>> {
    value: B,
    schema: Option<ResolvedNode<'s>>,
}

impl<'s> AvroValueSerialize<'s, Value> {
    pub fn new_with_schema(value: &Value, schema: ResolvedNode<'s>) -> AvroResult<Self>{
        let value = value.clone().resolve_internal(schema)?;
        Ok(AvroValueSerialize{value, schema: Some(schema)})
    }
}

impl<'s, 'a> AvroValueSerialize<'s, &'a Value> {
    pub fn new(value: &'a Value) -> Self{
        AvroValueSerialize{value, schema: None}
    }
}

impl<'s, B: Borrow<Value>> Serialize for AvroValueSerialize<'s, B> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.value.borrow() {
            Value::Null => serializer.serialize_unit(),
            Value::Boolean(boolean) => serializer.serialize_bool(*boolean),
            Value::Int(int) | Value::Date(int) | Value::TimeMillis(int) => {
                serializer.serialize_i32(*int)
            }
                Value::Long(long)
                | Value::TimeMicros(long)
                | Value::TimestampMillis(long)
                | Value::TimestampMicros(long)
                | Value::TimestampNanos(long)
                | Value::LocalTimestampMillis(long)
                | Value::LocalTimestampMicros(long)
                | Value::LocalTimestampNanos(long)
                => {
                serializer.serialize_i64(*long)
            }
            Value::Float(float) => {
                serializer.serialize_f32(*float)
            }
            Value::Double(double) => {
                serializer.serialize_f64(*double)
            }
                  Value::Bytes(bytes)
                | Value::Fixed(_, bytes)
                => serializer.serialize_bytes(bytes),
            Value::Uuid(uuid) => serializer.serialize_bytes(uuid.as_bytes()), // KTODO: need to provide a way
                                                                                // to encode uuid string!
            Value::BigDecimal(decimal) => {
                serializer.serialize_bytes(
                    &big_decimal_as_bytes(decimal)
                    .map_err(|err|{
                        S::Error::custom(format!("Error encoding Value::BigDecimal: {err:?}"))
                    })?)
            },
            Value::Decimal(decimal) => {
                serializer.serialize_bytes(
                    &decimal.to_vec()
                    .map_err(|err|{
                        S::Error::custom(format!("Error encoding Value::Decimal: {err:?}"))
                    })?)
            },
            Value::Duration(duration) => {
                let slice: [u8; 12] = duration.into();
                serializer.serialize_bytes(&slice)
            },
            Value::String(s) => {
                serializer.serialize_str(s)
            }
            Value::Enum(variant_index, _symbol) => { // KTODO, make more generic!
                serializer.serialize_unit_variant(
                    SERIALIZING_SCHEMA_DEFAULT,
                    *variant_index,
                    SERIALIZING_SCHEMA_DEFAULT,
                )
            }
            Value::Record(record) => {
                serializer.collect_map(record
                    .iter()
                    .map(|(key, val)|{
                        (key.as_ref(), AvroValueSerialize::new(val))
                    }))
            }
            Value::Map(map) => {
                serializer.collect_map(map
                    .iter()
                    .map(|(key, val)|{
                        (key, AvroValueSerialize::new(val))
                }))
            }
            Value::Array(array) => {
                serializer.collect_seq(array
                    .iter()
                    .map(AvroValueSerialize::new))
            }
            Value::Union(_union_index, val) => {
                    AvroValueSerialize::new(val).serialize(serializer)
            }
        }
    }
}
