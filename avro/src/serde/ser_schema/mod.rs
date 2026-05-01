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

//! Logic for serde-compatible schema-aware serialization which writes directly to a writer.

mod block;
mod record;
mod tuple;
mod union;
mod avro_value;

use std::{io::Write};

use block::BlockSerializer;
use record::RecordSerializer;
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value::Bool;
use tuple::{ManyTupleSerializer, TupleSerializer};
use union::UnionSerializer;

use crate::{
    Error,
    error::Details,
    schema::{
        DecimalSchema, InnerDecimalSchema,ResolvedMap, ResolvedNode, ResolvedRecord, SchemaKind, UuidSchema
    },
    util::{zig_i32, zig_i64},
};

#[derive(Copy, Clone)]
pub struct Config {
    /// At what block size to start a new block (for arrays and maps).
    ///
    /// This is a minimum value, the block size will always be larger than this except for the last
    /// block.
    ///
    /// When set to `None` all values will be written in a single block. This can be faster as no
    /// intermediate buffer is used, but seeking through written data will be slower.
    pub target_block_size: Option<usize>,
    /// Should `Serialize` implementations pick a human-readable format.
    ///
    /// It is recommended to set this to `false` as it results in compacter output.
    pub human_readable: bool,
}

pub struct SchemaAwareSerializer<'s, 'w, W: Write> {
    writer: &'w mut W,
    /// The schema of the data being serialized.
    ///
    /// [`ResolvedNode`] for walking the schema tree
    schema: ResolvedNode<'s>,
    config: Config,
}

impl<'s, 'w, W: Write> SchemaAwareSerializer<'s, 'w, W> {
    pub fn new(
        writer: &'w mut W,
        schema: ResolvedNode<'s>,
        config: Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            writer,
            schema,
            config,
        })
    }

    fn error(&self, ty: &'static str, error: impl Into<String>) -> Error {
        Error::new(Details::SerializeValueWithSchema {
            value_type: ty,
            value: error.into(),
            schema: self.schema
                .get_resolved()
                .unravel(),
        })
    }

    /// Create a new serializer with the existing writer and config.
    ///
    /// This will resolve the schema if it is a reference.
    fn with_different_schema(mut self, schema: ResolvedNode<'s>) -> Result<Self, Error> {
        self.schema = schema;
        Ok(self)
    }

    /// Write an integer to the writer.
    ///
    /// This will check that the current schema is [`Schema::Int`] or a logical type based on that.
    /// This also handles [`Schema::Union`].
    fn checked_write_int(self, original_ty: &'static str, v: i32) -> Result<usize, Error> {
        match self.schema {
            ResolvedNode::Int | ResolvedNode::Date | ResolvedNode::TimeMillis => zig_i32(v, self.writer),
            ResolvedNode::Union(union) => UnionSerializer::new(self.writer, union, self.config)
                .checked_write_int(original_ty, v),
            _ => Err(self.error(
                original_ty,
                "Expected Schema::Int | Schema::Date | Schema::TimeMillis",
            )),
        }
    }

    /// Write a long to the writer.
    ///
    /// This will check that the current schema is [`Schema::Long`] or a logical type based on that.
    /// This also handles [`Schema::Union`].
    fn checked_write_long(self, original_ty: &'static str, v: i64) -> Result<usize, Error> {
        match self.schema {
            ResolvedNode::Long | ResolvedNode::TimeMicros | ResolvedNode::TimestampMillis | ResolvedNode::TimestampMicros
            | ResolvedNode::TimestampNanos | ResolvedNode::LocalTimestampMillis | ResolvedNode::LocalTimestampMicros
            | ResolvedNode::LocalTimestampNanos => {
                zig_i64(v, self.writer)
            }
            ResolvedNode::Union(union) => UnionSerializer::new(self.writer, union, self.config).checked_write_long(original_ty, v),
            _ => {
                Err(self.error(original_ty, "Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}"))
            }
        }
    }

    /// Write bytes to the writer with preceding length header.
    ///
    /// This does not check the current schema.
    fn write_bytes_with_len(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let mut bytes_written = 0;
        bytes_written += zig_i64(bytes.len() as i64, &mut *self.writer)?;
        bytes_written += self.write_bytes(bytes)?;
        Ok(bytes_written)
    }

    /// Write bytes to the writer.
    ///
    /// This does not check the current schema.
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        self.writer.write_all(bytes).map_err(Details::WriteBytes)?;
        Ok(bytes.len())
    }

    /// Write an array of `n` bytes to the writer.
    ///
    /// This does not check the current schema.
    fn write_array<const N: usize>(&mut self, bytes: [u8; N]) -> Result<usize, Error> {
        self.write_bytes(&bytes)?;
        Ok(N)
    }
}

/// Indicate to the serializer that a record field default is being serialized.
///
/// This is needed because the serializer takes a `&'static str` for the enum name and variant name.
/// When this value is encountered, the serializer will blindly trust the variant index.
///
/// To prevent users from abusing this fact, the string is compared by pointer value. Because the static
/// is not public, there is no way for a user to obtain that value.
static SERIALIZING_SCHEMA_DEFAULT: &str = "This value is compared by pointer value";

impl<'s, 'w, W: Write> Serializer for SchemaAwareSerializer<'s, 'w, W> {
    /// The amount of bytes written.
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = BlockSerializer<'s, 'w, W>;
    type SerializeTuple = TupleSerializer<'s, 'w, W>;
    type SerializeTupleStruct = ManyTupleSerializer<'s, 'w, W>;
    type SerializeTupleVariant = ManyTupleSerializer<'s, 'w, W>;
    type SerializeMap = MapOrRecordSerializer<'s, 'w, W>;
    type SerializeStruct = RecordSerializer<'s, 'w, W>;
    type SerializeStructVariant = RecordSerializer<'s, 'w, W>;

    fn serialize_bool(mut self, v: bool) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Boolean => self.write_array([v as u8]),
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_bool(v)
            }
            _ => Err(self.error("bool", "Expected Schema::Boolean")),
        }
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i8", i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i16", i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("i32", v)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.checked_write_long("i64", v)
    }

    fn serialize_i128(mut self, v: i128) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "i128" => {
                self.write_array(v.to_le_bytes())
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_i128(v)
            }
            _ => Err(self.error("i128", r#"Expected Schema::Fixed(name: "i128", size: 16)"#)),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("u8", i32::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.checked_write_int("u16", i32::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.checked_write_long("u32", i64::from(v))
    }

    fn serialize_u64(mut self, v: u64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Fixed(fixed) if fixed.size == 8 && fixed.name.name() == "u64" => {
                self.write_array(v.to_le_bytes())
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_u64(v)
            }
            _ => Err(self.error("u64", r#"Expected Schema::Fixed(name: "u64", size: 8)"#)),
        }
    }

    fn serialize_u128(mut self, v: u128) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Fixed(fixed) if fixed.size == 16 && fixed.name.name() == "u128" => {
                self.write_array(v.to_le_bytes())
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_u128(v)
            }
            _ => Err(self.error("u128", r#"Expected Schema::Fixed(name: "u128", size: 16)"#)),
        }
    }

    fn serialize_f32(mut self, v: f32) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Float => self.write_array(v.to_le_bytes()),
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_f32(v)
            }
            _ => Err(self.error("f32", "Expected Schema::Float")),
        }
    }

    fn serialize_f64(mut self, v: f64) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Double => self.write_array(v.to_le_bytes()),
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_f64(v)
            }
            _ => Err(self.error("f64", "Expected Schema::Double")),
        }
    }

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            // Convert the UTF-32 character to UTF-8
            ResolvedNode::String => self.write_bytes_with_len(v.to_string().as_bytes()),
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_char(v)
            }
            _ => Err(self.error("char", "Expected Schema::String")),
        }
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::String | ResolvedNode::Uuid(UuidSchema::String) => {
                self.write_bytes_with_len(v.as_bytes())
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_str(v)
            }
            _ => Err(self.error("str", "Expected Schema::String | Schema::Uuid(String)")),
        }
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Bytes | ResolvedNode::BigDecimal | ResolvedNode::Decimal(DecimalSchema { inner: InnerDecimalSchema::Bytes, ..}) | ResolvedNode::Uuid(UuidSchema::Bytes) => {
                self.write_bytes_with_len(v)
            }
            ResolvedNode::Fixed(fixed) | ResolvedNode::Decimal(DecimalSchema { inner: InnerDecimalSchema::Fixed(fixed), .. }) | ResolvedNode::Uuid(UuidSchema::Fixed(fixed)) | ResolvedNode::Duration(fixed) => {
                if fixed.size != v.len() {
                    Err(self.error("bytes", format!("Fixed size ({}) does not match bytes length ({})", fixed.size, v.len())))
                } else {
                    self.write_bytes(v)
                }
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_bytes(v)
            }
            _ => Err(self.error("bytes", "Expected Schema::Bytes | Schema::Fixed | Schema::BigDecimal | Schema::Decimal | Schema::Uuid(Bytes | Fixed) | Schema::Duration")),
        }
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        if let ResolvedNode::Union(ref union) = self.schema
            && union.variants_len() == 2
            && let Some(null_index) = union.index_of_schema_kind(SchemaKind::Null)
        {
            zig_i32(null_index as i32, &mut *self.writer)
        } else {
            Err(self.error("none", "Expected Schema::Union([Schema::Null, _])"))
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if let ResolvedNode::Union(ref union) = self.schema
            && union.variants_len() == 2
            && let Some(null_index) = union.index_of_schema_kind(SchemaKind::Null)
        {
            let some_index = (null_index + 1) & 1;
            let mut bytes_written = zig_i32(some_index as i32, &mut *self.writer)?;
            let node = union.get_variant(some_index).unwrap();
            bytes_written +=
                value.serialize(self.with_different_schema(node)?)?;
            Ok(bytes_written)
        } else {
            Err(self.error("some", "Expected Schema::Union([Schema::Null, _])"))
        }
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Null => Ok(0),
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_unit()
            }
            _ => Err(self.error("unit", "Expected Schema::Null")),
        }
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Record(record) if record.no_fields() && record.name().name() == name => {
                Ok(0)
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_unit_struct(name)
            }
            _ => Err(self.error(
                "unit struct",
                format!(r#"Expected Schema::Record(name: "{name}", fields: [])"#),
            )),
        }
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        match self.schema {
            ResolvedNode::Enum(enum_schema) => {
                // Plain enum
                if variant.as_ptr() == SERIALIZING_SCHEMA_DEFAULT.as_ptr() || enum_schema.symbols[variant_index as usize] == variant {
                    zig_i32(variant_index as i32, &mut *self.writer)
                } else {
                    Err(self.error("unit variant", format!(r#"Expected symbol "{variant}" at index {variant_index} in enum"#)))
                }
            }
            ResolvedNode::Union(ref union) => match union.get_variant(variant_index as usize).unwrap() {
                // Bare union
                ResolvedNode::Null => zig_i32(variant_index as i32, &mut *self.writer),
                ResolvedNode::Record(ref record) if record.no_fields() && record.name().name() == variant => {
                    // Union of records
                    zig_i32(variant_index as i32, &mut *self.writer)
                }
                _ => Err(self.error("unit variant", format!("Expected Schema::Null | Schema::Record(name: {variant}, fields: []) at index {variant_index} in the union"))),
            }
            _ => Err(self.error("unit variant", format!("Expected Schema::Enum(symbols[{variant_index}] == {variant}) | Schema::Union(variants[{variant_index}] == Schema::Null | Schema::Record(name: {variant}, fields: []))"))),
        }
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self.schema {
            ResolvedNode::Record(ref record) if record.field_len() == 1 && record.name().name() == name => {
                let schema = record.get_field(0).unwrap().schema();
                value.serialize(self.with_different_schema(schema)?)
            }
            ResolvedNode::Union(union) => UnionSerializer::new(self.writer, union, self.config)
                .serialize_newtype_struct(name, value),
            _ => Err(self.error(
                "newtype struct",
                format!("Expected Schema::Record(name: {name}, fields: [_])"),
            )),
        }
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self.schema {
            ResolvedNode::Union(ref union) => match &union.get_variant(variant_index as usize).unwrap() {
                ResolvedNode::Record(record)
                    if record.field_len() == 1
                        && record.name().name() == variant
                        && record
                            .attributes()
                            .get("org.apache.avro.rust.union_of_records")
                            == Some(&Bool(true)) =>
                {
                    // Union of records
                    let mut bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
                    let schema = record.get_field(0).unwrap().schema();
                    bytes_written += value.serialize(self.with_different_schema(schema)?)?;
                    Ok(bytes_written)
                }
                schema => {
                    let mut bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
                    bytes_written += value.serialize(self.with_different_schema(schema.clone())?)?;
                    Ok(bytes_written)
                }
            },
            _ => Err(self.error("newtype variant", "Expected Schema::Union")),
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match self.schema {
            ResolvedNode::Array(array) => {
                BlockSerializer::array(self.writer, array, self.config, len, None)
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_seq(len)
            }
            _ => Err(self.error("seq", "Expected Schema::Array")),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        match self.schema {
            ResolvedNode::Union(union) => {
                // This needs to be matched first, otherwise the `schema if len == 1` will also
                // match unions
                UnionSerializer::new(self.writer, union, self.config).serialize_tuple(len)
            }
            // `len == 0` is not possible for derived Serialize implementations but users might use it.
            // The derived Serialize implementations use `serialize_unit` instead
            ResolvedNode::Null if len == 0 => Ok(TupleSerializer::unit(None)),
            schema if len == 1 => Ok(TupleSerializer::one(self.writer, schema, self.config, None)),
            ResolvedNode::Record(record) if record.field_len() == len => Ok(TupleSerializer::many(
                self.writer,
                record,
                self.config,
                None,
            )),
            // This error case can only happen for len > 1
            _ => Err(self.error(
                "tuple",
                format!("Expected Schema::Record(fields.len() == {len})"),
            )),
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        match self.schema {
            ResolvedNode::Record(record) if record.field_len() == len && record.name().name() == name => {
                Ok(ManyTupleSerializer::new(
                    self.writer,
                    record,
                    self.config,
                    None,
                ))
            }
            ResolvedNode::Union(union) => UnionSerializer::new(self.writer, union, self.config)
                .serialize_tuple_struct(name, len),
            _ => Err(self.error(
                "tuple struct",
                format!("Expected Schema::Record(name: {name}, fields.len() == {len})"),
            )),
        }
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        if let ResolvedNode::Union(ref union) = self.schema
            && let ResolvedNode::Record(record) = &union.get_variant(variant_index as usize).unwrap()
            && record.field_len() == len
            && record.name().name() == variant
        {
            let bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
            Ok(ManyTupleSerializer::new(
                self.writer,
                record.clone(),
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error("tuple variant", format!("Expected Schema::Union(variants[{variant_index}] == Schema::Record(name: {variant}, fields.len() == {len}))")))
        }
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match self.schema {
            ResolvedNode::Map(map) => Ok(MapOrRecordSerializer::map(
                self.writer,
                map,
                self.config,
                len,
                None,
            )?),
            ResolvedNode::Record(record) => {
                // Structs with flattened fields are serialized as a map
                Ok(MapOrRecordSerializer::record(
                    self.writer,
                    record,
                    self.config,
                    len,
                ))
            }
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_map(len)
            }
            _ => Err(self.error(
                "map",
                "Expected Schema::Map | Schema::Record for structs with flattened fields",
            )),
        }
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        match self.schema {
            // Serde is inconsistent with the `name` and `len` provided. When using internally tagged
            // enums the name can be the name of the inner type of a newtype variant. The length can
            // also change based on `serialize_if`.
            ResolvedNode::Record(record) => Ok(RecordSerializer::new(
                self.writer,
                record,
                self.config,
                None,
            )),
            ResolvedNode::Union(union) => {
                UnionSerializer::new(self.writer, union, self.config).serialize_struct(name, len)
            }
            _ => Err(self.error("struct", "Expected Schema::Record")),
        }
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        if let ResolvedNode::Union(ref union) = self.schema
            && let ResolvedNode::Record(record) = &union.get_variant(variant_index as usize).unwrap()
            && record.field_len() == len
            && record.name().name() == variant
        {
            let bytes_written = zig_i32(variant_index as i32, &mut *self.writer)?;
            Ok(RecordSerializer::new(
                self.writer,
                record.clone(),
                self.config,
                Some(bytes_written),
            ))
        } else {
            Err(self.error("struct variant", format!("Expected Schema::Union(variants[{variant_index}] == Schema::Record(name: {variant}, fields.len() == {len}))")))
        }
    }

    fn is_human_readable(&self) -> bool {
        self.config.human_readable
    }
}

pub enum MapOrRecordSerializer<'s, 'w, W: Write> {
    Map(BlockSerializer<'s, 'w, W>),
    Record(RecordSerializer<'s, 'w, W>),
}

impl<'s, 'w, W: Write> MapOrRecordSerializer<'s, 'w, W> {
    pub fn record(
        writer: &'w mut W,
        schema: ResolvedRecord<'s>,
        config: Config,
        bytes_written: Option<usize>,
    ) -> Self {
        Self::Record(RecordSerializer::new(writer, schema, config, bytes_written))
    }

    pub fn map(
        writer: &'w mut W,
        schema: ResolvedMap<'s>,
        config: Config,
        len: Option<usize>,
        bytes_written: Option<usize>,
    ) -> Result<Self, Error> {
        Ok(Self::Map(BlockSerializer::map(
            writer,
            schema,
            config,
            len,
            bytes_written,
        )?))
    }
}

impl<'s, 'w, W: Write> SerializeMap for MapOrRecordSerializer<'s, 'w, W> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            MapOrRecordSerializer::Map(map) => map.serialize_key(key),
            MapOrRecordSerializer::Record(record) => record.serialize_key(key),
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        match self {
            MapOrRecordSerializer::Map(map) => map.serialize_value(value),
            MapOrRecordSerializer::Record(record) => record.serialize_value(value),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self {
            MapOrRecordSerializer::Map(map) => map.end(),
            MapOrRecordSerializer::Record(record) => record.end(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        marker::PhantomData,
    };

    use apache_avro_test_helper::TestResult;
    use bigdecimal::BigDecimal;
    use num_bigint::{BigInt, Sign};
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_bytes::Bytes;
    use uuid::Uuid;

    use super::*;
    use crate::{
        Days, Duration, Millis, Months,
        decimal::Decimal,
        schema::{FixedSchema, ResolvedSchema, Schema, Name},
    };

    #[track_caller]
    fn assert_serialize_err<T: Serialize>(
        t: T,
        schema: &ResolvedSchema,
        expected: &str,
    ) {
        let config = Config {
            target_block_size: None,
            human_readable: false,
        };
        let mut buffer = Vec::new();
        let serializer = SchemaAwareSerializer::new(&mut buffer, ResolvedNode::new(schema), config).unwrap();
        let error = t
            .serialize(serializer)
            .expect_err("This should not serialize");
        assert_eq!(error.to_string(), expected);
    }

    #[track_caller]
    fn assert_serialize<T: Serialize>(
        t: T,
        schema: &ResolvedSchema,
        expected: &[u8],
    ) {
        let config = Config {
            target_block_size: None,
            human_readable: false,
        };
        let mut buffer = Vec::new();
        let serializer = SchemaAwareSerializer::new(&mut buffer, ResolvedNode::new(schema), config).unwrap();
        let bytes_written = t.serialize(serializer).expect("This should serialize");
        assert_eq!(bytes_written, buffer.len());
        assert_eq!(&buffer, expected);
    }

    #[test]
    fn test_serialize_null() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Null)?;

        assert_serialize((), &schema, &[]);
        assert_serialize_err(
            None::<()>,
            &schema,
            "Failed to serialize value of type `none` using Schema::Null: Expected Schema::Union([Schema::Null, _])",
        );
        assert_serialize_err(
            None::<i32>,
            &schema,
            "Failed to serialize value of type `none` using Schema::Null: Expected Schema::Union([Schema::Null, _])",
        );
        assert_serialize_err(
            None::<String>,
            &schema,
            "Failed to serialize value of type `none` using Schema::Null: Expected Schema::Union([Schema::Null, _])",
        );
        assert_serialize_err(
            "",
            &schema,
            "Failed to serialize value of type `str` using Schema::Null: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Null: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_bool() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Boolean)?;

        assert_serialize(true, &schema, &[1]);
        assert_serialize(false, &schema,  &[0]);
        assert_serialize_err(
            "",
            &schema,
            "Failed to serialize value of type `str` using Schema::Boolean: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Boolean: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_int() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Int)?;

        assert_serialize(4u8, &schema, &[8]);
        assert_serialize(31u16, &schema, &[62]);
        assert_serialize(7i8, &schema, &[14]);
        assert_serialize(-57i16, &schema, &[113]);
        assert_serialize(129i32, &schema, &[130, 2]);
        assert_serialize_err(
            13u32,
            &schema,
            "Failed to serialize value of type `u32` using Schema::Int: Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}",
        );
        assert_serialize_err(
            "",
            &schema,
            "Failed to serialize value of type `str` using Schema::Int: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Int: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_long() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Long)?;

        assert_serialize(13u32, &schema, &[26]);
        assert_serialize(-432i64, &schema,&[223, 6]);
        assert_serialize_err(
            4u8,
            &schema,
            "Failed to serialize value of type `u8` using Schema::Long: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            31u16,
            &schema,
            "Failed to serialize value of type `u16` using Schema::Long: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            7i8,
            &schema,
            "Failed to serialize value of type `i8` using Schema::Long: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            -57i16,
            &schema,
            "Failed to serialize value of type `i16` using Schema::Long: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            129i32,
            &schema,
            "Failed to serialize value of type `i32` using Schema::Long: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            24u64,
            &schema,
            r#"Failed to serialize value of type `u64` using Schema::Long: Expected Schema::Fixed(name: "u64", size: 8)"#,
        );
        assert_serialize_err(
            "",
            &schema,
            "Failed to serialize value of type `str` using Schema::Long: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Long: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_float() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Float)?;

        assert_serialize(4.7f32, &schema,  &[102, 102, 150, 64]);
        assert_serialize_err(
            -14.1f64,
            &schema,
            "Failed to serialize value of type `f64` using Schema::Float: Expected Schema::Double",
        );
        assert_serialize_err(
            "",
            &schema,
            "Failed to serialize value of type `str` using Schema::Float: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Float: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_double() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Double)?;

        assert_serialize(
            -14.1f64,
            &schema,
            &[51, 51, 51, 51, 51, 51, 44, 192],
        );
        assert_serialize_err(
            4.7f32,
            &schema,
            "Failed to serialize value of type `f32` using Schema::Double: Expected Schema::Float",
        );
        assert_serialize_err(
            "",
            &schema,
            "Failed to serialize value of type `str` using Schema::Double: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Double: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_bytes() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Bytes)?;

        assert_serialize(
            Bytes::new(&[12, 3, 7, 91, 4]),
            &schema,
            &[10, 12, 3, 7, 91, 4],
        );
        assert_serialize_err(
            'a',
            &schema,
            "Failed to serialize value of type `char` using Schema::Bytes: Expected Schema::String",
        );
        assert_serialize_err(
            "test",
            &schema,
            "Failed to serialize value of type `str` using Schema::Bytes: Expected Schema::String | Schema::Uuid(String)",
        );
        assert_serialize_err(
            (),
            &schema,
            "Failed to serialize value of type `unit` using Schema::Bytes: Expected Schema::Null",
        );
        assert_serialize_err(
            PhantomData::<String>,
            &schema,
            r#"Failed to serialize value of type `unit struct` using Schema::Bytes: Expected Schema::Record(name: "PhantomData", fields: [])"#,
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::Bytes: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_string() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::String)?;

        assert_serialize('a', &schema,  &[2, b'a']);
        assert_serialize("test", &schema,  &[8, b't', b'e', b's', b't']);
        assert_serialize(
            BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2),
            &schema,
            &[12, b'5', b'0', b'0', b'.', b'2', b'4'],
        );
        assert_serialize_err(
            Bytes::new(&[12, 3, 7, 91, 4]),
            &schema,
            "Failed to serialize value of type `bytes` using Schema::String: Expected Schema::Bytes | Schema::Fixed | Schema::BigDecimal | Schema::Decimal | Schema::Uuid(Bytes | Fixed) | Schema::Duration",
        );
        assert_serialize_err(
            (),
            &schema,
            "Failed to serialize value of type `unit` using Schema::String: Expected Schema::Null",
        );
        assert_serialize_err(
            PhantomData::<String>,
            &schema,
            r#"Failed to serialize value of type `unit struct` using Schema::String: Expected Schema::Record(name: "PhantomData", fields: [])"#,
        );
        assert_serialize_err(
            Some(""),
            &schema,
            "Failed to serialize value of type `some` using Schema::String: Expected Schema::Union([Schema::Null, _])",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_record() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"}
            ]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase", rename = "TestRecord")]
        struct GoodTestRecord {
            string_field: String,
            int_field: i32,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase", rename = "TestRecord")]
        struct BadTestRecord {
            foo_string_field: String,
            bar_int_field: i32,
        }

        let good_record = GoodTestRecord {
            string_field: String::from("test"),
            int_field: 10,
        };
        assert_serialize(
            good_record,
            &schema,
            &[8, b't', b'e', b's', b't', 20],
        );

        let bad_record = BadTestRecord {
            foo_string_field: String::from("test"),
            bar_int_field: 10,
        };
        assert_serialize_err(
            bad_record,
            &schema,
            r#"Missing field in record: "fooStringField""#,
        );
        assert_serialize_err(
            "",
            &schema,
            r#"Failed to serialize value of type `str` using Schema::Record(RecordSchema { name: Name { name: "TestRecord", .. }, fields: [RecordField { name: "stringField", schema: String, .. }, RecordField { name: "intField", schema: Int, .. }], .. }): Expected Schema::String | Schema::Uuid(String)"#,
        );
        assert_serialize_err(
            Some(""),
            &schema,
            r#"Failed to serialize value of type `some` using Schema::Record(RecordSchema { name: Name { name: "TestRecord", .. }, fields: [RecordField { name: "stringField", schema: String, .. }, RecordField { name: "intField", schema: Int, .. }], .. }): Expected Schema::Union([Schema::Null, _])"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_empty_record() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "record",
            "name": "EmptyRecord",
            "fields": []
        }"#,
        )?;

        #[derive(Serialize)]
        struct EmptyRecord;
        assert_serialize(EmptyRecord, &schema,  &[]);

        #[derive(Serialize)]
        #[serde(rename = "EmptyRecord")]
        struct NonEmptyRecord {
            foo: String,
        }
        let record = NonEmptyRecord {
            foo: "bar".to_string(),
        };
        assert_serialize_err(record, &schema,  r#"Missing field in record: "foo""#);
        assert_serialize_err(
            (),
            &schema,
            r#"Failed to serialize value of type `unit` using Schema::Record(RecordSchema { name: Name { name: "EmptyRecord", .. }, fields: [], .. }): Expected Schema::Null"#,
        );
        assert_serialize_err(
            "",
            &schema,
            r#"Failed to serialize value of type `str` using Schema::Record(RecordSchema { name: Name { name: "EmptyRecord", .. }, fields: [], .. }): Expected Schema::String | Schema::Uuid(String)"#,
        );
        assert_serialize_err(
            PhantomData::<String>,
            &schema,
            r#"Failed to serialize value of type `unit struct` using Schema::Record(RecordSchema { name: Name { name: "EmptyRecord", .. }, fields: [], .. }): Expected Schema::Record(name: "PhantomData", fields: [])"#,
        );
        assert_serialize_err(
            Some(""),
            &schema,
            r#"Failed to serialize value of type `some` using Schema::Record(RecordSchema { name: Name { name: "EmptyRecord", .. }, fields: [], .. }): Expected Schema::Union([Schema::Null, _])"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_enum() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "enum",
            "name": "Suit",
            "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        enum Suit {
            Spades,
            Hearts,
            Diamonds,
            Clubs,
        }

        assert_serialize(Suit::Spades, &schema,  &[0]);
        assert_serialize(Suit::Hearts, &schema,  &[2]);
        assert_serialize(Suit::Diamonds, &schema,  &[4]);
        assert_serialize(Suit::Clubs, &schema,  &[6]);
        assert_serialize_err(
            None::<()>,
            &schema,
            r#"Failed to serialize value of type `none` using Schema::Enum(EnumSchema { name: Name { name: "Suit", .. }, symbols: ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"], .. }): Expected Schema::Union([Schema::Null, _])"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_array() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "array",
            "items": "long"
        }"#,
        )?;

        assert_serialize(
            vec![10i64, 5, 400],
            &schema,
            &[6, 20, 10, 160, 6, 0],
        );
        assert_serialize_err(
            vec![1_f32],
            &schema,
            "Failed to serialize value of type `f32` using Schema::Long: Expected Schema::Float",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_map() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "map",
            "values": "long"
        }"#,
        )?;

        let mut map: BTreeMap<String, i64> = BTreeMap::new();
        map.insert(String::from("item1"), 10);
        map.insert(String::from("item2"), 400);

        assert_serialize(
            map,
            &schema,
            &[
                4, 10, b'i', b't', b'e', b'm', b'1', 20, 10, b'i', b't', b'e', b'm', b'2', 160, 6,
                0,
            ],
        );

        let mut map: BTreeMap<String, &str> = BTreeMap::new();
        map.insert(String::from("item1"), "value1");
        assert_serialize_err(
            map,
            &schema,
            "Failed to serialize value of type `str` using Schema::Long: Expected Schema::String | Schema::Uuid(String)",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_nullable_union() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"["null", "long"]"#,
        )?;

        #[derive(Serialize)]
        enum NullableLong {
            Null,
            Long(i64),
        }

        assert_serialize(Some(10i64), &schema,  &[2, 20]);
        assert_serialize(None::<i64>, &schema,  &[0]);
        assert_serialize(NullableLong::Long(400), &schema,  &[2, 160, 6]);
        assert_serialize(NullableLong::Null, &schema,  &[0]);
        assert_serialize(400i64, &schema,  &[2, 160, 6]);
        assert_serialize((), &schema,  &[0]);
        assert_serialize_err(
            "invalid",
            &schema,
            "Failed to serialize value of type `str` using Schema::Union(UnionSchema { schemas: [Null, Long] }): Expected Schema::String in variants",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_union() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"["null", "long", "string"]"#,
        )?;

        #[derive(Serialize)]
        enum LongOrString {
            Null,
            Long(i64),
            Str(String),
        }
        assert_serialize(LongOrString::Null, &schema,  &[0]);
        assert_serialize(LongOrString::Long(400), &schema,  &[2, 160, 6]);
        assert_serialize(
            LongOrString::Str("test".into()),
            &schema,
            &[4, 8, b't', b'e', b's', b't'],
        );
        assert_serialize((), &schema,  &[0]);
        assert_serialize(400i64, &schema,  &[2, 160, 6]);
        assert_serialize("test", &schema,  &[4, 8, b't', b'e', b's', b't']);
        assert_serialize_err(
            1f64,
            &schema,
            "Failed to serialize value of type `f64` using Schema::Union(UnionSchema { schemas: [Null, Long, String] }): Expected Schema::Double in variants",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_fixed() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "fixed",
            "size": 8,
            "name": "LongVal"
        }"#,
        )?;

        assert_serialize(
            Bytes::new(&[10, 124, 31, 97, 14, 201, 3, 88]),
            &schema,
            &[10, 124, 31, 97, 14, 201, 3, 88],
        );
        assert_serialize_err(
            Bytes::new(&[123]),
            &schema,
            r#"Failed to serialize value of type `bytes` using Schema::Fixed(FixedSchema { name: Name { name: "LongVal", .. }, size: 8, .. }): Fixed size (8) does not match bytes length (1)"#,
        );
        assert_serialize_err(
            [1u8; 8],
            &schema,
            r#"Failed to serialize value of type `tuple` using Schema::Fixed(FixedSchema { name: Name { name: "LongVal", .. }, size: 8, .. }): Expected Schema::Record(fields.len() == 8)"#,
        );
        assert_serialize_err(
            [1u8, 2, 3, 4, 5, 6, 7, 8].as_slice(),
            &schema,
            r#"Failed to serialize value of type `seq` using Schema::Fixed(FixedSchema { name: Name { name: "LongVal", .. }, size: 8, .. }): Expected Schema::Array"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_decimal_bytes() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#,
        )?;

        let val = Decimal::from(&[251, 155]);
        assert_serialize(val, &schema,  &[4, 251, 155]);
        assert_serialize_err(
            (),
            &schema,
            "Failed to serialize value of type `unit` using Schema::Decimal(DecimalSchema { precision: 16, scale: 2, inner: Bytes }): Expected Schema::Null",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_decimal_fixed() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "fixed",
            "name": "FixedDecimal",
            "size": 8,
            "logicalType": "decimal",
            "precision": 16,
            "scale": 2
        }"#,
        )?;

        let val = Decimal::from(&[0, 0, 0, 0, 0, 0, 251, 155]);
        assert_serialize(val, &schema,  &[0, 0, 0, 0, 0, 0, 251, 155]);
        assert_serialize_err(
            (),
            &schema,
            r#"Failed to serialize value of type `unit` using Schema::Decimal(DecimalSchema { precision: 16, scale: 2, inner: Fixed(FixedSchema { name: Name { name: "FixedDecimal", .. }, size: 8, attributes: {"precision": Number(16), "scale": Number(2)}, .. }) }): Expected Schema::Null"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_bigdecimal() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "bytes",
            "logicalType": "big-decimal"
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(transparent)]
        struct BigDecimalWrapper {
            // This is needed because the Serialize implementation of BigDecimal serializes to a string.
            // The with implementation serializes to bytes.
            #[serde(with = "crate::serde::bigdecimal")]
            value: BigDecimal,
        }

        let val = BigDecimalWrapper {
            value: BigDecimal::new(BigInt::new(Sign::Plus, vec![50024]), 2),
        };
        assert_serialize(val, &schema,  &[10, 6, 0, 195, 104, 4]);

        Ok(())
    }

    #[test]
    fn test_serialize_uuid() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "fixed",
            "size": 16,
            "logicalType": "uuid",
            "name": "FixedUuid"
        }"#,
        )?;

        // Uuid serialize implementation changes based on this value
        assert!(!crate::util::is_human_readable());

        let uuid = "8c28da81-238c-4326-bddd-4e3d00cc5099".parse::<Uuid>()?;

        assert_serialize(
            uuid,
            &schema,
            &[
                140, 40, 218, 129, 35, 140, 67, 38, 189, 221, 78, 61, 0, 204, 80, 153,
            ],
        );
        assert_serialize_err(
            1u8,
            &schema,
            r#"Failed to serialize value of type `u8` using Schema::Uuid(Fixed(FixedSchema { name: Name { name: "FixedUuid", .. }, size: 16, .. })): Expected Schema::Int | Schema::Date | Schema::TimeMillis"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_date() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "int",
            "logicalType": "date"
        }"#,
        )?;

        assert_serialize(100u8, &schema,  &[200, 1]);
        assert_serialize(1000u16, &schema,  &[208, 15]);
        assert_serialize(1000i16, &schema,  &[208, 15]);
        assert_serialize(10000i32, &schema,  &[160, 156, 1]);
        assert_serialize_err(
            10000u32,
            &schema,
            "Failed to serialize value of type `u32` using Schema::Date: Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}",
        );
        assert_serialize_err(
            10000f32,
            &schema,
            "Failed to serialize value of type `f32` using Schema::Date: Expected Schema::Float",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_time_millis() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "int",
            "logicalType": "time-millis"
        }"#,
        )?;

        assert_serialize(100u8, &schema,  &[200, 1]);
        assert_serialize(1000u16, &schema,  &[208, 15]);
        assert_serialize(1000i16, &schema,  &[208, 15]);
        assert_serialize(10000i32, &schema,  &[160, 156, 1]);
        assert_serialize_err(
            10000u32,
            &schema,
            "Failed to serialize value of type `u32` using Schema::TimeMillis: Expected Schema::Long | Schema::TimeMicros | Schema::{,Local}Timestamp{Millis,Micros,Nanos}",
        );
        assert_serialize_err(
            10000f32,
            &schema,
            "Failed to serialize value of type `f32` using Schema::TimeMillis: Expected Schema::Float",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_time_micros() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "long",
            "logicalType": "time-micros"
        }"#,
        )?;

        assert_serialize(10000u32, &schema,  &[160, 156, 1]);
        assert_serialize(10000i64, &schema,  &[160, 156, 1]);
        assert_serialize_err(
            100u8,
            &schema,
            "Failed to serialize value of type `u8` using Schema::TimeMicros: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            1000u16,
            &schema,
            "Failed to serialize value of type `u16` using Schema::TimeMicros: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            1000i16,
            &schema,
            "Failed to serialize value of type `i16` using Schema::TimeMicros: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            10000i32,
            &schema,
            "Failed to serialize value of type `i32` using Schema::TimeMicros: Expected Schema::Int | Schema::Date | Schema::TimeMillis",
        );
        assert_serialize_err(
            10000f32,
            &schema,
            "Failed to serialize value of type `f32` using Schema::TimeMicros: Expected Schema::Float",
        );

        Ok(())
    }

    #[test]
    fn test_serialize_timestamp() -> TestResult {
        for (precision, error) in [
            ("millis", "Millis"),
            ("micros", "Micros"),
            ("nanos", "Nanos"),
        ] {
            let schema = ResolvedSchema::parse_str(&format!(
                r#"{{
                "type": "long",
                "logicalType": "timestamp-{precision}"
            }}"#
            ))?;

            assert_serialize(10000u32, &schema,  &[160, 156, 1]);
            assert_serialize(10000i64, &schema,  &[160, 156, 1]);
            assert_serialize_err(
                100u8,
                &schema,
                &format!(
                    "Failed to serialize value of type `u8` using Schema::Timestamp{error}: Expected Schema::Int | Schema::Date | Schema::TimeMillis"
                ),
            );
            assert_serialize_err(
                1000u16,
                &schema,
                &format!(
                    "Failed to serialize value of type `u16` using Schema::Timestamp{error}: Expected Schema::Int | Schema::Date | Schema::TimeMillis"
                ),
            );
            assert_serialize_err(
                1000i16,
                &schema,
                &format!(
                    "Failed to serialize value of type `i16` using Schema::Timestamp{error}: Expected Schema::Int | Schema::Date | Schema::TimeMillis"
                ),
            );
            assert_serialize_err(
                10000i32,
                &schema,
                &format!(
                    "Failed to serialize value of type `i32` using Schema::Timestamp{error}: Expected Schema::Int | Schema::Date | Schema::TimeMillis"
                ),
            );
            assert_serialize_err(
                10000f32,
                &schema,
                &format!(
                    "Failed to serialize value of type `f32` using Schema::Timestamp{error}: Expected Schema::Float"
                ),
            );
        }

        Ok(())
    }

    #[test]
    fn test_serialize_duration() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "fixed",
            "size": 12,
            "name": "duration",
            "logicalType": "duration"
        }"#,
        )?;

        let duration = Duration::new(Months::new(3), Days::new(2), Millis::new(1200));
        assert_serialize(
            duration,
            &schema,
            &[3, 0, 0, 0, 2, 0, 0, 0, 176, 4, 0, 0],
        );
        assert_serialize_err(
            [0u8; 12],
            &schema,
            r#"Failed to serialize value of type `tuple` using Schema::Duration(FixedSchema { name: Name { name: "duration", .. }, size: 12, .. }): Expected Schema::Record(fields.len() == 12)"#,
        );

        Ok(())
    }

    #[test]
    fn test_serialize_recursive_record() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "stringField", "type": "string"},
                {"name": "intField", "type": "int"},
                {"name": "uuidField", "type": {"name": "uuid", "type": "fixed", "size": 16, "logicalType": "uuid"}},
                {"name": "innerRecord", "type": ["null", "TestRecord"]}
            ]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            string_field: String,
            int_field: i32,
            uuid_field: Uuid,
            // #[serde(skip_serializing_if = "Option::is_none")] => Never ignore None!
            inner_record: Option<Box<TestRecord>>,
        }

        assert!(!crate::util::is_human_readable());

        let good_record = TestRecord {
            string_field: String::from("test"),
            int_field: 10,
            uuid_field: "8c28da81-238c-4326-bddd-4e3d00cc5098".parse::<Uuid>()?,
            inner_record: Some(Box::new(TestRecord {
                string_field: String::from("inner_test"),
                int_field: 100,
                uuid_field: "8c28da81-238c-4326-bddd-4e3d00cc5099".parse::<Uuid>()?,
                inner_record: None,
            })),
        };
        assert_serialize(
            good_record,
            &schema,
            &[
                8, 116, 101, 115, 116, 20, 140, 40, 218, 129, 35, 140, 67, 38, 189, 221, 78, 61, 0,
                204, 80, 152, 2, 20, 105, 110, 110, 101, 114, 95, 116, 101, 115, 116, 200, 1, 140,
                40, 218, 129, 35, 140, 67, 38, 189, 221, 78, 61, 0, 204, 80, 153, 0,
            ],
        );

        Ok(())
    }

    #[test]
    fn avro_rs_337_serialize_union_record_variant() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [{
                "name": "innerUnion", "type": [
                    {"type": "record", "name": "innerRecordFoo", "fields": [
                        {"name": "foo", "type": "string"}
                    ]},
                    {"type": "record", "name": "innerRecordBar", "fields": [
                        {"name": "bar", "type": "string"}
                    ]},
                    {"name": "intField", "type": "int"},
                    {"name": "stringField", "type": "string"}
                ]
            }]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            inner_union: InnerUnion,
        }

        #[derive(Serialize)]
        #[serde(untagged)]
        enum InnerUnion {
            InnerVariantFoo(InnerRecordFoo),
            InnerVariantBar(InnerRecordBar),
            IntField(i32),
            StringField(String),
        }

        #[derive(Serialize)]
        #[serde(rename = "innerRecordFoo")]
        struct InnerRecordFoo {
            foo: String,
        }

        #[derive(Serialize)]
        #[serde(rename = "innerRecordBar")]
        struct InnerRecordBar {
            bar: String,
        }

        let foo_record = TestRecord {
            inner_union: InnerUnion::InnerVariantFoo(InnerRecordFoo {
                foo: String::from("foo"),
            }),
        };
        assert_serialize(
            foo_record,
            &schema,
            &[0, 6, b'f', b'o', b'o'],
        );
        let bar_record = TestRecord {
            inner_union: InnerUnion::InnerVariantBar(InnerRecordBar {
                bar: String::from("bar"),
            }),
        };
        assert_serialize(
            bar_record,
            &schema,
            &[2, 6, b'b', b'a', b'r'],
        );
        let int_record = TestRecord {
            inner_union: InnerUnion::IntField(1),
        };
        assert_serialize(int_record, &schema, &[4, 2]);
        let string_record = TestRecord {
            inner_union: InnerUnion::StringField(String::from("string")),
        };
        assert_serialize(
            string_record,
            &schema,
            &[6, 12, b's', b't', b'r', b'i', b'n', b'g'],
        );
        Ok(())
    }

    #[test]
    fn avro_rs_337_serialize_option_union_record_variant() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"{
            "type": "record",
            "name": "TestRecord",
            "fields": [{
                "name": "innerUnion", "type": [
                    "null",
                    {"type": "record", "name": "innerRecordFoo", "fields": [
                        {"name": "foo", "type": "string"}
                    ]},
                    {"type": "record", "name": "innerRecordBar", "fields": [
                        {"name": "bar", "type": "string"}
                    ]},
                    {"name": "intField", "type": "int"},
                    {"name": "stringField", "type": "string"}
                ]
            }]
        }"#,
        )?;

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct TestRecord {
            inner_union: Option<InnerUnion>,
        }

        #[derive(Serialize)]
        #[serde(untagged)]
        enum InnerUnion {
            IntField(i32),
        }

        // Flattening a Option into the underlying union is NOT supported
        let null_record = TestRecord { inner_union: None };
        assert_serialize_err(
            null_record,
            &schema,
            r#"Failed to serialize field 'innerUnion' of record RecordSchema { name: Name { name: "TestRecord", .. }, fields: [RecordField { name: "innerUnion", schema: Union(UnionSchema { schemas: [Null, Record(RecordSchema { name: Name { name: "innerRecordFoo", .. }, fields: [RecordField { name: "foo", schema: String, .. }], .. }), Record(RecordSchema { name: Name { name: "innerRecordBar", .. }, fields: [RecordField { name: "bar", schema: String, .. }], .. }), Int, String] }), .. }], .. }: Failed to serialize value of type `none`: Expected Schema::Union([Schema::Null, _])"#,
        );
        let foo_record = TestRecord {
            inner_union: Some(InnerUnion::IntField(42)),
        };
        assert_serialize_err(
            foo_record,
            &schema,
            r#"Failed to serialize field 'innerUnion' of record RecordSchema { name: Name { name: "TestRecord", .. }, fields: [RecordField { name: "innerUnion", schema: Union(UnionSchema { schemas: [Null, Record(RecordSchema { name: Name { name: "innerRecordFoo", .. }, fields: [RecordField { name: "foo", schema: String, .. }], .. }), Record(RecordSchema { name: Name { name: "innerRecordBar", .. }, fields: [RecordField { name: "bar", schema: String, .. }], .. }), Int, String] }), .. }], .. }: Failed to serialize value of type `some`: Expected Schema::Union([Schema::Null, _])"#,
        );
        Ok(())
    }

    #[test]
    fn avro_rs_351_different_field_order_serde_vs_schema() -> TestResult {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Foo {
            a: String,
            b: String,
            c: i64,
            d: f64,
            e: i64,
        }
        let schema = ResolvedSchema::parse_str(
            r#"
        {
            "type":"record",
            "name":"Foo",
            "fields": [
                {
                    "name":"b",
                    "type":"string"
                },
                {
                    "name":"a",
                    "type":"string"
                },
                {
                    "name":"d",
                    "type":"double"
                },
                {
                    "name":"e",
                    "type":"long"
                },
                {
                    "name":"c",
                    "type":"long"
                }
            ]
        }
        "#,
        )?;

        let foo = Foo {
            a: "Hello".into(),
            b: "World".into(),
            c: 42,
            d: std::f64::consts::PI,
            e: 5,
        };

        assert_serialize(
            foo,
            &schema,
            &[
                10, b'W', b'o', b'r', b'l', b'd', 10, b'H', b'e', b'l', b'l', b'o', 24, 45, 68, 84,
                251, 33, 9, 64, 10, 84,
            ],
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_string() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::String)?;

        assert_serialize('a', &schema,  &[2, b'a']);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_bytes() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Bytes)?;

        assert_serialize_err(
            'a',
            &schema,
            "Failed to serialize value of type `char` using Schema::Bytes: Expected Schema::String",
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_char_as_fixed() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("char")?.into(),
            aliases: None,
            doc: None,
            size: 4,
            attributes: Default::default(),
        }))?;

        assert_serialize_err(
            'a',
            &schema,
            r#"Failed to serialize value of type `char` using Schema::Fixed(FixedSchema { name: Name { name: "char", .. }, size: 4, .. }): Expected Schema::String"#,
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_emoji_char_as_string() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::String)?;

        assert_serialize('👹', &schema,  &[8, 240, 159, 145, 185]);

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_i128_as_fixed() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("i128")?.into(),
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        }))?;

        assert_serialize(
            i128::MAX,
            &schema,
            &[
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0x7F,
            ],
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_i128_as_fixed_wrong_name() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("onehundredtwentyeight")?.into(),
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        }))?;

        assert_serialize_err(
            i128::MAX,
            &schema,
            r#"Failed to serialize value of type `i128` using Schema::Fixed(FixedSchema { name: Name { name: "onehundredtwentyeight", .. }, size: 16, .. }): Expected Schema::Fixed(name: "i128", size: 16)"#,
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_i128_as_fixed_wrong_size() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("i128")?.into(),
            aliases: None,
            doc: None,
            size: 8,
            attributes: Default::default(),
        }))?;

        assert_serialize_err(
            i128::MAX,
            &schema,
            r#"Failed to serialize value of type `i128` using Schema::Fixed(FixedSchema { name: Name { name: "i128", .. }, size: 8, .. }): Expected Schema::Fixed(name: "i128", size: 16)"#,
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_u128_as_fixed() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("u128")?.into(),
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        }))?;

        assert_serialize(
            u128::MAX,
            &schema,
            &[
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF,
            ],
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_u128_as_fixed_wrong_name() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("onehundredtwentyeight")?.into(),
            aliases: None,
            doc: None,
            size: 16,
            attributes: Default::default(),
        }))?;

        assert_serialize_err(
            u128::MAX,
            &schema,
            r#"Failed to serialize value of type `u128` using Schema::Fixed(FixedSchema { name: Name { name: "onehundredtwentyeight", .. }, size: 16, .. }): Expected Schema::Fixed(name: "u128", size: 16)"#,
        );

        Ok(())
    }

    #[test]
    fn avro_rs_414_serialize_u128_as_fixed_wrong_size() -> TestResult {
        let schema = ResolvedSchema::builder().build_one(Schema::Fixed(FixedSchema {
            name: Name::new("u128")?.into(),
            aliases: None,
            doc: None,
            size: 8,
            attributes: Default::default(),
        }))?;

        assert_serialize_err(
            u128::MAX,
            &schema,
            r#"Failed to serialize value of type `u128` using Schema::Fixed(FixedSchema { name: Name { name: "u128", .. }, size: 8, .. }): Expected Schema::Fixed(name: "u128", size: 16)"#,
        );

        Ok(())
    }

    #[test]
    fn avro_rs_421_serialize_bytes_union_of_fixed() -> TestResult {
        let schema = ResolvedSchema::parse_str(
            r#"[
            { "name": "fixed4", "type": "fixed", "size": 4 },
            { "name": "fixed8", "type": "fixed", "size": 8 }
        ]"#,
        )?;
        assert_serialize(Bytes::new(&[0, 1, 2, 3]), &schema,  &[0, 0, 1, 2, 3]);
        assert_serialize(
            Bytes::new(&[4, 5, 6, 7, 8, 9, 10, 11]),
            &schema,
            &[2, 4, 5, 6, 7, 8, 9, 10, 11],
        );

        Ok(())
    }
}
