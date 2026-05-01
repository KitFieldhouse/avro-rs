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

use std::{io::Read};

use serde::de::{DeserializeSeed, MapAccess};

use crate::{
    Error,
    schema::{ResolvedNode, ResolvedRecord},
    serde::deser_schema::{Config, SchemaAwareDeserializer, identifier::IdentifierDeserializer},
};

pub struct RecordDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: ResolvedRecord<'s>,
    config: Config,
    current_field: usize,
}

impl<'s, 'r, R: Read> RecordDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: ResolvedRecord<'s>, config: Config) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: 0,
        }
    }
}

impl<'de, 's, 'r, R: Read> MapAccess<'de> for RecordDeserializer<'s, 'r, R> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.current_field >= self.schema.field_len() {
            // Finished reading this record
            Ok(None)
        } else {
            seed.deserialize(IdentifierDeserializer::string(
                self.schema.get_field(self.current_field).unwrap().name(),
            ))
            .map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let schema = &self.schema.get_field(self.current_field).unwrap().schema();
        let value = if let ResolvedNode::Record(record) = schema
            && record.field_len() == 1
            && record
                .attributes()
                .get("org.apache.avro.rust.union_of_records")
                == Some(&serde_json::Value::Bool(true))
        {
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                record.get_field(0).unwrap().schema(),
                self.config,
            ))?
        } else {
            seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema.clone(),
                self.config,
            ))?
        };
        self.current_field += 1;
        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.schema.field_len() - self.current_field)
    }
}
