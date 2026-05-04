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

use serde::de::{DeserializeSeed, SeqAccess};

use crate::{
    Error,
    schema::{ResolvedNode, ResolvedRecord},
    serde::deser_schema::{Config, SchemaAwareDeserializer},
};

pub struct OneTupleDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: ResolvedNode<'s>,
    config: Config,
    field_read: bool,
}

impl<'s, 'r, R: Read> OneTupleDeserializer<'s, 'r, R> {
    pub fn new(
        reader: &'r mut R,
        schema: ResolvedNode<'s>,
        config: Config,
    ) -> Self{
        Self {
            reader,
            schema,
            config,
            field_read: false,
        }
    }
}

impl<'de, 's, 'r, R: Read> SeqAccess<'de>
    for OneTupleDeserializer<'s, 'r, R>
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.field_read {
            Ok(None)
        } else {
            let val = seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                self.schema,
                self.config,
            ))?;
            self.field_read = true;
            Ok(Some(val))
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(1 - usize::from(self.field_read))
    }
}

pub struct ManyTupleDeserializer<'s, 'r, R: Read> {
    reader: &'r mut R,
    schema: ResolvedRecord<'s>,
    config: Config,
    current_field: usize,
}

impl<'s, 'r, R: Read> ManyTupleDeserializer<'s, 'r, R> {
    pub fn new(reader: &'r mut R, schema: ResolvedRecord<'s>, config: Config) -> Self {
        Self {
            reader,
            schema,
            config,
            current_field: 0,
        }
    }
}

impl<'de, 's, 'r, R: Read> SeqAccess<'de>
    for ManyTupleDeserializer<'s, 'r, R>
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.current_field < self.schema.field_len() {
            let schema = self.schema.get_field(self.current_field).unwrap().schema();
            let val = seed.deserialize(SchemaAwareDeserializer::new(
                self.reader,
                schema,
                self.config,
            ))?;
            self.current_field += 1;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.schema.field_len() - self.current_field)
    }
}
