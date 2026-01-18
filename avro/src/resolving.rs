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

// Items used for handling and/or providing named schema resolution.

use serde::Serialize;
use serde_json::Value;

use crate::schema::{self, Aliases, ArraySchema, DecimalSchema, Documentation, EnumSchema, FixedSchema, LeafSchema, MapSchema, Name, NamesRef, RecordField, RecordFieldOrder, RecordSchema, Schema, SchemaKind, SchemaWithSymbols, UnionSchema, UuidSchema, derive};
use crate::{AvroResult, types};
use crate::error::{Details,Error};
use std::collections::BTreeMap;
use std::{collections::{HashMap, HashSet}, sync::Arc, iter::once};

/// contians a schema with all of the schema
/// definitions it needs to be completely resolved
/// This type is a promise from the API that each named
/// type in the schema has exactly one unique definition
/// and every named reference in the schema can be uniquely
/// resolved to one of these definitions.
#[derive(Debug,Clone)]
pub struct ResolvedSchema{
    pub schema: Arc<Schema>,
    context_definitions: HashMap<Arc<Name>,Arc<Schema>>
}

// convenience types
pub type NameMap = HashMap<Arc<Name>, Arc<Schema>>;
pub type NameSet = HashSet<Arc<Name>>;

impl ResolvedSchema{
    pub fn new_empty() -> Self{
       Schema::Null.try_into().unwrap()
    }

    pub fn from_str(input: impl AsRef<str>) -> AvroResult<ResolvedSchema>{
        Self::from_str_with_resolver(input, &mut DefaultResolver::new())
    }

    pub fn from_str_with_resolver(input:  impl AsRef<str>, resolver: &mut impl Resolver) -> AvroResult<ResolvedSchema>{
        let empty_additional : Vec<String> = Vec::new();
        let [resolved] = Self::from_strings_array_with_resolver([input], empty_additional, resolver)?;
        Ok(resolved)
    }

    pub fn from_strings_array<const N : usize>(to_resolve: [impl AsRef<str>; N] , additional: impl IntoIterator<Item = impl AsRef<str>>) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_strings_array_with_resolver(to_resolve, additional, &mut DefaultResolver::new())
    }

    pub fn from_strings_array_with_resolver<const N : usize>(to_resolve:  [impl AsRef<str>; N] , additional: impl IntoIterator<Item = impl AsRef<str>> , resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]>{
        let to_resolve = SchemaWithSymbols::parse_array(to_resolve)?;
        let additional = SchemaWithSymbols::parse_list(additional)?;
        let resolved = Self::from_schemata_array_with_resolver(to_resolve, additional.into_iter(), resolver)?;
        Ok(resolved)
    }

    pub fn from_strings<T : AsRef<str>>(to_resolve: Vec<T> , additional: Vec<T>) -> AvroResult<Vec<ResolvedSchema>>{
        Self::from_strings_with_resolver(to_resolve, additional, &mut DefaultResolver::new())
    }

    pub fn from_strings_with_resolver<T : AsRef<str>>(to_resolve: Vec<T> , additional: Vec<T>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>>{
        let to_resolve_len : usize = to_resolve.len();
        let schemata_with_symbols = SchemaWithSymbols::parse_list(to_resolve.into_iter().chain(additional.into_iter()))?;
        let mut resolved = Self::from_schemata_with_resolver(schemata_with_symbols, Vec::new(), resolver)?;
        Ok(resolved.drain(0..to_resolve_len).collect())
    }

    pub fn from_raw_schema_array<'a,const N: usize>(to_resolve: [&Schema;N], schemata: impl IntoIterator<Item = &'a Schema>) -> AvroResult<[ResolvedSchema; N]>{
        let to_resolve_with_symbols : [SchemaWithSymbols; N] = std::array::from_fn(|i|{
            (*to_resolve.get(i).unwrap()).clone().into()
        });
        ResolvedSchema::from_schemata_array(to_resolve_with_symbols,
            schemata.into_iter().map(|schema| schema.clone().into()))
    }

    pub fn from_schemata_array<const N : usize>(to_resolve: [SchemaWithSymbols;N], schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>) -> AvroResult<[ResolvedSchema; N]>{
        Self::from_schemata_array_with_resolver(to_resolve, schemata_with_symbols, &mut DefaultResolver::new())
    }

    pub fn from_schemata(to_resolve: impl IntoIterator<Item = SchemaWithSymbols>, schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>) -> AvroResult<Vec<ResolvedSchema>>{
        Self::from_schemata_with_resolver(to_resolve, schemata_with_symbols, &mut DefaultResolver::new())
    }

    /// Takes two vectors of schemata. Both of these vectors are checked that they form a complete
    /// schema context in which there every named schema has a unique defition and every schema
    /// reference can be uniquely resolved to one of these definitions. The first vector of
    /// schemata are those in which we want the associated ResolvedSchema forms of the schema to be
    /// returned. The second vector are schemata that are used for schema resolution, but do not
    /// have their ResolvedSchema form returned.
    pub fn from_schemata_array_with_resolver<const N : usize>(to_resolve: [SchemaWithSymbols; N], schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]> {

        let mut definined_names : NameMap = HashMap::new();

        Self::add_schemata(&mut definined_names, to_resolve.clone().into_iter().chain(schemata_with_symbols), resolver)?;

        // check defualts for each with_symbol
        for with_symbol in to_resolve.iter(){
            for (schema, value) in &with_symbol.field_defaults_to_resolve{
               let avro_value = types::Value::from(value.clone());
               avro_value.resolve_internal(ResolvedNode::new(&ResolvedSchema{
                   schema: Arc::new(schema.clone()), // TODO: this should be optimized away
                   context_definitions: definined_names.clone()
               }))?;
            }
        }

        let mut to_resolve_iter = to_resolve.into_iter();
        Ok(std::array::from_fn(|_|{
            let with_symbol = to_resolve_iter.next().unwrap();
            ResolvedSchema{
                schema: with_symbol.schema,
                context_definitions: Self::copy_needed_definitions(&definined_names, with_symbol.referenced_names)
            }
        }))
    }

    pub fn from_schemata_with_resolver(to_resolve: impl IntoIterator<Item = SchemaWithSymbols>, schemata_with_symbols: impl IntoIterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {

        let mut definined_names : NameMap = HashMap::new();
        let to_resolve : Vec<SchemaWithSymbols> = Vec::from_iter(to_resolve.into_iter());

        Self::add_schemata(&mut definined_names, to_resolve.iter().cloned().chain(schemata_with_symbols), resolver)?;

        // check defualts for each with_symbol
        for with_symbol in to_resolve.iter(){
            for (schema, value) in &with_symbol.field_defaults_to_resolve{
               let avro_value = types::Value::from(value.clone());
               avro_value.resolve_internal(ResolvedNode::new(&ResolvedSchema{
                   schema: Arc::new(schema.clone()), // TODO: this should be optimized away
                   context_definitions: definined_names.clone()
               }))?;
            }
        }

        Ok(to_resolve.into_iter().map(|schema_with_symbols|{ResolvedSchema{
                schema: schema_with_symbols.schema,
                context_definitions: Self::copy_needed_definitions(&definined_names, schema_with_symbols.referenced_names)
            }}).collect())
    }

    // convenience method for copying (pointer copy) ony the definitions we need for a given schema
    fn copy_needed_definitions(defined_names: &NameMap, needed_references: HashSet<Arc<Name>>) -> NameMap {
        let mut needed_defs : NameMap = HashMap::new();
        for needed in needed_references{
            if let Some(def) = defined_names.get(&needed){
                needed_defs.insert(needed, Arc::clone(def));
            }else{
                panic!("Unable to find a definition of needed reference after a resolution step and did not error when I should have. This is an internal error");
            }
        };
        needed_defs
    }

    // checks that the provided definition names do not conflict with existing definitions.
    fn check_if_conflicts<'a>(defined_names: &NameMap, names: impl Iterator<Item = &'a Arc<Name> >) -> AvroResult<()>{
        let mut conflicting_fullnames : Vec<String> = Vec::new();
        for name in names{
            if defined_names.contains_key(name){
                conflicting_fullnames.push(name.fullname(Option::None));
            }
        }

        if conflicting_fullnames.len() == 0 {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }

    // add the list of schemata into the context.
    fn add_schemata(defined_names: &mut NameMap, schemata: impl Iterator<Item = SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<()>{
        let mut references : HashSet<Arc<Name>> = HashSet::new();

        for schema_with_symbol in schemata{
            Self::check_if_conflicts(&defined_names, schema_with_symbol.defined_names.keys())?;
            defined_names.extend(schema_with_symbol.defined_names);
            references.extend(schema_with_symbol.referenced_names);
        }

        for schema_ref in references{
            Self::resolve_name(defined_names, &schema_ref, resolver)?;
        }

        Ok(())

    }

    // attempt to resolve the schema name, first from the known schema definitions, and if that
    // fails, from the provided resolver.
    fn resolve_name(defined_names: &mut NameMap, name: &Arc<Name>, resolver: &mut impl Resolver) -> AvroResult<()>{
       
        // first, check if can resolve internally
        if defined_names.contains_key(name) {
            return Ok(());
        }
         
        
        // second, use provided resolver
        match resolver.find_schema(name){
            Ok(schema_with_symbols) => {

                // check that what we got back from the resolver actually matches what we expect
                if !schema_with_symbols.defined_names.contains_key(name) {
                    return Err(Details::CustomSchemaResolverMismatch(name.as_ref().clone(), 
                            Vec::from_iter(schema_with_symbols.defined_names.keys().map(|key| {key.as_ref().clone()}))).into())
                }
                // matches, lets add this as a schemata that we should have, and recurse in
                Self::add_schemata(defined_names, once(schema_with_symbols), resolver)?;
                Ok(())
            },
            Err(msg) => {
                return Err(Details::SchemaResolutionErrorWithMsg(name.as_ref().clone(), msg).into());
            }
        }
    }

    /// Gets the context_definitions as a `NameRef` map. This is for backwards
    /// compatiblity and otherwise should be avoided as it copies `Names` and builds a new HashMap,
    /// which is slow
    /// TODO: This should be deprecated an replaced with proper methods in the future.
    pub fn get_names<'a>(&'a self) -> NamesRef<'a>{
        self.context_definitions.iter().map(|(key, value)| (key.as_ref().clone(), value.as_ref())).collect()
    }

    pub fn get_context_definitions(&self) -> &NameMap{
       &self.context_definitions
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedArray<'a>{
    pub attributes: &'a BTreeMap<String, Value>,
    items: &'a Schema,
    root: &'a ResolvedSchema
}

#[derive(Clone, Debug)]
pub struct ResolvedMap<'a>{
    pub attributes: &'a BTreeMap<String, Value>,
    types: &'a Schema,
    root: &'a ResolvedSchema
}

#[derive(Clone, Debug)]
pub struct ResolvedUnion<'a>{
    pub variant_index: &'a BTreeMap<SchemaKind, usize>,

    union_schema: &'a UnionSchema,
    schemas: &'a Vec<Schema>,
    root: &'a ResolvedSchema
}

#[derive(Clone,Debug)]
pub struct ResolvedRecord<'a>{
    pub name: &'a Arc<Name>,
    pub aliases: &'a Aliases,
    pub doc: &'a Documentation,
    pub lookup: &'a BTreeMap<String, usize>,
    pub attributes: &'a BTreeMap<String, Value>,
    pub fields: Vec<ResolvedRecordField<'a>>,

    root: &'a ResolvedSchema
}

#[derive(Clone,Debug)]
pub struct ResolvedRecordField<'a>{
    pub name: &'a String,
    pub doc: &'a Documentation,
    pub aliases: &'a Option<Vec<String>>,
    pub default: &'a Option<Value>,
    pub order: &'a RecordFieldOrder,
    pub position: &'a usize,
    pub custom_attributes: &'a BTreeMap<String, Value>,

    record_field: &'a RecordField,
    schema: &'a Schema,
    root: &'a ResolvedSchema
}

#[derive(Clone,Debug)]
pub enum ResolvedNode<'a>{
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    BigDecimal,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    LocalTimestampMillis,
    LocalTimestampMicros,
    LocalTimestampNanos,
    Uuid(&'a UuidSchema),
    Duration(&'a FixedSchema),
    Enum(&'a EnumSchema),
    Fixed(&'a FixedSchema),
    Decimal(&'a DecimalSchema),
    Array(ResolvedArray<'a>),
    Map(ResolvedMap<'a>),
    Union(ResolvedUnion<'a>),
    Record(ResolvedRecord<'a>),
}

/// Represents a node inside a resolved schema.
/// This is can be used when traversing down a resolved schema tree as it couples
/// the root definintion information with a reference into the schema.
impl<'a> ResolvedNode<'a> {
   pub fn new(root: &'a ResolvedSchema)->ResolvedNode<'a>{
       let schema = root.schema.as_ref();
       Self::from_schema(schema, root)
   }

   fn from_schema(schema: &'a Schema, root: &'a ResolvedSchema) -> ResolvedNode<'a>{
       match schema {
        Schema::Map(MapSchema{attributes, types}) => ResolvedNode::Map(ResolvedMap{attributes, types, root}),
        Schema::Union(union_schema) => ResolvedNode::Union(ResolvedUnion{union_schema, schemas: &union_schema.schemas, variant_index: &union_schema.variant_index, root}),
        Schema::Array(ArraySchema { items, attributes }) => ResolvedNode::Array(ResolvedArray{items, attributes, root}),
        Schema::Record(RecordSchema { name, aliases, doc, fields, lookup, attributes }) => {
            let fields = fields.iter()
                .map(|field|{
                    let RecordField {name, doc, aliases, default, schema, order, position, custom_attributes} = field;
                    ResolvedRecordField{name, doc, aliases, default, schema, order, position, custom_attributes, record_field: field, root}
                }).collect();
            ResolvedNode::Record(ResolvedRecord{ name, aliases, doc, fields, lookup, attributes, root})
        },
        Schema::Ref{name} => Self::from_schema(root.get_context_definitions().get(name).unwrap(), root),
        Schema::Null => ResolvedNode::Null,
        Schema::Boolean => ResolvedNode::Boolean,
        Schema::Int => ResolvedNode::Int,
        Schema::Long => ResolvedNode::Long,
        Schema::Float => ResolvedNode::Float,
        Schema::Double => ResolvedNode::Double,
        Schema::Bytes => ResolvedNode::Bytes,
        Schema::String => ResolvedNode::String,
        Schema::BigDecimal => ResolvedNode::BigDecimal,
        Schema::Uuid(uuid_schema) => ResolvedNode::Uuid(uuid_schema),
        Schema::Date => ResolvedNode::Date,
        Schema::TimeMillis => ResolvedNode::TimeMillis,
        Schema::TimeMicros => ResolvedNode::TimeMicros,
        Schema::TimestampMillis => ResolvedNode::TimestampMillis,
        Schema::TimestampMicros => ResolvedNode::TimestampMicros,
        Schema::TimestampNanos => ResolvedNode::TimestampNanos,
        Schema::LocalTimestampMillis => ResolvedNode::LocalTimestampMillis,
        Schema::LocalTimestampMicros => ResolvedNode::LocalTimestampMicros,
        Schema::LocalTimestampNanos => ResolvedNode::LocalTimestampNanos,
        Schema::Duration(fixed_schema) => ResolvedNode::Duration(fixed_schema),
        Schema::Enum(enum_schema) => ResolvedNode::Enum(enum_schema),
        Schema::Fixed(fixed_schema) => ResolvedNode::Fixed(fixed_schema),
        Schema::Decimal(decimal_schema) => ResolvedNode::Decimal(decimal_schema)
       }
   }
}

impl From<&ResolvedNode<'_>> for SchemaKind {
    fn from(value: &ResolvedNode) -> Self {
        match value {
            ResolvedNode::Null => Self::Null,
            ResolvedNode::Boolean => Self::Boolean,
            ResolvedNode::Int => Self::Int,
            ResolvedNode::Long => Self::Long,
            ResolvedNode::Float => Self::Float,
            ResolvedNode::Double => Self::Double,
            ResolvedNode::Bytes => Self::Bytes,
            ResolvedNode::String => Self::String,
            ResolvedNode::Array(_) => Self::Array,
            ResolvedNode::Map(_) => Self::Map,
            ResolvedNode::Union(_) => Self::Union,
            ResolvedNode::Record(_) => Self::Record,
            ResolvedNode::Enum(_) => Self::Enum,
            ResolvedNode::Fixed(_) => Self::Fixed,
            ResolvedNode::Decimal { .. } => Self::Decimal,
            ResolvedNode::BigDecimal => Self::BigDecimal,
            ResolvedNode::Uuid(_) => Self::Uuid,
            ResolvedNode::Date => Self::Date,
            ResolvedNode::TimeMillis => Self::TimeMillis,
            ResolvedNode::TimeMicros => Self::TimeMicros,
            ResolvedNode::TimestampMillis => Self::TimestampMillis,
            ResolvedNode::TimestampMicros => Self::TimestampMicros,
            ResolvedNode::TimestampNanos => Self::TimestampNanos,
            ResolvedNode::LocalTimestampMillis => Self::LocalTimestampMillis,
            ResolvedNode::LocalTimestampMicros => Self::LocalTimestampMicros,
            ResolvedNode::LocalTimestampNanos => Self::LocalTimestampNanos,
            ResolvedNode::Duration { .. } => Self::Duration,
        }
    }
}

impl<'a> ResolvedMap<'a>{
    pub fn resolve_types(&self)->ResolvedNode{
       ResolvedNode::from_schema(&self.types, self.root)
    }
}

impl<'a> ResolvedUnion<'a>{
    pub fn resolve_schemas(&self)->Vec<ResolvedNode>{
        self.schemas.iter().map(|schema|{
            ResolvedNode::from_schema(schema, self.root)
        }).collect()
    }

    /// For getting the original schema for nice error printing
    /// Other than that, use should be avoided.
    pub(crate) fn get_union_schema(&self) -> &'a UnionSchema{
        self.union_schema
    }

    pub fn structural_match_on_schema(&self, value: &types::Value) -> Option<(usize,ResolvedNode)>{
        let value_schema_kind = SchemaKind::from(value);
        let resolved_nodes = self.resolve_schemas();
        if let Some(i) = self.get_variant_index(&value_schema_kind) {
            // fast path
            Some((i, resolved_nodes.get(i).unwrap().clone()))
        } else {
            // slow path (required for matching logical or named types)
            self.resolve_schemas().into_iter().enumerate().find(|(_, resolved_node)| {
                value
                    .clone()
                    .resolve_internal(resolved_node.clone())
                    .is_ok()
            })
        }
    }

    pub fn get_variant_index(&self, schema_kind: &SchemaKind) -> Option<usize>{
       self.variant_index.get(schema_kind).copied()
    }
}

impl<'a> ResolvedArray<'a>{
    pub fn resolve_items(&self)->ResolvedNode{
        ResolvedNode::from_schema(&self.items, self.root)
    }
}

impl<'a> ResolvedRecordField<'a>{
    pub fn resolve_field(&self)->ResolvedNode{
        ResolvedNode::from_schema(&self.schema, self.root)
    }

    // delegate to implementation on Schema
    pub fn is_nullable(&self) -> bool {
        self.record_field.is_nullable()
    }
}

/// this is a schema object that is "self contained" in that it contains all named definitions
/// needed to encode/decode form this schema.
pub struct CompleteSchema(Schema);

impl CompleteSchema{
    /// Returns the [Parsing Canonical Form] of `self` that is self contained (not dependent on
    /// any definitions in `schemata`)
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas
    pub fn independent_canonical_form(&self) -> Result<String, Error> {
        Ok(self.0.canonical_form())
    }
}

impl From<&ResolvedSchema> for CompleteSchema{
    fn from(value: &ResolvedSchema) -> Self {

        fn unravel(schema: &mut Schema, defined_schemata: &NameMap, placed_schemata: &mut NameSet){
            match schema {
                Schema::Ref{name}=> {
                    if !placed_schemata.contains(name) {
                        let mut definition = defined_schemata.get(name).unwrap().as_ref().clone();
                        unravel(&mut definition, defined_schemata, placed_schemata);
                        *schema = definition;
                    }
                },
                Schema::Record(record_schema) => {
                    if !placed_schemata.insert(Arc::clone(&record_schema.name)) {
                        panic!("When converting to complete schema, attempted to double define a schema when unraveling");
                    }
                    for field in &mut record_schema.fields {
                       unravel(&mut field.schema, defined_schemata, placed_schemata);
                    }
                }
                Schema::Array(array_schema) => {
                    unravel(&mut array_schema.items, defined_schemata, placed_schemata);
                }
                Schema::Map(map_schema) => {
                    unravel(map_schema.types.as_mut(), defined_schemata, placed_schemata);
                }
                Schema::Union(union_schema) => {
                    for mut el_schema in &mut union_schema.schemas {
                        unravel(&mut el_schema, defined_schemata, placed_schemata);
                    }
                },
                Schema::Fixed(fixed_schema) => {
                    if !placed_schemata.insert(Arc::clone(&fixed_schema.name)) {
                        panic!("When converting to complete schema, attempted to double define a schema when unraveling");
                    }
                },
                Schema::Enum(enum_schema) => {
                    if !placed_schemata.insert(Arc::clone(&enum_schema.name)) {
                        panic!("When converting to complete schema, attempted to double define a schema when unraveling");
                    }
                }
                _ => {}
            }
        }

        let mut schema = value.schema.as_ref().clone();
        unravel(&mut schema, &value.context_definitions, &mut HashSet::new());
        CompleteSchema(schema)
    }
}

impl Serialize for CompleteSchema{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
                self.0.serialize(serializer)
    }
}

impl PartialEq<CompleteSchema> for CompleteSchema{
   fn eq(&self, other: &CompleteSchema) -> bool {
        self.0 == other.0
    }
}

impl Serialize for ResolvedSchema{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer {
       let complete_schema = CompleteSchema::from(self);
       complete_schema.serialize(serializer)
    }
}

impl PartialEq for ResolvedSchema{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_with_context(&self.schema, &other.schema, &self.context_definitions, &other.context_definitions, &HashSet::new(), &HashSet::new())
    }
}

impl PartialEq for SchemaWithSymbols{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_with_context(&self.schema, &other.schema, &self.defined_names, &other.defined_names, &self.referenced_names, &other.referenced_names)
    }
}

impl TryFrom<SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: SchemaWithSymbols) -> AvroResult<Self> {
        let resolved_schema = ResolvedSchema::from_schemata(vec![schema], Vec::new())?.pop().unwrap();
        Ok(resolved_schema)
    }
}

impl TryFrom<&SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &SchemaWithSymbols) -> AvroResult<Self> {
        let resolved_schema = ResolvedSchema::from_schemata(vec![schema.clone()], Vec::new())?.pop().unwrap();
        Ok(resolved_schema)
    }
}

impl TryFrom<Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        let with_symbols : SchemaWithSymbols = schema.into();
        let resolved_schema = ResolvedSchema::from_schemata(vec![with_symbols], Vec::new())?.pop().unwrap();
        Ok(resolved_schema)
    }
}

/// NOTE: this will copy the schema, and is therefore not the most performant option
impl TryFrom<&Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &Schema) -> AvroResult<Self> {
        let schema = schema.clone();
        let with_symbols : SchemaWithSymbols = schema.into();
        let resolved_schema = ResolvedSchema::from_schemata(vec![with_symbols], Vec::new())?.pop().unwrap();
        Ok(resolved_schema)
    }
}

/// trait for implementing a custom schema name resolver. For instance this 
/// could be used to create resolvers that lookup schema names 
/// from a shcema registry.
pub trait Resolver{
    fn find_schema(&mut self, name: &Arc<Name>) -> Result<SchemaWithSymbols, String>;
}

pub struct DefaultResolver{}
impl Resolver for DefaultResolver{
    fn find_schema(&mut self, _name: &Arc<Name>) -> Result<SchemaWithSymbols, String> {
       Err(String::from("Definition not found, no custom resolver was given for ResolutionContext")) 
    }
}

impl DefaultResolver{
    pub fn new()->Self{
        DefaultResolver{}
    }
}

fn compare_schema_with_context(schema_one: &Schema, schema_two: &Schema, context_one: &NameMap, context_two: &NameMap, dangling_one: &NameSet, dangling_two: &NameSet) -> bool{
    if dangling_one != dangling_two{
        return false;
    }

    // verify the provided definitions are the same:
    let one_contains_two = context_one.keys().fold(true, |acc, val|{acc && context_two.contains_key(val)});
    let two_contains_one = context_two.keys().fold(true, |acc, val|{acc && context_one.contains_key(val)});

    if !(one_contains_two && two_contains_one){
        return false
    }

    // we now know that the two contexts claim to define the same set of names, lets verify

    if context_one.iter().fold(true, |acc, (name, schema)|{acc &&
        schema.as_ref() == context_two.get(name).unwrap().as_ref()}) {
        return false
    }

    schema_one == schema_two

}

// TODO: need to fill out tests! Doh!
#[cfg(test)]
mod tests{
    use apache_avro_test_helper::TestResult;

    #[test]
    fn test_resolution() -> TestResult{
        Ok(())
    }

}
