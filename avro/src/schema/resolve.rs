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
use strum::Display;

use crate::schema::{Aliases, ArraySchema, DecimalSchema, DefaultToResolve, Documentation, EnumSchema, FixedSchema, InnerDecimalSchema, MapSchema, Name, NameMap, NameSet, RecordField, RecordSchema, Schema, SchemaKind, SchemaWithSymbols, UnionSchema, UuidSchema, unravel_inner};
use crate::schema_equality::compare_resolved;
use crate::types::Value;
use crate::{AvroResult, types};
use crate::error::{Details,Error};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::{collections::{HashMap, HashSet}, sync::Arc};

/// A map of names and definitions that is valid in that any reference to a named schema has a unambiguous definition.
#[derive(Debug,Clone)]
pub struct ResolvedContext{
    definitions: NameMap,
    default_fields: HashMap<(Arc<Name>, Arc<str>), Arc<types::Value>>
}

impl ResolvedContext{
    /// gets the resolved context from a `ResolvedSchema`
    pub fn from_resolved_schema(resolved_schema : &ResolvedSchema) -> ResolvedContext{
        resolved_schema.context.clone()
    }

    /// Returns a new empty `ResolvedContext`.
    pub fn empty() -> ResolvedContext{
        ResolvedContext{definitions: HashMap::new(), default_fields: HashMap::new()}
    }

    /// Attempts to form a context from the provided schemata. This will use the provided custom
    /// resolver if a referenced schema name is not found in the provided schemata.
    pub fn new_context_with_resolver(schemata: &Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<ResolvedContext>{
        let mut context : NameMap = HashMap::new();
        let mut defaults : Vec<Arc<Vec<DefaultToResolve>>> = Vec::new();
        Self::add_to_context(&mut context, &mut defaults, schemata, resolver)?;
        let mut context = ResolvedContext{definitions: context, default_fields: HashMap::new()};
        Self::check_defaults(&defaults, &mut context)?;
        Ok(context)
    }

    /// Attempts to form the minimal context needed to resolve all references in `schema` from definitions
    /// in `schemata`. This will use the provided custom resolver if a referenced schema name is
    /// not found in the provided schemata.
    pub fn needed_context_with_resolver(schema: &SchemaWithSymbols, schemata: &[SchemaWithSymbols], resolver: &mut impl Resolver) -> AvroResult<ResolvedContext>{
        let mut context : NameMap = HashMap::new();
        let mut defaults : Vec<Arc<Vec<DefaultToResolve>>> = Vec::new();
        Self::check_if_schemata_redefine_names(schemata.iter())?;
        Self::add_needed_to_context(schema, &mut context, &mut defaults ,schemata, resolver)?;
        let mut context = ResolvedContext{definitions: context, default_fields: HashMap::new()};
        Self::check_defaults(&defaults, &mut context)?;
        Ok(context)
    }

    // checks that the provided definition names do not conflict with existing definitions.
    fn check_if_conflicts(defined_names: &NameMap, added: &NameMap) -> AvroResult<()>{
        let mut conflicting_fullnames : Vec<String> = Vec::new();
        for (name, schema) in added{
            if defined_names.contains_key(name) && Some(schema) != defined_names.get(name){
                conflicting_fullnames.push(name.fullname(Option::None));
            }
        }

        if conflicting_fullnames.is_empty() {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }

    // add the with_symbols into the defined_names and check for naming conflicts.
    // If we can't resolved from the local context, we will ask the resolved if it can find
    // the schema we need.
    fn add_to_context(defined_names: &mut NameMap, defaults: &mut Vec<Arc<Vec<DefaultToResolve>>>, schemata: &Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<()>{
        let mut references : HashSet<Arc<Name>> = HashSet::new();

        for schema_with_symbol in schemata{
            defaults.push(Arc::clone(&schema_with_symbol.field_defaults_to_resolve));
            Self::check_if_conflicts(defined_names, &schema_with_symbol.defined_names)?;
            defined_names.extend(schema_with_symbol.defined_names.clone());
            references.extend(schema_with_symbol.referenced_names.clone());
        }

        for schema_ref in references{
            Self::resolve_name(defined_names, defaults, &schema_ref, resolver)?;
        }

        Ok(())

    }

    fn add_needed_to_context(schema: &SchemaWithSymbols, defined_names: &mut NameMap, defaults: &mut Vec<Arc<Vec<DefaultToResolve>>>, schemata: &[SchemaWithSymbols], resolver: &mut impl Resolver) -> AvroResult<()>{
        defined_names.extend(schema.defined_names.clone());
        defaults.push(Arc::clone(&schema.field_defaults_to_resolve));
        let mut worklist: HashSet<Arc<Name>> = schema.referenced_names.clone();

        while let Some(needed) = Self::take_one(&mut worklist) {
            if defined_names.contains_key(&needed) {
                continue;
            }

            if let Some(provider) = schemata.iter().find(|s| s.defined_names.contains_key(&needed)) {
                Self::check_if_conflicts(defined_names, &provider.defined_names)?;
                defined_names.extend(provider.defined_names.clone());
                defaults.push(Arc::clone(&provider.field_defaults_to_resolve));
                worklist.extend(provider.referenced_names.clone());
            } else {
                Self::resolve_name(defined_names, defaults, &needed, resolver)?;
            }
        }

        Ok(())
    }

    fn take_one(set: &mut HashSet<Arc<Name>>) -> Option<Arc<Name>> {
        let name = set.iter().next()?.clone();
        set.remove(&name);
        Some(name)
    }

    // checks if the list we have provided locally has duplicate definitions for
    // a given name.
    fn check_if_schemata_redefine_names<'s>(schemata: impl IntoIterator<Item = &'s SchemaWithSymbols>)-> AvroResult<()>{
        let mut defined_names : HashMap<&Arc<Name>, &Arc<Schema>> = HashMap::new();

        let mut conflicting_fullnames : Vec<String> = Vec::new();

        for schema in schemata{
            for (name, schema) in &schema.defined_names{
                if defined_names.contains_key(name) && Some(schema) != defined_names.get(name).map(|v| &**v){
                    conflicting_fullnames.push(name.fullname(Option::None));
                }
            }
            defined_names.extend(&schema.defined_names);
        }

        if conflicting_fullnames.is_empty() {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }

    // attempt to resolve the schema name, first from the known schema definitions, and if that
    // fails, from the provided resolver.
    fn resolve_name(defined_names: &mut NameMap, defaults: &mut Vec<Arc<Vec<DefaultToResolve>>>, name: &Arc<Name>, resolver: &mut impl Resolver) -> AvroResult<()>{
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
                Self::add_to_context(defined_names, defaults, &vec![schema_with_symbols], resolver)?;
                Ok(())
            },
            Err(msg) => {
                Err(Details::SchemaResolutionErrorWithMsg(name.as_ref().clone(), msg).into())
            }
        }
    }

    fn check_defaults(default_vecs: &Vec<Arc<Vec<DefaultToResolve>>>, context: &mut ResolvedContext) -> AvroResult<()>{
        for default_vec in default_vecs{
            for default in default_vec.as_ref(){
                let mut avro_value = types::Value::try_from(default.json.clone())?;
                avro_value = avro_value.resolve_internal(ResolvedNode::new(&ResolvedSchema{
                    stub: Arc::clone(&default.schema),
                    context: context.clone()
                })).map_err(|error| {
                    Details::DefaultValidationWithReason {
                        record_name: default.defualt_id.0
                            .as_ref()
                            .fullname(None)
                            .to_string(),
                        field_name: default.defualt_id.1.to_string(),
                        value: default.json.clone() ,
                        schema: default.schema
                            .as_ref()
                            .clone(),
                        reason: error.to_string() }
                })?;
                context.default_fields.insert(default.defualt_id.clone(), avro_value.into());
            }
        }

        Ok(())
    }
}

/// Trait for types that can be converted into a [`SchemaWithSymbols`] for schema resolution.
///
/// Implemented for:
/// - `&str`, `String`, `&String` (parses JSON schema string)
/// - `&Schema` (clones and converts)
/// - `SchemaWithSymbols` (identity)
pub trait IntoSchemaWithSymbols {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols>;
}

impl<S: AsRef<str>> IntoSchemaWithSymbols for S {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        SchemaWithSymbols::parse_str(self.as_ref())
    }
}

impl IntoSchemaWithSymbols for &Schema {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        Ok(self.clone().into())
    }
}

impl IntoSchemaWithSymbols for Schema {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        Ok(self.into())
    }
}

impl IntoSchemaWithSymbols for SchemaWithSymbols {
    fn into_schema_with_symbols(self) -> AvroResult<SchemaWithSymbols> {
        Ok(self)
    }
}

/// Builder for resolving one or more schemas into [`ResolvedSchema`] values.
///
/// Created via [`ResolvedSchema::builder()`]. Accepts schema strings, `&Schema` references,
/// or [`SchemaWithSymbols`] values as input — all via the [`IntoSchemaWithSymbols`] trait.
///
/// # Examples
///
/// ```rust
/// use apache_avro::schema::{SchemaWithSymbols, ResolvedSchema};
/// // Resolve a single schema:
/// let schema = r#"
///     {
///         "name": "helloWorldRecord",
///         "type": "record",
///         "fields": [{"name": "field", "type": "string"}]
///     }
/// "#;
///
/// let resolved = ResolvedSchema::parse_str(&schema).unwrap();
///
/// // Multiple schemas with context:
///
/// let schema_name_ref = r#""mySchema""#;
/// let schema_union = r#"["mySchema", "long"]"#;
/// let schema_definition = r#"{
///     "name": "mySchema",
///     "type": "enum",
///     "symbols": ["A", "B", "C"]
/// }"#;
///
/// // Resolve with additional context schemas:
/// let [resolved_name_ref, resvoled_union] = ResolvedSchema::builder()
///     .additional(vec![schema_definition]).unwrap()
///     .build_array([schema_name_ref, schema_union]).unwrap();
///
/// // With a custom resolver (e.g. schema registry):
/// // let [resolved] = ResolvedSchema::builder()
/// //    .build_array_with_resolver([schema], &mut my_resolver).unwrap();
/// ```
pub struct ResolvedSchemaBuilder {
    additional: Vec<SchemaWithSymbols>,
}

impl ResolvedSchemaBuilder {
    fn new() -> Self {
        ResolvedSchemaBuilder {
            additional: Vec::new(),
        }
    }

    /// Provide additional schemas that participate in name resolution but are not themselves
    /// returned. Accepts anything that implements [`IntoSchemaWithSymbols`]: JSON strings,
    /// `&Schema` references, or `SchemaWithSymbols` values.
    pub fn additional(mut self, additional: impl IntoIterator<Item = impl IntoSchemaWithSymbols>) -> AvroResult<Self> {
        self.additional = additional
            .into_iter()
            .map(|s| s.into_schema_with_symbols())
            .collect::<AvroResult<Vec<_>>>()?;
        Ok(self)
    }

    /// Resolve a single schema. This is the simplest entry point.
    pub fn build_one(self, to_resolve: impl IntoSchemaWithSymbols) -> AvroResult<ResolvedSchema> {
        let [result] = self.build_array([to_resolve])?;
        Ok(result)
    }

    /// Resolve a single schema with a custom [`Resolver`] for external name lookup.
    pub fn build_one_with_resolver(self, to_resolve: impl IntoSchemaWithSymbols, resolver: &mut impl Resolver) -> AvroResult<ResolvedSchema> {
        let [result] = self.build_array_with_resolver([to_resolve], resolver)?;
        Ok(result)
    }

    /// Resolve a fixed-size array of schemas, preserving the count at compile time.
    /// All elements must be the same type (e.g. all `&str` or all `&Schema`).
    pub fn build_array<const N: usize>(self, to_resolve: [impl IntoSchemaWithSymbols; N]) -> AvroResult<[ResolvedSchema; N]> {
        self.build_array_with_resolver(to_resolve, &mut DefaultResolver::new())
    }

    /// Resolve a fixed-size array of schemas with a custom [`Resolver`].
    pub fn build_array_with_resolver<const N: usize>(self, to_resolve: [impl IntoSchemaWithSymbols; N], resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]> {
        let converted: AvroResult<Vec<SchemaWithSymbols>> = to_resolve
            .into_iter()
            .map(|s| s.into_schema_with_symbols())
            .collect();
        let converted = converted?;
        let mut iter = converted.into_iter();
        let to_resolve_array: [SchemaWithSymbols; N] = std::array::from_fn(|_| iter.next().unwrap());
        ResolvedSchema::resolve_symbols_array(to_resolve_array, self.additional, resolver)
    }

    /// Resolve a dynamic list of schemas.
    pub fn build_list(self, to_resolve: impl IntoIterator<Item = impl IntoSchemaWithSymbols>) -> AvroResult<Vec<ResolvedSchema>> {
        self.build_list_with_resolver(to_resolve, &mut DefaultResolver::default())
    }

    /// Resolve a dynamic list of schemas with a custom [`Resolver`].
    pub fn build_list_with_resolver(self, to_resolve: impl IntoIterator<Item = impl IntoSchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {
        let converted: AvroResult<Vec<SchemaWithSymbols>> = to_resolve
            .into_iter()
            .map(|s| s.into_schema_with_symbols())
            .collect();
        ResolvedSchema::resolve_symbols_list(converted?, self.additional, resolver)
    }
}

/// Contains a schema with a map of schema definitions such that each named type
/// in the schema has exactly one unique definition and every named reference
/// in the schema can be uniquely resolved to one of these definitions.
///
/// `ResolvedSchema` wraps `Arc` references and is therefore cheap to clone.
///
/// # Construction
///
/// Use [`ResolvedSchema::builder()`] to get a [`ResolvedSchemaBuilder`], or
/// [`ResolvedSchema::parse_str()`] for the single-schema convenience method.
///
/// ```rust
/// use apache_avro::schema::ResolvedSchema;
///
/// // Single schema:
/// let schema = r#"
///     {
///         "name": "helloWorldRecord",
///         "type": "record",
///         "fields": [{"name": "field", "type": "string"}]
///     }
/// "#;
///
/// let resolved = ResolvedSchema::parse_str(&schema).unwrap();
///
/// // Multiple schemas with context:
///
/// let schema_name_ref = r#""mySchema""#;
/// let schema_union = r#"["mySchema", "long"]"#;
/// let schema_definition = r#"{
///     "name": "mySchema",
///     "type": "enum",
///     "symbols": ["A", "B", "C"]
/// }"#;
///
/// let [a, b] = ResolvedSchema::builder()
///     .additional(vec![schema_definition]).unwrap()
///     .build_array([schema_name_ref, schema_union]).unwrap();
/// ```
#[derive(Debug,Clone)]
pub struct ResolvedSchema{
    pub stub: Arc<Schema>,
    context: ResolvedContext
}

impl ResolvedSchema{

    /// Start building a `ResolvedSchema`. Returns a [`ResolvedSchemaBuilder`].
    pub fn builder() -> ResolvedSchemaBuilder {
        ResolvedSchemaBuilder::new()
    }

    /// Returns the stub of this `ResolvedSchema` as a fully unwrapped standalone [`Schema`].
    pub fn unravel(&self) -> Schema{
        let complete : CompleteSchema = self.into();
        complete.0
    }

    /// creates and returns an empty [`ResolvedSchema`] with stub `Schema::Null`
    pub fn new_empty() -> Self{
       Schema::Null.try_into().unwrap()
    }

    /// Parse and resolve a single schema string. Convenience for
    /// `ResolvedSchema::builder().build_one(string)`.
    pub fn parse_str(string: impl AsRef<str>) -> AvroResult<ResolvedSchema>{
        Self::builder().build_one(string)
    }

    fn resolve_symbols_array<const N : usize>(to_resolve: [SchemaWithSymbols; N], additional: Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<[ResolvedSchema; N]> {

        let mut all_schemata : Vec<SchemaWithSymbols> = to_resolve.clone().into();
        all_schemata.extend(additional);

        let contexts : Vec<AvroResult<ResolvedContext>> = to_resolve.iter().map(|schema|{
            ResolvedContext::needed_context_with_resolver(schema , &all_schemata, resolver)
        }).collect();

        let context_errs : Vec<_> = contexts.iter().filter_map(|context|{context.as_ref().err()}).collect();

        if !context_errs.is_empty(){
            let context_errs : Vec<_> = contexts.into_iter().filter_map(|context|{context.err()}).collect();
            return Err(Details::ResolvedSchemaCreationError(context_errs).into())
        }

        let mut to_resolve_iter = to_resolve.into_iter();
        let mut context_iter = contexts.into_iter().map(AvroResult::unwrap);
        Ok(std::array::from_fn(|_|{
            let with_symbol = to_resolve_iter.next().unwrap();
            let context = context_iter.next().unwrap();
            ResolvedSchema{
                stub: with_symbol.stub,
                context
            }
        }))
    }

    fn resolve_symbols_list(to_resolve: Vec<SchemaWithSymbols>, additional: Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {

        let mut all_schemata = to_resolve.clone();
        all_schemata.extend(additional);
        let contexts : Vec<AvroResult<ResolvedContext>> = to_resolve.iter().map(|schema|{
            ResolvedContext::needed_context_with_resolver(schema , &all_schemata, resolver)
        }).collect();

        let context_errs : Vec<_> = contexts.iter().filter_map(|context|{context.as_ref().err()}).collect();

        if !context_errs.is_empty(){
            let context_errs : Vec<_> = contexts.into_iter().filter_map(|context|{context.err()}).collect();
            return Err(Details::ResolvedSchemaCreationError(context_errs).into())
        }

        let schema_and_context = to_resolve.into_iter().zip(contexts);
        Ok(schema_and_context.into_iter().map(|(schema_with_symbols, context)|{ResolvedSchema{
                stub: schema_with_symbols.stub,
                context: context.unwrap()
            }}).collect())
    }



    /// Get a reference to the [`NameMap`] for this `ResolvedSchema`.
    pub fn get_names(&self) -> &NameMap{
        &self.context.definitions
    }

    /// Get an [`Iterator`] of this `ResolvedSchema`'s defined schemata.
    pub fn get_schemata(&self) -> impl Iterator<Item = &Arc<Schema>>{
        self.context.definitions.values()
    }
}

/// An Avro array that makes a type level promise that its `"item"` schema is fully resolved.
#[derive(Clone, Debug, Copy)]
pub struct ResolvedArray<'a>{
    root: &'a ResolvedSchema,
    array_schema: &'a ArraySchema,
}

/// An Avro map that makes a type level promise that its `"types"` schema is fully resolved
#[derive(Clone, Debug, Copy)]
pub struct ResolvedMap<'a>{
    root: &'a ResolvedSchema,
    map_schema: &'a MapSchema
}

/// An Avro union that makes a type level promise that the schemata it contains are all fully resolved.
#[derive(Clone, Debug, Copy)]
pub struct ResolvedUnion<'a>{
    root: &'a ResolvedSchema,
    union_schema: &'a UnionSchema
}

/// An Avro record that makes a type level promise that each of its field's schemata are fully
/// resolved and any provided default value has been resolved against this schema.
#[derive(Clone, Debug, Copy)]
pub struct ResolvedRecord<'a>{
    root: &'a ResolvedSchema,
    record_schema: &'a RecordSchema,
}

/// An Avro record field that makes a type level promise that its schema is fully resolved and any
/// provided default value has been resolved against this schema.
#[derive(Clone, Debug, Copy)]
pub struct ResolvedRecordField<'a>{
    parent_name: &'a Arc<Name>,
    root: &'a ResolvedSchema,
    field: &'a RecordField
}

/// A node within a walk of a Avro Schema that makes the type level promise that its children
/// schema are resovled unambiguously.
#[derive(Clone,Debug, Display, Copy)]
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

impl<'a> ResolvedNode<'a> {
   pub fn new(root: &'a ResolvedSchema)->ResolvedNode<'a>{
       let schema = root.stub.as_ref();
       Self::from_schema(schema, root)
   }

   fn from_schema(schema: &'a Schema, root: &'a ResolvedSchema) -> ResolvedNode<'a>{
       match schema {
        Schema::Map(map_schema) => ResolvedNode::Map(ResolvedMap{root, map_schema}),
        Schema::Union(union_schema) => ResolvedNode::Union(ResolvedUnion{root, union_schema}),
        Schema::Array(array_schema) => ResolvedNode::Array(ResolvedArray{root, array_schema}),
        Schema::Record(record_schema) => ResolvedNode::Record(ResolvedRecord{root, record_schema}),
        Schema::Ref{name} => Self::from_schema(root.context.definitions.get(name).unwrap().as_ref(), root),
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

   /// Gets the name of this [`ResolvedNode`], if it has one
   pub fn get_name(&self)-> Option<Arc<Name>>{
        match self {
              ResolvedNode::Record(resolved_record) => Some(Arc::clone(&resolved_record.record_schema.name)),
              ResolvedNode::Enum(EnumSchema { name, .. })
            | ResolvedNode::Fixed(FixedSchema { name, .. })
            | ResolvedNode::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(FixedSchema { name, .. }),
                ..
            })
            | ResolvedNode::Uuid(UuidSchema::Fixed(FixedSchema { name, .. }))
            | ResolvedNode::Duration(FixedSchema { name, .. }) => Some(Arc::clone(name)),
            _ => None,
        }
   }

    /// Unravels this [`ResolvedNode`] into a [`Schema`]
    pub fn unravel(&self)-> Schema{
        match self{
            ResolvedNode::Null => Schema::Null,
            ResolvedNode::Boolean => Schema::Boolean,
            ResolvedNode::Int => Schema::Int,
            ResolvedNode::Long => Schema::Long,
            ResolvedNode::Float => Schema::Float,
            ResolvedNode::Double => Schema::Double,
            ResolvedNode::Bytes => Schema::Bytes,
            ResolvedNode::String => Schema::String,
            ResolvedNode::BigDecimal => Schema::BigDecimal,
            ResolvedNode::Date => Schema::Date,
            ResolvedNode::TimeMillis => Schema::TimeMillis,
            ResolvedNode::TimeMicros => Schema::TimeMicros,
            ResolvedNode::TimestampMillis => Schema::TimestampMillis,
            ResolvedNode::TimestampMicros => Schema::TimestampMicros,
            ResolvedNode::TimestampNanos => Schema::TimestampNanos,
            ResolvedNode::LocalTimestampMillis => Schema::LocalTimestampMillis,
            ResolvedNode::LocalTimestampMicros => Schema::LocalTimestampMicros,
            ResolvedNode::LocalTimestampNanos => Schema::LocalTimestampNanos,
            ResolvedNode::Uuid(uuid) => Schema::Uuid((*uuid).clone()),
            ResolvedNode::Duration(fixed_schema) => Schema::Duration((*fixed_schema).clone()),
            ResolvedNode::Enum(enum_schema) => Schema::Enum((*enum_schema).clone()),
            ResolvedNode::Fixed(fixed_schema) => Schema::Fixed((*fixed_schema).clone()),
            ResolvedNode::Decimal(decimal_schema) => Schema::Decimal((*decimal_schema).clone()),
            ResolvedNode::Array(resolved_array) => {
                let mut schema = Schema::Array(resolved_array.array_schema.clone());
                unravel_inner(&mut schema, &resolved_array.root.context.definitions, &mut HashSet::new(), &mut HashMap::new());
                schema
            },
            ResolvedNode::Map(resolved_map)=> {
                let mut schema = Schema::Map(resolved_map.map_schema.clone());
                unravel_inner(&mut schema, &resolved_map.root.context.definitions, &mut HashSet::new(), &mut HashMap::new());
                schema
            },
            ResolvedNode::Union(resolved_union) => {
                let mut schema = Schema::Union(resolved_union.union_schema.clone());
                unravel_inner(&mut schema, &resolved_union.root.context.definitions, &mut HashSet::new(), &mut HashMap::new());
                schema
            },
            ResolvedNode::Record(resolved_record) => {
                let mut schema = Schema::Record(resolved_record.record_schema.clone());
                unravel_inner(&mut schema, &resolved_record.root.context.definitions, &mut HashSet::new(), &mut HashMap::new());
                schema
            },
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

impl<'a> ResolvedRecord<'a>{
    pub fn unravel(&self) -> RecordSchema{
        let schema = ResolvedNode::Record(*self).unravel();
        if let Schema::Record(record_schema) = schema{
            record_schema
        }else{
            unreachable!();
        }
    }

    pub fn name(&self) -> &Arc<Name>{
        &self.record_schema.name
    }

    pub fn aliases(&self)-> &'a Aliases{
        &self.record_schema.aliases
    }

    pub fn doc(&self) -> &'a Documentation{
        &self.record_schema.doc
    }


    pub fn lookup(&self) -> &'a BTreeMap<Arc<str>, usize>{
        &self.record_schema.lookup
    }

    pub fn attributes(&self) -> &'a BTreeMap<String, serde_json::Value>{
        &self.record_schema.attributes
    }

    pub fn fields(&self) -> impl Iterator<Item = ResolvedRecordField<'a>>{
        self.record_schema.fields.iter().map(|field|{
            ResolvedRecordField{
                parent_name: &self.record_schema.name,
                field,
                root: self.root
            }
        })
    }

    pub fn get_field(&self, index: usize) -> Option<ResolvedRecordField<'a>>{
        self.record_schema.fields.get(index).map(|field|{
            ResolvedRecordField{
                parent_name: &self.record_schema.name,
                field,
                root: self.root
            }
        })
    }

    pub fn field_len(&self) -> usize{
        self.record_schema.fields.len()
    }

    pub fn no_fields(&self) -> bool{
        self.record_schema.fields.is_empty()
    }

}

impl<'a> ResolvedMap<'a>{
    pub fn types(&self)->ResolvedNode<'a>{
       ResolvedNode::from_schema(&self.map_schema.types, self.root)
    }

    pub fn attributes(&self)-> &'a BTreeMap<String, serde_json::Value>{
        &self.map_schema.attributes
    }
}


// KTODO: is the Iterator opaque type okay here??
impl<'a> ResolvedUnion<'a>{
    pub fn variants(&self)-> impl Iterator<Item = ResolvedNode<'a>>{
        self.union_schema.schemas.iter().map(|schema|{
            ResolvedNode::from_schema(schema, self.root)
        })
    }

    pub fn variants_len(&self) -> usize{
        self.union_schema.schemas.len()
    }

    /// Get variant at given index.
    pub fn get_variant(&self, index: usize)->AvroResult<ResolvedNode<'a>>{
        self.union_schema.schemas.get(index).map(|schema|{
            ResolvedNode::from_schema(schema, self.root)
        }).ok_or_else(|| {
            Details::GetUnionVariant {
                index: index as i64,
                num_variants: self.union_schema.schemas.len(),
            }
            .into()
        })
    }

    /// For getting the original schema for nice error printing
    /// Other than that, use should be avoided.
    pub(crate) fn get_union_schema(&self) -> &'a UnionSchema{
        self.union_schema
    }

    pub fn structural_match_on_schema(&'a self, value: &types::Value) -> Option<(usize,ResolvedNode<'a>)>{
        let value_schema_kind = SchemaKind::from(value);
        if let Some(i) = self.index_of_schema_kind(value_schema_kind) {
            // fast path
            let variant_clone = self.get_variant(i).unwrap();
            if value
                .clone()
                .resolve_internal(variant_clone)
                .is_ok(){
                Some((i, variant_clone))
            }else{
                None
            }
        } else {
            // slow path (required for matching logical or named types)
            self.variants().enumerate().find(|(_, resolved_node)| {
                value
                    .clone()
                    .resolve_internal(*resolved_node)
                    .is_ok()
            })
        }
    }

    pub fn index_of_schema_kind<K: Borrow<SchemaKind>>(&self, schema_kind: K) -> Option<usize>{
       self.union_schema.variant_index.get(schema_kind.borrow()).copied()
    }

    /// Get the index and schema for the provided Rust type name.
    ///
    /// This ignores the namespace of schemas.
    pub(crate) fn find_named_schema<'s>(
        &'s self,
        name: &str,
    ) -> Option<(usize, ResolvedNode<'a>)> {
        self.variants().enumerate().find(|(_index, node)|{
            node.get_name()
                .is_some_and(|schema_name|{schema_name.name() == name})
        })
    }

    /// Find a [`Schema::Fixed`] with the given size.
    pub(crate) fn find_fixed_of_size_n(
        &self,
        size: usize,
    ) -> Option<(usize, &'a FixedSchema)> {
        self.variants().enumerate().find_map(|(index, node)|{
            match node {
                ResolvedNode::Fixed(fixed)
                | ResolvedNode::Uuid(UuidSchema::Fixed(fixed))
                | ResolvedNode::Decimal(DecimalSchema {
                    inner: InnerDecimalSchema::Fixed(fixed),
                    ..
                })
                | ResolvedNode::Duration(fixed)
                    if fixed.size == size =>
                {
                    Some((index, fixed))
                }
                _ => {None}
            }
        })
    }

    /// Find a [`Schema::Record`] with `n` fields.
    pub(crate) fn find_record_with_n_fields(
        &self,
        n_fields: usize,
    ) -> Option<(usize, ResolvedRecord<'a>)> {
        self.variants().enumerate().find_map(|(index, node)|{
            match node {
                ResolvedNode::Record(record) if record.record_schema.fields.len() == n_fields => {
                    Some((index, record))
                }
                _ => {None}
            }
        })
    }

    /// Returns true if any of the variants of this `ResolvedUnion` is `Null`.
    pub fn is_nullable(&self) -> bool {
        self.union_schema.is_nullable()
    }
}

impl<'a> ResolvedArray<'a>{
    pub fn items(&self)->ResolvedNode<'a>{
        ResolvedNode::from_schema(&self.array_schema.items, self.root)
    }

    pub fn attributes(&self) -> &'a BTreeMap<String, serde_json::Value>{
        &self.array_schema.attributes
    }
}

impl<'a> ResolvedRecordField<'a>{
    pub fn schema(&self)->ResolvedNode<'a>{
        ResolvedNode::from_schema(&self.field.schema, self.root)
    }

    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn name(&self) -> &Arc<str>{
        &self.field.name
    }

    pub fn doc(&self)->&'a Documentation{
        &self.field.doc
    }

    pub fn default(&self)-> Option<&'a Value>{
        if self.field.default.is_some(){
            self.root.context.default_fields
                .get(&(Arc::clone(self.parent_name), Arc::clone(&self.field.name)))
                .map(|value|{value.as_ref()})
        }else{ // fast path, don't need to lookup default value
            None
        }
    }

    pub fn aliases(&self) -> &'a Vec<Arc<str>>{
        &self.field.aliases
    }

    pub fn custom_attributes(&self) -> &'a BTreeMap<String, serde_json::Value>{
        &self.field.custom_attributes
    }

}

/// Represents an Avro [Schema] with a type level promise that this `Schema` is self contained and
/// complete in that all named schema references have a unique defintion within this `Schema`.
#[derive(Debug)]
pub struct CompleteSchema(Schema);

impl CompleteSchema{
    /// Returns the [Parsing Canonical Form] of `self` that is self contained (not dependent on
    /// any definitions in `schemata`)
    ///
    /// [Parsing Canonical Form]:
    /// https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas
    pub fn independent_canonical_form(&self) -> Result<String, Error> { // TODO: I think this will
                                                                        // never *actually* return
                                                                        // Error..
        Ok(self.0.canonical_form())
    }
}

impl From<&ResolvedSchema> for CompleteSchema{
    fn from(value: &ResolvedSchema) -> Self {
        let mut stub = value.stub.as_ref().clone();
        unravel_inner(&mut stub, &value.context.definitions, &mut HashSet::new(),  &mut HashMap::new());
        CompleteSchema(stub)
    }
}

impl TryFrom<Schema> for CompleteSchema{
    type Error = Error;

    fn try_from(value: Schema) -> Result<Self, Error> {
        let resolved : ResolvedSchema = value.try_into()?;
        Ok((&resolved).into())
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
        compare_schema_and_context(&self.stub, &other.stub, &self.context.definitions, &other.context.definitions, &HashSet::new(), &HashSet::new())
    }
}


// KTODO: need to think about this a little more...
impl PartialEq for ResolvedNode<'_>{
    fn eq(&self, other: &Self) -> bool {
        compare_resolved(self, other)
    }
}

impl PartialEq for SchemaWithSymbols{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_and_context(&self.stub, &other.stub, &self.defined_names, &other.defined_names, &self.referenced_names, &other.referenced_names)
    }
}

impl TryFrom<SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: SchemaWithSymbols) -> AvroResult<Self> {
        ResolvedSchema::builder().build_one(schema)
    }
}

impl TryFrom<&SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &SchemaWithSymbols) -> AvroResult<Self> {
        ResolvedSchema::builder().build_one(schema.clone())
    }
}

impl TryFrom<Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: Schema) -> AvroResult<Self> {
        let with_symbols: SchemaWithSymbols = schema.into();
        ResolvedSchema::builder().build_one(with_symbols)
    }
}

/// NOTE: this will copy the schema, and is therefore not the most performant option
impl TryFrom<&Schema> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: &Schema) -> AvroResult<Self> {
        ResolvedSchema::builder().build_one(schema)
    }
}

/// Trait for implementing a custom schema name resolver. This resolved is designed to be used with
/// [`ResolvedSchema`]/[`ResolvedContext`] and will be called during schema name resolution if a local
/// defintion for the schema name can't be found.
/// # Example
/// ```rust
/// use apache_avro::schema::{ResolvedSchema,SchemaWithSymbols,Resolver, Name};
/// use std::{collections::HashMap, sync::Arc};
///
/// // Here is a basic resolver that stores schema definitions in a map.
/// struct MapResolver {
///     registry: HashMap<String, String>,
///     call_log: Vec<String>,
/// }
///
/// impl MapResolver {
///     fn new(entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
///         MapResolver {
///             registry: entries.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
///             call_log: Vec::new(),
///         }
///     }
///
///     fn calls(&self) -> &[String] {
///         &self.call_log
///     }
/// }
///
/// impl Resolver for MapResolver {
///     fn find_schema(&mut self, name: &Arc<Name>) -> Result<SchemaWithSymbols, String> {
///         let fullname = name.fullname(None);
///         self.call_log.push(fullname.clone());
///         match self.registry.get(&fullname) {
///             Some(json) => SchemaWithSymbols::parse_str(json)
///                 .map_err(|e| format!("parse error: {e}")),
///             None => Err(format!("not found: {fullname}")),
///         }
///     }
/// }
///
/// ```
pub trait Resolver{
    fn find_schema(&mut self, name: &Arc<Name>) -> Result<SchemaWithSymbols, String>;
}

#[derive(Default)]
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

/// compares the given two schema inside the context AND ALSO compares the contexts themselves. That
/// is, ensures the two contexts are identical
fn compare_schema_and_context(schema_one: &Schema, schema_two: &Schema, context_one: &NameMap, context_two: &NameMap, dangling_one: &NameSet, dangling_two: &NameSet) -> bool{
    if dangling_one != dangling_two{
        return false;
    }

    // verify the provided definitions are the same:
    let one_contains_two = context_one.keys().all(|val|{context_two.contains_key(val)});
    let two_contains_one = context_two.keys().all(|val|{context_one.contains_key(val)});

    if !(one_contains_two && two_contains_one){
        return false
    }

    // we now know that the two contexts claim to define the same set of names, lets verify

    if !context_one.iter().fold(true, |acc, (name, schema)|{acc &&
        schema.as_ref() == context_two.get(name).unwrap().as_ref()}) {
        return false
    }

    schema_one == schema_two

}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;

    use super::{Resolver, ResolvedSchema};
    use crate::{
        Schema,
        schema::{Alias, CompleteSchema, Name, RecordField, RecordSchema, ResolvedNode, SchemaWithSymbols, UnionSchema},
    };
    use apache_avro_test_helper::TestResult;

    // ---------- custom resolver infrastructure for tests ----------

    // A simple resolver backed by a HashMap of JSON schema strings keyed by fullname.
    struct MapResolver {
        registry: HashMap<String, String>,
        call_log: Vec<String>,
    }

    impl MapResolver {
        fn new(entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
            MapResolver {
                registry: entries.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
                call_log: Vec::new(),
            }
        }

        fn calls(&self) -> &[String] {
            &self.call_log
        }
    }

    impl Resolver for MapResolver {
        fn find_schema(&mut self, name: &Arc<Name>) -> Result<SchemaWithSymbols, String> {
            let fullname = name.fullname(None);
            self.call_log.push(fullname.clone());
            match self.registry.get(&fullname) {
                Some(json) => SchemaWithSymbols::parse_str(json)
                    .map_err(|e| format!("parse error: {e}")),
                None => Err(format!("not found: {fullname}")),
            }
        }
    }

    // ---------- custom resolver tests ----------

    #[test]
    fn custom_resolver_resolves_missing_record() -> TestResult {
        // A record that references "ext.Address" which is not provided locally.
        let person = r#"{
            "type": "record",
            "name": "Person",
            "namespace": "app",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "address", "type": "ext.Address"}
            ]
        }"#;

        let address = r#"{
            "type": "record",
            "name": "Address",
            "namespace": "ext",
            "fields": [
                {"name": "street", "type": "string"},
                {"name": "city", "type": "string"}
            ]
        }"#;

        let mut resolver = MapResolver::new([("ext.Address", address)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([person], &mut resolver)?;

        // The resolver was called for ext.Address
        assert!(resolver.calls().contains(&"ext.Address".to_string()));

        // The context should contain both Person and Address
        assert!(rs.get_names().contains_key(&Name::new("app.Person")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.Address")?));
        assert_eq!(rs.get_names().len(), 2);

        Ok(())
    }

    #[test]
    fn custom_resolver_resolves_missing_enum() -> TestResult {
        let order = r#"{
            "type": "record",
            "name": "Order",
            "namespace": "shop",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "status", "type": "shop.OrderStatus"}
            ]
        }"#;

        let status_enum = r#"{
            "type": "enum",
            "name": "OrderStatus",
            "namespace": "shop",
            "symbols": ["PENDING", "SHIPPED", "DELIVERED"]
        }"#;

        let mut resolver = MapResolver::new([("shop.OrderStatus", status_enum)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([order], &mut resolver)?;

        assert!(resolver.calls().contains(&"shop.OrderStatus".to_string()));
        assert!(rs.get_names().contains_key(&Name::new("shop.Order")?));
        assert!(rs.get_names().contains_key(&Name::new("shop.OrderStatus")?));

        Ok(())
    }

    #[test]
    fn custom_resolver_resolves_missing_fixed() -> TestResult {
        let msg = r#"{
            "type": "record",
            "name": "Message",
            "namespace": "proto",
            "fields": [
                {"name": "payload", "type": "string"},
                {"name": "checksum", "type": "proto.Checksum"}
            ]
        }"#;

        let checksum_fixed = r#"{
            "type": "fixed",
            "name": "Checksum",
            "namespace": "proto",
            "size": 16
        }"#;

        let mut resolver = MapResolver::new([("proto.Checksum", checksum_fixed)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([msg], &mut resolver)?;

        assert!(resolver.calls().contains(&"proto.Checksum".to_string()));
        assert!(rs.get_names().contains_key(&Name::new("proto.Message")?));
        assert!(rs.get_names().contains_key(&Name::new("proto.Checksum")?));

        Ok(())
    }

    #[test]
    fn custom_resolver_not_called_when_locally_resolved() -> TestResult {
        // Both schemas provided locally — resolver should never be called.
        let person = r#"{
            "type": "record",
            "name": "Person",
            "namespace": "app",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "address", "type": "app.Address"}
            ]
        }"#;

        let address = r#"{
            "type": "record",
            "name": "Address",
            "namespace": "app",
            "fields": [
                {"name": "street", "type": "string"}
            ]
        }"#;

        let mut resolver = MapResolver::new(Vec::<(&str, &str)>::new());
        let [rs] = ResolvedSchema::builder().additional(vec![address])?.build_array_with_resolver([person], &mut resolver)?;

        assert!(resolver.calls().is_empty(), "resolver should not be called when all names resolve locally");
        assert_eq!(rs.get_names().len(), 2);

        Ok(())
    }

    #[test]
    fn custom_resolver_transitive_resolution() -> TestResult {
        // A references B, B references C. Only A is provided; B and C come from the resolver.
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "chain",
            "fields": [{"name": "b", "type": "chain.B"}]
        }"#;

        let schema_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "chain",
            "fields": [{"name": "c", "type": "chain.C"}]
        }"#;

        let schema_c = r#"{
            "type": "record",
            "name": "C",
            "namespace": "chain",
            "fields": [{"name": "value", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([
            ("chain.B", schema_b),
            ("chain.C", schema_c),
        ]);

        let [rs] = ResolvedSchema::builder().build_array_with_resolver([schema_a], &mut resolver)?;

        // Both B and C should have been requested
        assert!(resolver.calls().contains(&"chain.B".to_string()));
        assert!(resolver.calls().contains(&"chain.C".to_string()));

        // All three should be in the context
        assert!(rs.get_names().contains_key(&Name::new("chain.A")?));
        assert!(rs.get_names().contains_key(&Name::new("chain.B")?));
        assert!(rs.get_names().contains_key(&Name::new("chain.C")?));
        assert_eq!(rs.get_names().len(), 3);

        Ok(())
    }

    #[test]
    fn custom_resolver_error_propagates() -> TestResult {
        // A references a name that the resolver cannot provide.
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [{"name": "x", "type": "ns.Missing"}]
        }"#;

        let mut resolver = MapResolver::new(Vec::<(&str, &str)>::new());
        let result = ResolvedSchema::builder().build_array_with_resolver([schema_a], &mut resolver);

        assert!(result.is_err(), "should fail when resolver cannot find the schema");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("ns.Missing"), "error should mention the missing name, got: {err_msg}");

        Ok(())
    }

    #[test]
    fn custom_resolver_mismatch_error() -> TestResult {
        // The resolver returns a schema that defines "wrong.Name" when asked for "ns.Expected".
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [{"name": "x", "type": "ns.Expected"}]
        }"#;

        let wrong_schema = r#"{
            "type": "record",
            "name": "WrongName",
            "namespace": "wrong",
            "fields": [{"name": "y", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([("ns.Expected", wrong_schema)]);
        let result = ResolvedSchema::builder().build_array_with_resolver([schema_a], &mut resolver);

        assert!(result.is_err(), "should fail when resolver returns a schema that doesn't define the requested name");

        Ok(())
    }

    #[test]
    fn custom_resolver_with_multiple_schemas_to_resolve() -> TestResult {
        // Two schemas that each reference an external type.
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [{"name": "shared", "type": "ext.Shared"}]
        }"#;

        let schema_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "ns",
            "fields": [{"name": "shared", "type": "ext.Shared"}]
        }"#;

        let shared = r#"{
            "type": "record",
            "name": "Shared",
            "namespace": "ext",
            "fields": [{"name": "value", "type": "string"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Shared", shared)]);
        let resolved = ResolvedSchema::builder().build_list_with_resolver(
            vec![schema_a, schema_b],
            &mut resolver,
        )?;

        assert_eq!(resolved.len(), 2);
        // Both resolved schemas should have ext.Shared in their context
        for rs in &resolved {
            assert!(rs.get_names().contains_key(&Name::new("ext.Shared")?));
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_mixed_local_and_external() -> TestResult {
        // A references B (provided locally) and C (provided by resolver).
        let schema_a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "ns",
            "fields": [
                {"name": "b", "type": "ns.B"},
                {"name": "c", "type": "ext.C"}
            ]
        }"#;

        let schema_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "ns",
            "fields": [{"name": "val", "type": "int"}]
        }"#;

        let schema_c = r#"{
            "type": "record",
            "name": "C",
            "namespace": "ext",
            "fields": [{"name": "val", "type": "long"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.C", schema_c)]);
        let [rs] = ResolvedSchema::builder().additional(vec![schema_b])?.build_array_with_resolver([schema_a], &mut resolver)?;

        // B resolved locally, C externally
        assert!(rs.get_names().contains_key(&Name::new("ns.A")?));
        assert!(rs.get_names().contains_key(&Name::new("ns.B")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.C")?));
        assert_eq!(rs.get_names().len(), 3);

        // Only C should have triggered a resolver call
        assert_eq!(resolver.calls().len(), 1);
        assert_eq!(resolver.calls()[0], "ext.C");

        Ok(())
    }

    #[test]
    fn custom_resolver_resolved_schema_resolves_node_correctly() -> TestResult {
        // Verify that a schema resolved via custom resolver actually works for
        // navigating the schema tree through ResolvedNode.
        let wrapper = r#"{
            "type": "record",
            "name": "Wrapper",
            "namespace": "ns",
            "fields": [
                {"name": "inner", "type": "ext.Inner"}
            ]
        }"#;

        let inner = r#"{
            "type": "record",
            "name": "Inner",
            "namespace": "ext",
            "fields": [
                {"name": "x", "type": "int"},
                {"name": "y", "type": "string"}
            ]
        }"#;

        let mut resolver = MapResolver::new([("ext.Inner", inner)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([wrapper], &mut resolver)?;

        // Walk the resolved schema tree
        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(record) => {
                assert_eq!(record.field_len(), 1);
                let inner_node = record.get_field(0).unwrap().schema();
                match inner_node {
                    ResolvedNode::Record(inner_record) => {
                        assert_eq!(inner_record.field_len(), 2);
                        assert_eq!(inner_record.get_field(0).unwrap().name().as_ref(), "x");
                        assert_eq!(inner_record.get_field(1).unwrap().name().as_ref(), "y");
                    },
                    other => panic!("expected Record for inner, got {other}"),
                }
            },
            other => panic!("expected Record for wrapper, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_in_union_field() -> TestResult {
        // A field has a union type where one variant comes from the resolver.
        let record = r#"{
            "type": "record",
            "name": "Event",
            "namespace": "ns",
            "fields": [
                {"name": "payload", "type": ["null", "ext.Payload"]}
            ]
        }"#;

        let payload = r#"{
            "type": "record",
            "name": "Payload",
            "namespace": "ext",
            "fields": [{"name": "data", "type": "bytes"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Payload", payload)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([record], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ext.Payload")?));

        // Walk the tree: Event -> payload field -> union -> second variant should be Record
        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(rec) => {
                let field_node = rec.get_field(0).unwrap().schema();
                match field_node {
                    ResolvedNode::Union(union) => {
                        assert_eq!(union.variants_len(), 2);
                        assert!(matches!(union.get_variant(0).unwrap(), ResolvedNode::Null));
                        assert!(matches!(union.get_variant(1).unwrap(), ResolvedNode::Record(_)));
                    },
                    other => panic!("expected Union, got {other}"),
                }
            },
            other => panic!("expected Record, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_in_array_items() -> TestResult {
        // An array whose items type comes from the resolver.
        let record = r#"{
            "type": "record",
            "name": "Container",
            "namespace": "ns",
            "fields": [
                {"name": "items", "type": {"type": "array", "items": "ext.Item"}}
            ]
        }"#;

        let item = r#"{
            "type": "record",
            "name": "Item",
            "namespace": "ext",
            "fields": [{"name": "label", "type": "string"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Item", item)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([record], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ext.Item")?));

        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(rec) => {
                let field_node = rec.get_field(0).unwrap().schema();
                match field_node {
                    ResolvedNode::Array(arr) => {
                        let items_node = arr.items();
                        assert!(matches!(items_node, ResolvedNode::Record(_)));
                    },
                    other => panic!("expected Array, got {other}"),
                }
            },
            other => panic!("expected Record, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_in_map_values() -> TestResult {
        // A map whose value type comes from the resolver.
        let record = r#"{
            "type": "record",
            "name": "Registry",
            "namespace": "ns",
            "fields": [
                {"name": "entries", "type": {"type": "map", "values": "ext.Entry"}}
            ]
        }"#;

        let entry = r#"{
            "type": "record",
            "name": "Entry",
            "namespace": "ext",
            "fields": [{"name": "value", "type": "double"}]
        }"#;

        let mut resolver = MapResolver::new([("ext.Entry", entry)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([record], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ext.Entry")?));

        let node = ResolvedNode::new(&rs);
        match node {
            ResolvedNode::Record(rec) => {
                let field_node = rec.get_field(0).unwrap().schema();
                match field_node {
                    ResolvedNode::Map(map) => {
                        let values_node = map.types();
                        assert!(matches!(values_node, ResolvedNode::Record(_)));
                    },
                    other => panic!("expected Map, got {other}"),
                }
            },
            other => panic!("expected Record, got {other}"),
        }

        Ok(())
    }

    #[test]
    fn custom_resolver_resolved_schema_equality() -> TestResult {
        // A schema resolved via custom resolver should equal the same schema resolved locally.
        let main = r#"{
            "type": "record",
            "name": "Main",
            "namespace": "ns",
            "fields": [{"name": "dep", "type": "ns.Dep"}]
        }"#;

        let dep = r#"{
            "type": "record",
            "name": "Dep",
            "namespace": "ns",
            "fields": [{"name": "v", "type": "int"}]
        }"#;

        // Resolve via custom resolver
        let mut resolver = MapResolver::new([("ns.Dep", dep)]);
        let [via_resolver] = ResolvedSchema::builder().build_array_with_resolver([main], &mut resolver)?;

        // Resolve by providing dep locally
        let [via_local] = ResolvedSchema::builder().additional(vec![dep])?.build_array([main])?;

        assert_eq!(via_resolver, via_local);

        Ok(())
    }

    #[test]
    fn custom_resolver_serialization_roundtrip() -> TestResult {
        // A schema resolved via custom resolver should serialize correctly.
        let main = r#"{
            "type": "record",
            "name": "ns.Main",
            "fields": [{"name": "dep", "type": "ns.Dep"}]
        }"#;

        let dep = r#"{
            "type": "record",
            "name": "ns.Dep",
            "fields": [{"name": "x", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([("ns.Dep", dep)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([main], &mut resolver)?;

        // Serialize and re-parse: the resulting schema should be self-contained.
        let json = serde_json::to_string(&rs)?;
        let reparsed = ResolvedSchema::parse_str(&json)?;
        assert_eq!(rs, reparsed);

        Ok(())
    }

    #[test]
    fn custom_resolver_resolver_returns_schema_with_extra_definitions() -> TestResult {
        // The resolver returns a schema that defines the requested name plus additional
        // names (e.g. a record with an inline enum). Those extra names should also be
        // available in the context.
        let main = r#"{
            "type": "record",
            "name": "Main",
            "namespace": "ns",
            "fields": [{"name": "detail", "type": "ext.Detail"}]
        }"#;

        // Detail defines an inline enum ext.Status
        let detail = r#"{
            "type": "record",
            "name": "Detail",
            "namespace": "ext",
            "fields": [
                {"name": "status", "type": {
                    "type": "enum",
                    "name": "Status",
                    "namespace": "ext",
                    "symbols": ["OK", "FAIL"]
                }}
            ]
        }"#;

        let mut resolver = MapResolver::new([("ext.Detail", detail)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([main], &mut resolver)?;

        assert!(rs.get_names().contains_key(&Name::new("ns.Main")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.Detail")?));
        assert!(rs.get_names().contains_key(&Name::new("ext.Status")?));

        Ok(())
    }

    #[test]
    fn custom_resolver_deep_transitive_chain() -> TestResult {
        // A -> B -> C -> D, all resolved externally.
        let schema_a = r#"{
            "type": "record", "name": "deep.A",
            "fields": [{"name": "b", "type": "deep.B"}]
        }"#;
        let schema_b = r#"{
            "type": "record", "name": "deep.B",
            "fields": [{"name": "c", "type": "deep.C"}]
        }"#;
        let schema_c = r#"{
            "type": "record", "name": "deep.C",
            "fields": [{"name": "d", "type": "deep.D"}]
        }"#;
        let schema_d = r#"{
            "type": "record", "name": "deep.D",
            "fields": [{"name": "val", "type": "int"}]
        }"#;

        let mut resolver = MapResolver::new([
            ("deep.B", schema_b),
            ("deep.C", schema_c),
            ("deep.D", schema_d),
        ]);

        let [rs] = ResolvedSchema::builder().build_array_with_resolver([schema_a], &mut resolver)?;

        for name in ["deep.A", "deep.B", "deep.C", "deep.D"] {
            assert!(rs.get_names().contains_key(&Name::new(name)?), "missing {name}");
        }
        assert_eq!(rs.get_names().len(), 4);

        // Walk all the way down
        let node = ResolvedNode::new(&rs);
        let ResolvedNode::Record(a) = node else { panic!("expected Record") };
        let ResolvedNode::Record(b) = a.get_field(0).unwrap().schema() else { panic!("expected Record") };
        let ResolvedNode::Record(c) = b.get_field(0).unwrap().schema() else { panic!("expected Record") };
        let ResolvedNode::Record(d) = c.get_field(0).unwrap().schema() else { panic!("expected Record") };
        let val = d.get_field(0).unwrap().schema();
        assert!(matches!(val, ResolvedNode::Int));

        Ok(())
    }

    #[test]
    fn custom_resolver_with_schema_with_symbols_api() -> TestResult {
        // Test using the SchemaWithSymbols-level API with a custom resolver.
        let main = r#"{
            "type": "record",
            "name": "ns.Main",
            "fields": [{"name": "ref_field", "type": "ns.External"}]
        }"#;

        let external = r#"{
            "type": "enum",
            "name": "ns.External",
            "symbols": ["X", "Y", "Z"]
        }"#;

        let main_sws = SchemaWithSymbols::parse_str(main)?;

        let mut resolver = MapResolver::new([("ns.External", external)]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver(
            [main_sws],
            &mut resolver,
        )?;

        assert!(rs.get_names().contains_key(&Name::new("ns.Main")?));
        assert!(rs.get_names().contains_key(&Name::new("ns.External")?));

        // Verify the field resolves to an Enum
        let ResolvedNode::Record(rec) = ResolvedNode::new(&rs) else { panic!("expected Record") };
        assert!(matches!(rec.get_field(0).unwrap().schema(), ResolvedNode::Enum(_)));

        Ok(())
    }

    #[test]
    fn custom_resolver_unravel_produces_self_contained_schema() -> TestResult {
        // After resolving via custom resolver, unravel() should produce a fully
        // self-contained schema with no Schema::Ref nodes.
        let main = r#"{
            "type": "record",
            "name": "ns.Root",
            "fields": [
                {"name": "tag", "type": "ns.Tag"},
                {"name": "meta", "type": "ns.Meta"}
            ]
        }"#;
        let tag = r#"{
            "type": "enum", "name": "ns.Tag",
            "symbols": ["ALPHA", "BETA"]
        }"#;
        let meta = r#"{
            "type": "fixed", "name": "ns.Meta", "size": 8
        }"#;

        let mut resolver = MapResolver::new([
            ("ns.Tag", tag),
            ("ns.Meta", meta),
        ]);
        let [rs] = ResolvedSchema::builder().build_array_with_resolver([main], &mut resolver)?;

        let unraveled = rs.unravel();

        // The unraveled schema should be a Record with inline definitions, no Ref nodes
        match &unraveled {
            Schema::Record(rec) => {
                assert_eq!(rec.fields.len(), 2);
                assert!(matches!(&rec.fields[0].schema, Schema::Enum(_)));
                assert!(matches!(&rec.fields[1].schema, Schema::Fixed(_)));
            },
            other => panic!("expected Record, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_enum_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_enum_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_fixed_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_qualified_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_fixed_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_record_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"inner_record_name",
                            "namespace":"inner_space",
                            "fields":[
                                {
                                    "name":"inner_field_1",
                                    "type":"double"
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_record_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_enum_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_resolution_inner_fixed_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size": 16
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_outer_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "space.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "space.middle_record_name",
            "space.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_middle_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "middle_namespace.middle_record_name",
            "middle_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_multi_level_resolution_inner_record_inner_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type":"record",
                            "name":"middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 3);
        for s in [
            "space.record_name",
            "middle_namespace.middle_record_name",
            "inner_namespace.inner_record_name",
        ] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_array_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"array",
                  "items":{
                      "type":"record",
                      "name":"in_array_record",
                      "fields": [
                          {
                              "name":"array_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_array_record"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.in_array_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3448_test_proper_in_map_resolution_inherited_namespace() -> TestResult {
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": {
                  "type":"map",
                  "values":{
                      "type":"record",
                      "name":"in_map_record",
                      "fields": [
                          {
                              "name":"map_record_field",
                              "type":"string"
                          }
                      ]
                  }
              }
            },
            {
                "name":"outer_field_2",
                "type":"in_map_record"
            }
          ]
        }
        "#;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "space.in_map_record"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_enum_inner_namespace() -> TestResult {
        let schema = r#"
        {
        "name": "record_name",
        "namespace": "space",
        "type": "record",
        "fields": [
            {
            "name": "outer_field_1",
            "type": [
                        "null",
                        {
                            "type":"enum",
                            "name":"inner_enum_name",
                            "namespace": "inner_space",
                            "symbols":["Extensive","Testing"]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_enum_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_enum_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema)?;
        let _schema = Schema::parse_str(&schema_str)?;
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_3466_test_to_json_inner_fixed_inner_namespace() -> TestResult {
        let schema = r#"
        {
        "name": "record_name",
        "namespace": "space",
        "type": "record",
        "fields": [
            {
            "name": "outer_field_1",
            "type": [
                        "null",
                        {
                            "type":"fixed",
                            "name":"inner_fixed_name",
                            "namespace": "inner_space",
                            "size":54
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_space.inner_fixed_name"
            }
        ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let [rs] = ResolvedSchema::builder().build_array([&schema])?;

        // confirm we have expected 2 full-names
        assert_eq!(rs.get_names().len(), 2);
        for s in ["space.record_name", "inner_space.inner_fixed_name"] {
            assert!(rs.get_names().contains_key(&Name::new(s)?));
        }

        // convert Schema back to JSON string
        let schema_str = serde_json::to_string(&schema)?;
        let _schema = Schema::parse_str(&schema_str)?;
        assert_eq!(schema, _schema);

        Ok(())
    }

    #[test]
    fn avro_rs_339_schema_ref_uuid() -> TestResult {
        let schema = r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "uuid",
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                }
            ]
        }"#;
        let _resolved = ResolvedSchema::builder().build_array([&schema])?;

        Ok(())
    }

    #[test]
    fn avro_rs_339_schema_ref_decimal() -> TestResult {
        let schema = r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                }
            ]
        }"#;
        let _resolved = ResolvedSchema::builder().build_array([&schema])?;

        Ok(())
    }

    #[test]
    fn avro_rs_444_do_not_allow_duplicate_names_in_known_schemata() -> TestResult {
        let schema = r#"{
            "name": "foo",
            "type": "record",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "decimal",
                        "precision": 4,
                        "scale": 2,
                        "name": "bar"
                    }
                },
                {
                    "name": "b",
                    "type": "bar"
                },
                {
                    "name": "c",
                    "type": {
                        "type": "fixed",
                        "size": 16,
                        "logicalType": "uuid",
                        "name": "duplicated_name"
                    }
                }
            ]
        }"#;

        let other_schema = r#"{
                "name": "duplicated_name",
                "type": "enum",
                "symbols": ["A", "B", "C"]

        }"#;

        let ambig_schema = r#""duplicated_name""#;

        let result = ResolvedSchema::builder().additional(vec![&schema, &other_schema])?.build_array([&ambig_schema])
            .unwrap_err();

        assert!(
            result.to_string().contains("Schamata with fullnames: [\"duplicated_name\"], already have definitions in this context.")
        );

        Ok(())
    }

    #[test]
    fn allow_identical_definitions() -> TestResult{
        let schema1 = r#"
        {
            "name": "someRecord",
            "type": "record",
            "fields": [
            {
                "name": "field1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B","C"]
                }
            }
            ]
        }
            "#;

        let schema2 = r#"
        {
            "name": "someOtherRecord",
            "type": "record",
            "fields": [
            {
                "name": "otherField1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B","C"]
                }
            }
            ]
        }
            "#;

        let _resolved = ResolvedSchema::builder().build_array([schema1, schema2])?;

        Ok(())
    }

    #[test]
    fn dont_allow_close_but_not_identical_definitions() -> TestResult{
        let union = "[someRecord, someOtherRecord]";

        let schema1 = r#"
        {
            "name": "someRecord",
            "type": "record",
            "fields": [
            {
                "name": "field1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B","C"]
                }
            }
            ]
        }
            "#;

        let schema2 = r#"
        {
            "name": "someOtherRecord",
            "type": "record",
            "fields": [
            {
                "name": "otherField1",
                "type": {
                    "name": "duplicateEnum",
                    "type": "enum",
                    "symbols":["A","B"]
                }
            }
            ]
        }
            "#;

        assert!(
            ResolvedSchema::builder().additional([schema1, schema2])?.build_array([union]).is_err()
            );

        Ok(())

    }

    #[test]
    fn unravel_is_alias_aware() -> TestResult{
        let complete = CompleteSchema::from(&ResolvedSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "LongList",
              "aliases": ["LinkedLongs"],
              "fields" : [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "LinkedLongs"]}
              ]
            }
        "#,
        )?);

        let mut lookup = BTreeMap::new();
        lookup.insert("value".into(), 0);
        lookup.insert("next".into(), 1);

        let expected = Schema::Record(RecordSchema {
            name: Name::new("LongList")?.into(),
            aliases: Some(vec![Alias::new("LinkedLongs").unwrap()]),
            doc: None,
            fields: vec![
                RecordField::builder()
                    .name("value".to_string())
                    .schema(Schema::Long)
                    .build(),
                RecordField::builder()
                    .name("next".to_string())
                    .schema(Schema::Union(UnionSchema::new(vec![
                        Schema::Null,
                        Schema::Ref {
                            name: Name::new("LongList")?.into(),
                        },
                    ])?))
                    .build(),
            ],
            lookup,
            attributes: Default::default(),
        });

        assert_eq!(
                complete.0,
                expected
            );

        Ok(())
    }

    #[test]
    fn correct_comparison_after_resolved_node_traversal() -> TestResult{
        let schema1 = r#"{
            "name": "someRecord",
            "type": "record",
            "fields": [
                {
                    "name": "matchingInner",
                    "type": {
                        "name": "matchingInner",
                        "type": "record",
                        "fields": [{"name": "f1", "type": "long"}]
                    }
                },
                {
                    "name": "someLongField",
                    "type": "long"
                }
            ]
        }"#;

        let schema2 = r#"{
            "name": "someOtherRecord",
            "type": "record",
            "fields": [
                {
                    "name": "matchingInner",
                    "type": {
                        "name": "matchingInner",
                        "type": "record",
                        "fields": [{"name": "f1", "type": "long"}]
                    }
                },
                {
                    "name": "someStringField",
                    "type": "string"
                }
            ]
        }"#;

        let resolved1 = ResolvedSchema::parse_str(schema1)?;
        let resolved2 = ResolvedSchema::parse_str(schema2)?;

        let node1 = ResolvedNode::new(&resolved1);
        let node2 = ResolvedNode::new(&resolved2);

        let matching_inner1 = match node1 {
            ResolvedNode::Record(resolved_record) => {
                Some(resolved_record.fields().find(|field| field.name().as_ref() == "matchingInner").unwrap().schema())
            }
            _ => None
        };

        let matching_inner2 = match node2 {
            ResolvedNode::Record(resolved_record) => {
                Some(resolved_record.fields().find(|field| field.name().as_ref() == "matchingInner").unwrap().schema())
            }
            _ => None
        };

        assert_eq!(
            matching_inner1,
            matching_inner2
        );

        Ok(())
    }

    #[test]
    fn schema_not_needed_are_ignored() -> TestResult{
        let schema1 = r#"["enum1", "enum2", "myrecord"]"#;
        let schema2 = r#"{
            "name": "enum1",
            "type": "enum",
            "symbols": ["A","B","C"]
        }"#;
        let schema3 = r#"{
            "name": "enum2",
            "type": "enum",
            "symbols": ["F","W","D"]
        }"#;
        let schema4 = r#"{
            "name": "myrecord",
            "type": "record",
            "fields": [{"name": "myfield", "type": "long"}]
        }"#;

        let schema5 = r#"{
            "name": "myfixed",
            "type": "fixed",
            "size": 4
        }"#;

        let [rs] = ResolvedSchema::builder().additional([schema2, schema3, schema4, schema5])?.build_array([schema1])?;
        let my_fixed = rs.context.definitions.get(&Name::try_from("myfixed")?);

        assert_eq!(
            my_fixed,
            None
        );

        Ok(())
    }

    #[test]
    fn duplicate_in_provided_definitions_is_okay_if_equal() -> TestResult{
        let schema1 = r#"["enum1", "enum2", "myrecord"]"#;
        let schema2 = r#"{
            "name": "enum1",
            "type": "enum",
            "symbols": ["A","B","C"]
        }"#;
        let schema3 = r#"{
            "name": "enum2",
            "type": "enum",
            "symbols": ["F","W","D"]
        }"#;
        let schema4 = r#"{
            "name": "myrecord",
            "type": "record",
            "fields": [{"name": "myfield", "type": "long"}]
        }"#;

        let _rs = ResolvedSchema::builder().additional([&schema2, &schema3, &schema4, &schema4])?.build_array([&schema1])?;

        Ok(())
    }

    #[test]
    fn test_multiple_resolved_schema_at_once() -> TestResult{
        let union = r#"["enum1", "enum2"]"#;
        let record = r#"{
            "name": "myRecord",
            "type": "record",
            "fields": [
                {
                    "name": "f1",
                    "type": "enum1"
                },
                {
                    "name": "f2",
                    "type": "enum2"
                }
            ]
        }"#;
        let schema1 = r#"{
            "name": "enum1",
            "type": "enum",
            "symbols": ["A","B","C"]
        }"#;
        let schema2 = r#"{
            "name": "enum2",
            "type": "enum",
            "symbols": ["F","W","D"]
        }"#;

        let _rs = ResolvedSchema::builder().additional([&schema1, &schema2])?.build_array([&union, &record])?;

        Ok(())
    }

    const BAR_ENUM: &str = r#"{
        "type": "enum",
        "name": "bar",
        "symbols": ["A", "B", "C"]
    }"#;

    const BAZ_FIXED: &str = r#"{
        "type": "fixed",
        "name": "baz",
        "size": 16
    }"#;

    const QUX_ENUM: &str = r#"{
        "type": "enum",
        "name": "qux",
        "symbols": ["X", "Y"]
    }"#;

    /// baz as a record instead of fixed — conflicts with `BAZ_FIXED`
    const BAZ_RECORD: &str = r#"{
        "type": "record",
        "name": "baz",
        "fields": [{"name": "x", "type": "int"}]
    }"#;

    const FOO_USES_BAR: &str = r#"{
        "type": "record",
        "name": "foo",
        "fields": [
            {"name": "a", "type": "bar"}
        ]
    }"#;

    #[test]
    fn resolved_schema_only_includes_needed_definitions() {
        let [resolved] = ResolvedSchema::builder()
            .additional(vec![BAR_ENUM, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        let names: HashSet<String> = resolved
            .get_names()
            .keys()
            .map(|n| n.fullname(None))
            .collect();

        assert!(names.contains("foo"), "should contain root definition 'foo'");
        assert!(names.contains("bar"), "should contain referenced definition 'bar'");
        assert!(!names.contains("baz"), "should NOT contain unreferenced definition 'baz'");
    }

    #[test]
    fn equal_when_different_unused_definitions_in_pool() {
        // resolve foo with {bar, baz}
        let [resolved_a] = ResolvedSchema::builder()
            .additional(vec![BAR_ENUM, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        // resolve foo with {bar, qux}
        let [resolved_b] = ResolvedSchema::builder()
            .additional(vec![BAR_ENUM, QUX_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_eq!(
            resolved_a, resolved_b,
            "resolved schemas should be equal when only the unused pool members differ"
        );
    }

    #[test]
    fn equal_despite_conflicting_unused_definitions_across_pools() {
        // pool A has baz as a fixed
        let [resolved_a] = ResolvedSchema::builder()
            .additional(vec![BAR_ENUM, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        // pool B has baz as a record — conflicts with pool A's baz,
        // but neither resolution actually needs baz
        let [resolved_b] = ResolvedSchema::builder()
            .additional(vec![BAR_ENUM, BAZ_RECORD])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_eq!(
            resolved_a, resolved_b,
            "conflicting definitions of 'baz' shouldn't matter since neither schema uses it"
        );
    }

    #[test]
    fn not_equal_when_shared_definition_differs() {
        let bar_enum_alt: &str = r#"{
            "type": "enum",
            "name": "bar",
            "symbols": ["D", "E", "F"]
        }"#;

        let [resolved_a] = ResolvedSchema::builder()
            .additional(vec![BAR_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        let [resolved_b] = ResolvedSchema::builder()
            .additional(vec![bar_enum_alt])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_ne!(
            resolved_a, resolved_b,
            "resolved schemas should differ when a used definition differs"
        );
    }

    #[test]
    fn transitive_dependencies_included_but_not_unrelated() {
        // bar references baz, so resolving foo -> bar -> baz should pull in both
        let bar_uses_baz: &str = r#"{
            "type": "record",
            "name": "bar",
            "fields": [
                {"name": "b", "type": "baz"}
            ]
        }"#;

        let [resolved] = ResolvedSchema::builder()
            .additional(vec![bar_uses_baz, BAZ_FIXED, QUX_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        let names: HashSet<String> = resolved
            .get_names()
            .keys()
            .map(|n| n.fullname(None))
            .collect();

        assert!(names.contains("foo"), "root should be present");
        assert!(names.contains("bar"), "direct dependency should be present");
        assert!(names.contains("baz"), "transitive dependency should be present");
        assert!(!names.contains("qux"), "unrelated definition should NOT be present");
    }

    #[test]
    fn equal_with_transitive_deps_from_different_pools() {
        let bar_uses_baz: &str = r#"{
            "type": "record",
            "name": "bar",
            "fields": [
                {"name": "b", "type": "baz"}
            ]
        }"#;

        // pool has an extra unrelated schema
        let [resolved_a] = ResolvedSchema::builder()
            .additional(vec![bar_uses_baz, BAZ_FIXED, QUX_ENUM])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        // pool without the extra
        let [resolved_b] = ResolvedSchema::builder()
            .additional(vec![bar_uses_baz, BAZ_FIXED])
            .unwrap()
            .build_array([FOO_USES_BAR])
            .unwrap();

        assert_eq!(
            resolved_a, resolved_b,
            "transitive resolution should produce equal schemas regardless of extra pool members"
        );
    }

}
