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

use crate::schema::{self, derive, ArraySchema, DecimalSchema, EnumSchema, FixedSchema, LeafSchema, MapSchema, Name, RecordField, RecordSchema, Schema, SchemaWithSymbols, UnionSchema};
use crate::AvroResult;
use crate::error::{Details,Error};
use std::{collections::{HashMap, HashSet}, sync::Arc, iter::once};

/// contians a schema with all of the schema
/// definitions it needs to be completely resolved
/// This type is a promise from the API that each named
/// type in the schema has exactly one unique definition
/// and every named reference in the schema can be uniquely
/// resolved to one of these definitions.
#[derive(Debug)]
pub struct ResolvedSchema{
    pub schema: Arc<Schema>,
    context_definitions: HashMap<Arc<Name>,Arc<Schema>>
}

// convenience types
pub type NameMap = HashMap<Arc<Name>, Arc<Schema>>;
pub type NameSet = HashSet<Arc<Name>>;

impl ResolvedSchema{
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

        let mut cloned_to_resolve = to_resolve.clone().into_iter();
        Self::add_schemata(&mut definined_names, to_resolve.into_iter().chain(schemata_with_symbols), resolver)?;

        Ok(std::array::from_fn(|_|{
            let with_symbol = cloned_to_resolve.next().unwrap();
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

    pub fn get_context_definitions(&self) -> &NameMap{
       &self.context_definitions
    }
}

#[derive(Clone)]
pub struct ResolvedArray<'a>{
    schema: &'a Schema,
    array_schema: &'a ArraySchema,
    root: &'a ResolvedSchema
}

#[derive(Clone)]
pub struct ResolvedMap<'a>{
    schema: &'a Schema,
    map_schema: &'a MapSchema,
    root: &'a ResolvedSchema
}

#[derive(Clone)]
pub struct ResolvedUnion<'a>{
    schema: &'a Schema,
    union_schema: &'a UnionSchema,
    root: &'a ResolvedSchema
}

#[derive(Clone)]
pub struct ResolvedRecord<'a>{
    schema: &'a Schema,
    record_schema: &'a RecordSchema,
    root: &'a ResolvedSchema
}

#[derive(Clone)]
pub struct ResolvedRecordField<'a>{
    field: &'a RecordField,
    root: &'a ResolvedSchema
}

#[derive(Clone)]
pub enum ResolvedNode<'a>{
    Leaf(LeafSchema<'a>),
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
        Schema::Map(map_schema) => ResolvedNode::Map(ResolvedMap{schema, map_schema, root}),
        Schema::Union(union_schema) => ResolvedNode::Union(ResolvedUnion{schema, union_schema, root}),
        Schema::Array(array_schema) => ResolvedNode::Array(ResolvedArray{schema, array_schema, root}),
        Schema::Record(record_schema) => ResolvedNode::Record(ResolvedRecord{schema, record_schema, root}),
        Schema::Ref{name} => Self::from_schema(root.get_context_definitions().get(name).unwrap(), root),
        Schema::Null => ResolvedNode::Leaf(LeafSchema::Null),
        Schema::Boolean => ResolvedNode::Leaf(LeafSchema::Boolean),
        Schema::Int => ResolvedNode::Leaf(LeafSchema::Int),
        Schema::Long => ResolvedNode::Leaf(LeafSchema::Long),
        Schema::Float => ResolvedNode::Leaf(LeafSchema::Float),
        Schema::Double => ResolvedNode::Leaf(LeafSchema::Double),
        Schema::Bytes => ResolvedNode::Leaf(LeafSchema::Bytes),
        Schema::String => ResolvedNode::Leaf(LeafSchema::String),
        Schema::BigDecimal => ResolvedNode::Leaf(LeafSchema::BigDecimal),
        Schema::Uuid => ResolvedNode::Leaf(LeafSchema::Uuid),
        Schema::Date => ResolvedNode::Leaf(LeafSchema::Date),
        Schema::TimeMillis => ResolvedNode::Leaf(LeafSchema::TimeMillis),
        Schema::TimeMicros => ResolvedNode::Leaf(LeafSchema::TimeMicros),
        Schema::TimestampMillis => ResolvedNode::Leaf(LeafSchema::TimestampMillis),
        Schema::TimestampMicros => ResolvedNode::Leaf(LeafSchema::TimestampMicros),
        Schema::TimestampNanos => ResolvedNode::Leaf(LeafSchema::TimestampNanos),
        Schema::LocalTimestampMillis => ResolvedNode::Leaf(LeafSchema::LocalTimestampMillis),
        Schema::LocalTimestampMicros => ResolvedNode::Leaf(LeafSchema::LocalTimestampMicros),
        Schema::LocalTimestampNanos => ResolvedNode::Leaf(LeafSchema::LocalTimestampNanos),
        Schema::Duration => ResolvedNode::Leaf(LeafSchema::Duration),
        Schema::Enum(enum_schema) => ResolvedNode::Leaf(LeafSchema::Enum(schema, enum_schema)),
        Schema::Fixed(fixed_schema) => ResolvedNode::Leaf(LeafSchema::Fixed(schema, fixed_schema)),
        Schema::Decimal(decimal_schema) => ResolvedNode::Leaf(LeafSchema::Decimal(schema, decimal_schema))
       }
   }

   pub fn get_schema(&self)-> &Schema{
       match *self {
           Self::Leaf(ref schema) => schema.into(),
           Self::Array(ref resolved_array) => resolved_array.schema,
           Self::Union(ref resolved_union) => resolved_union.schema,
           Self::Record(ref resolved_record) => resolved_record.schema,
           Self::Map(ref resolved_map) => resolved_map.schema
       }
   }
}

impl<'a> ResolvedMap<'a>{
    pub fn resolve_types(&self)->ResolvedNode{
       ResolvedNode::from_schema(&self.map_schema.types, self.root)
    }

    pub fn get_map_schema(&self) -> &'a MapSchema{
        self.map_schema
    }
}

impl<'a> ResolvedUnion<'a>{
    pub fn resolve_schemas(&self)->Vec<ResolvedNode>{
        self.union_schema.schemas.iter().map(|schema|{
            ResolvedNode::from_schema(schema, self.root)
        }).collect()
    }

    pub fn get_union_schema(&self) -> &'a UnionSchema{
        self.union_schema
    }
}

impl<'a> ResolvedArray<'a>{
    pub fn resolve_items(&self)->ResolvedNode{
        ResolvedNode::from_schema(&self.array_schema.items, self.root)
    }

    pub fn get_array_schema(&self) -> &'a ArraySchema{
        self.array_schema
    }
}

impl<'a> ResolvedRecord<'a>{
    pub fn resolve_fields(&self)->Vec<ResolvedRecordField>{
        self.record_schema.fields.iter().map(|field|{
            ResolvedRecordField{
                field,
                root: self.root
            }
        }).collect()
    }

    pub fn get_record_schema(&self) -> &'a RecordSchema{
        self.record_schema
    }
}

impl<'a> ResolvedRecordField<'a>{
    pub fn resolve_field_schema(&self)->ResolvedNode{
        ResolvedNode::from_schema(&self.field.schema, self.root)
    }

    pub fn get_record_field(&self) -> &'a RecordField{
        self.field
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
        compare_schema_with_context(&self.schema, &other.schema, &self.context_definitions, &other.context_definitions, &HashSet::new(), &HashSet::new(), false)
    }
}

impl PartialEq for SchemaWithSymbols{
    fn eq(&self, other: &Self) -> bool {
        compare_schema_with_context(&self.schema, &other.schema, &self.defined_names, &other.defined_names, &self.referenced_names, &other.referenced_names, false)
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

fn compare_schema_with_context(schema_one: &Schema, schema_two: &Schema, context_one: &NameMap, context_two: &NameMap, dangling_one: &NameSet, dangling_two: &NameSet, include_attributes: bool) -> bool{
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
        compare_inner(schema.as_ref(), context_two.get(name).unwrap().as_ref(), include_attributes)}) {
        return false
    }

    compare_inner(schema_one, schema_two, include_attributes)

}

fn compare_inner(schema_one: &Schema, schema_two: &Schema, include_attributes: bool) -> bool {

    if schema_one.name() != schema_two.name() {
        return false;
    }

    if  include_attributes
        && schema_one.custom_attributes() != schema_two.custom_attributes()
    {
        return false;
    }

    match (schema_one, schema_two) {
        (Schema::Null, Schema::Null) => true,
        (Schema::Null, _) => false,
        (Schema::Boolean, Schema::Boolean) => true,
        (Schema::Boolean, _) => false,
        (Schema::Int, Schema::Int) => true,
        (Schema::Int, _) => false,
        (Schema::Long, Schema::Long) => true,
        (Schema::Long, _) => false,
        (Schema::Float, Schema::Float) => true,
        (Schema::Float, _) => false,
        (Schema::Double, Schema::Double) => true,
        (Schema::Double, _) => false,
        (Schema::Bytes, Schema::Bytes) => true,
        (Schema::Bytes, _) => false,
        (Schema::String, Schema::String) => true,
        (Schema::String, _) => false,
        (Schema::Uuid, Schema::Uuid) => true,
        (Schema::Uuid, _) => false,
        (Schema::BigDecimal, Schema::BigDecimal) => true,
        (Schema::BigDecimal, _) => false,
        (Schema::Date, Schema::Date) => true,
        (Schema::Date, _) => false,
        (Schema::Duration, Schema::Duration) => true,
        (Schema::Duration, _) => false,
        (Schema::TimeMicros, Schema::TimeMicros) => true,
        (Schema::TimeMicros, _) => false,
        (Schema::TimeMillis, Schema::TimeMillis) => true,
        (Schema::TimeMillis, _) => false,
        (Schema::TimestampMicros, Schema::TimestampMicros) => true,
        (Schema::TimestampMicros, _) => false,
        (Schema::TimestampMillis, Schema::TimestampMillis) => true,
        (Schema::TimestampMillis, _) => false,
        (Schema::TimestampNanos, Schema::TimestampNanos) => true,
        (Schema::TimestampNanos, _) => false,
        (Schema::LocalTimestampMicros, Schema::LocalTimestampMicros) => true,
        (Schema::LocalTimestampMicros, _) => false,
        (Schema::LocalTimestampMillis, Schema::LocalTimestampMillis) => true,
        (Schema::LocalTimestampMillis, _) => false,
        (Schema::LocalTimestampNanos, Schema::LocalTimestampNanos) => true,
        (Schema::LocalTimestampNanos, _) => false,
        (
            Schema::Record(RecordSchema { fields: fields_one, ..}),
            Schema::Record(RecordSchema { fields: fields_two, ..})
        ) => {
            compare_record_schema_fields(&fields_one, &fields_two, include_attributes)
        }
        (Schema::Record(_), _) => false,
        (
            Schema::Enum(EnumSchema { symbols: symbols_one, ..}),
            Schema::Enum(EnumSchema { symbols: symbols_two, .. })
        ) => {
            symbols_one == symbols_two
        }
        (Schema::Enum(_), _) => false,
        (
            Schema::Fixed(FixedSchema { size: size_one, ..}),
            Schema::Fixed(FixedSchema { size: size_two, .. })
        ) => {
            size_one == size_two
        }
        (Schema::Fixed(_), _) => false,
        (
            Schema::Union(UnionSchema { schemas: schemas_one, ..}),
            Schema::Union(UnionSchema { schemas: schemas_two, .. })
        ) => {
            schemas_one.len() == schemas_two.len()
                && schemas_one
                .iter()
                .zip(schemas_two.iter())
                .all(|(s1, s2)| compare_inner(s1, s2, include_attributes))
        }
        (Schema::Union(_), _) => false,
        (
            Schema::Decimal(DecimalSchema { precision: precision_one, scale: scale_one, inner: inner_one }),
            Schema::Decimal(DecimalSchema { precision: precision_two, scale: scale_two, inner: inner_two })
        ) => {
            precision_one == precision_two && scale_one == scale_two && compare_inner(inner_one, inner_two,include_attributes)
        }
        (Schema::Decimal(_), _) => false,
        (
            Schema::Array(ArraySchema { items: items_one, ..}),
            Schema::Array(ArraySchema { items: items_two, ..})
        ) => {
            compare_inner(items_one, items_two, include_attributes)
        }
        (Schema::Array(_), _) => false,
        (
            Schema::Map(MapSchema { types: types_one, ..}),
            Schema::Map(MapSchema { types: types_two, ..})
        ) => {
            compare_inner(types_one, types_two, include_attributes)
        }
        (Schema::Map(_), _) => false,
        (
            Schema::Ref { name: name_one },
            Schema::Ref { name: name_two }
        ) => {
            name_one == name_two
        }
        (Schema::Ref { .. }, _) => false,
    }
}

fn compare_record_schema_fields(fields_one: &[RecordField], fields_two: &[RecordField], include_attributes: bool) -> bool{
    fields_one.len() == fields_two.len()
        && fields_one
            .iter()
            .zip(fields_two.iter())
            .all(|(f1, f2)| f1.name == f2.name && compare_inner(&f1.schema, &f2.schema, include_attributes))
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
