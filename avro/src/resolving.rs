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

use crate::schema::{Schema, SchemaWithSymbols, Name,};
use crate::AvroResult;
use crate::error::{Details,Error};
use std::{collections::{HashMap, HashSet}, sync::Arc, iter::once};

/// contians a schema with all of the schema
/// definitions it needs to be completely resolved
/// This type is a promise from the API that each named
/// type in the schema has exactly one unique definition
/// and every named reference in the schema can be uniquely
/// resolved to one of these definitions.
pub struct ResolvedSchema{
    pub(crate) schema: Arc<Schema>,
    pub(crate) context_definitions: HashMap<Arc<Name>,Arc<Schema>>
}

// convenience types
type NameMap = HashMap<Arc<Name>, Arc<Schema>>;
type NameSet = HashSet<Arc<Name>>;

impl ResolvedSchema{

    /// Takes two vectors of schemata. Both of these vectors are checked that they form a complete
    /// schema context in which there every named schema has a unique defition and every schema
    /// reference can be uniquely resolved to one of these definitions. The first vector of
    /// schemata are those in which we want the associated ResolvedSchema forms of the schema to be
    /// returned. The second vector are schemata that are used for schema resolution, but do not
    /// have their ResolvedSchema form returned.
    pub fn from_schemata(to_resolve: Vec<SchemaWithSymbols> , schemata_with_symbols: Vec<SchemaWithSymbols>, resolver: &mut impl Resolver) -> AvroResult<Vec<ResolvedSchema>> {

        let mut definined_names : NameMap = HashMap::new();

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
}

/// this is a schema object that is "self contained" in that it contains all named definitions
/// needed to encode/decode form this schema.
pub struct CompleteSchema(Schema);

impl From<ResolvedSchema> for CompleteSchema{
    fn from(value: ResolvedSchema) -> Self {

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

impl TryFrom<SchemaWithSymbols> for ResolvedSchema{
    type Error = Error;

    fn try_from(schema: SchemaWithSymbols) -> AvroResult<Self> {
        let resolved_schema = ResolvedSchema::from_schemata(vec![schema], Vec::new(), &mut DefaultResolver::new())?.pop().unwrap();
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
    fn new()->Self{
        DefaultResolver{}
    }
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
