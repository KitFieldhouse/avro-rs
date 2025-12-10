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

// this struct is a context where 
// each names schema reference *must*
// have a resolution in the context.
use crate::schema::{Schema, SchemaWithSymbols, Name,};
use crate::AvroResult;
use crate::error::Details;
use std::{collections::{HashMap, HashSet}, sync::Arc, iter::once};

#[derive(Debug)]
pub struct ResolvedContext<T: Resolver>{
       defined_names: HashMap<Arc<Name>,Arc<Schema>>,
       resolver: T
}

impl<T : Resolver> ResolvedContext<T>{

    /// creates a new resolved context from the supplied schemata with symbols. The supplied schemata must be a
    /// covering set of all referenced named schemata. If an external resolver is required, use 
    /// new_from_schemata_with_symbols_with_resolver.
    pub fn new(schemata_with_symbols: impl Iterator<Item = SchemaWithSymbols>, resolver: T) -> AvroResult<ResolvedContext<T>> {

       let mut context = ResolvedContext{
            defined_names: HashMap::new(),
            resolver: resolver 
        };
        
       context.add_schemata_with_symbols(schemata_with_symbols)?;

       Ok(context)
    }

    /// TODO: work on documentation
    /// stitches the schema together into parsing canonical form and inlines all needed defintions
    /// from the context.
    pub fn independent_canonical_form(&self, schema: Schema, defined_schemata: &mut HashSet<Arc<Name>>) -> AvroResult<Schema>{
        match schema {
            Schema::Ref{ref name}=> {
                if defined_schemata.contains(name) {Ok(schema)} else {
                    defined_schemata.insert(Arc::clone(&name));
                    Ok(self.independent_canonical_form(self.return_schema_by_name(name)?, defined_schemata)?)
                }               
            },
            Schema::Record(mut record_schema) => {
                defined_schemata.insert(Arc::clone(&record_schema.name));
                for field in &mut record_schema.fields {
                    field.schema = self.independent_canonical_form(field.schema.clone(), defined_schemata)?; 
                };
                Ok(Schema::Record(record_schema))
            }
            Schema::Array(array_schema) => {
                Ok(self.independent_canonical_form(*array_schema.items ,defined_schemata)?)
            }
            Schema::Map(map_schema) => {
                Ok(self.independent_canonical_form(*map_schema.types, defined_schemata)?)
            }
            Schema::Union(mut union_schema) => {
                for el_schema in &mut union_schema.schemas {
                    *el_schema = self.independent_canonical_form(el_schema.clone(), defined_schemata)?;
                }
                Ok(Schema::Union(union_schema))
            },
            Schema::Fixed(ref fixed_schema) => {
                defined_schemata.insert(Arc::clone(&fixed_schema.name));
                Ok(schema)
            },
            Schema::Enum(ref enum_schema) => {
                defined_schemata.insert(Arc::clone(&enum_schema.name));
                Ok(schema)
            }
            _ => {Ok(schema)}
        }
    }


    fn check_if_conflicts<'a>(&self, names: impl Iterator<Item = &'a Arc<Name> >) -> AvroResult<()>{
        let mut conflicting_fullnames : Vec<String> = Vec::new();
        for name in names{
            if self.defined_names.contains_key(name){
                conflicting_fullnames.push(name.fullname(Option::None));
            }
        }

        if conflicting_fullnames.len() == 0 {
            Ok(())
        }else{
            Err(Details::MultipleNameCollision(conflicting_fullnames).into())
        }
    }
    
    /// returns the names in the supplied iterator that are not defined in this context
    fn find_unresolved(&self, names: impl Iterator<Item = Arc<Name>>) -> HashSet<Arc<Name>>{

        let mut unresolved : HashSet<Arc<Name>> = HashSet::new();

        for name in names {
            if !self.defined_names.contains_key(&name){
                unresolved.insert(name);
            }
        }

        unresolved
    }

    /// Add schema with symbols into this context. No "dangling" references are allowed
    pub fn add_schema_with_symbols(&mut self, schema_with_symbol: SchemaWithSymbols) -> AvroResult<()>{
        self.add_schemata_with_symbols(once(schema_with_symbol))
    }

    fn add_schemata_with_symbols(&mut self, schemata: impl Iterator<Item = SchemaWithSymbols>) -> AvroResult<()>{
        let mut references : HashSet<Arc<Name>> = HashSet::new();

        for schema_with_symbol in schemata{
            self.check_if_conflicts(schema_with_symbol.defined_names.keys())?;
            self.defined_names.extend(schema_with_symbol.defined_names);
            references.extend(schema_with_symbol.referenced_names);
        }

        for schema_ref in references{
            self.resolve_name(&schema_ref)?;
        }

        Ok(())

    }

    /// This method attempts to match the provided name with a schema definition in this context.
    /// If the name resolves to a schema that requires additional resolutions, this is done
    /// recursivly. TODO: fix up docs
    /// 
    fn resolve_name(&mut self, name: &Arc<Name>) -> AvroResult<&Arc<Schema>>{
       
        // first, check internal cache
        if self.defined_names.contains_key(name) {
            return Ok(&self.defined_names.get(name).unwrap());
        }
         
        
        // second, use provided resolver
        match self.resolver.find_schema(name){
            Ok(schema_with_symbols) => {

                // check that what we got back from the resolver actually matches what we expect
                if !schema_with_symbols.defined_names.contains_key(name) {
                    return Err(Details::CustomSchemaResolverMismatch(name.as_ref().clone(), 
                            Vec::from_iter(schema_with_symbols.defined_names.keys().map(|key| {key.as_ref().clone()}))).into())
                }
                // matches, lets add to the cache and recursively resolve on this new schema
                self.add_schema_with_symbols(schema_with_symbols)?;
                self.resolve_name(name) 
            },
            Err(msg) => {
                return Err(Details::SchemaResolutionErrorWithMsg(name.as_ref().clone(), msg).into());
            }
        }
    }

    /// Looks up the schema name in this context's cache. This is a convenience method that
    /// simply handles generating an error message if no schema is found
    /// Returns the schema if found.
    fn return_schema_by_name(&self, name: &Arc<Name>) -> AvroResult<Schema>{
        if !self.defined_names.contains_key(name) {
            return Err(Details::SchemaLookupError(name.as_ref().clone()).into());
        } 
        Ok(self.defined_names.get(name).unwrap().as_ref().clone())
    }
}

/// Wrapper for containing a Schema that has been resolved in a given resolved context.
/// Can only be constructed from a ResolutionContext.
pub(crate) struct ResolvedSchema<'a, T : Resolver>{
    schema: Arc<Schema>,
    context: &'a ResolvedContext<T> 
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
