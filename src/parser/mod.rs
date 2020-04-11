// Allow wildcards in match statements for the parsing rules `Rule::*`.
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::unused_self)]

use crate::computations::ComputationProperties;
use crate::computations::ComputationType;
use crate::error::{parsing_error, GraphSurgeError};
use crate::graph::key_store::KeyId;
use crate::graph::key_store::KeyStore;
use crate::graph::properties::operations::{Operand, Operator, RightOperand};
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::serde::DEFAULT_SERDE_THREADS;
use crate::graph::stream_data::aggregation::AggregationOperation;
use crate::graph::stream_data::edge_data::get_edge_closure;
use crate::graph::stream_data::vertex_data::get_vertex_closure;
use crate::graph::VertexOrEdge;
use crate::query_handler::create_aggregated_cube::CreateAggregatedCubeAst;
use crate::query_handler::create_filtered_cube::CreateViewCollectionAst;
use crate::query_handler::create_view::{
    AggregateClause, CreateViewAst, GroupClause, GroupCondition, SectionDetails, WhereCondition,
    WhereConditions, WherePredicate,
};
use crate::query_handler::delete_cubes::DeleteCollectionsAst;
use crate::query_handler::generate_cube::GenerateCubeAst;
use crate::query_handler::load_cube::LoadCubeAst;
use crate::query_handler::load_graph::executor::DEFAULT_HAS_HEADERS;
use crate::query_handler::load_graph::LoadGraphAst;
use crate::query_handler::run_computation::{MaterializeResults, RunComputationAst};
use crate::query_handler::serde::{Operation, Serde};
use crate::query_handler::set_threads::SetThreads;
use crate::query_handler::show_computations::ShowComputationsAst;
use crate::query_handler::show_cube_data::ShowCollectionDataAst;
use crate::query_handler::show_cubes::ShowCollectionsAst;
use crate::query_handler::window_cube::WindowCubeAst;
use crate::query_handler::write_cube::WriteCubeAst;
use crate::query_handler::write_graph::WriteGraphAst;
use crate::query_handler::GraphSurgeQuery;
use hashbrown::HashMap;
use hashbrown::HashSet;
use pest::iterators::Pair;
use pest::iterators::Pairs;
use pest::Parser;
use pest_derive::Parser;
use std::convert::TryFrom;
use std::fmt::Arguments;
use std::num::NonZeroU8;

#[derive(Parser)]
#[grammar = "parser/graphsurge_grammar.pest"]
pub struct GraphSurgeParser<'a> {
    key_store: &'a KeyStore,
}

impl<'a> GraphSurgeParser<'a> {
    pub fn new(key_store: &'a KeyStore) -> Self {
        Self { key_store }
    }

    pub fn parse_query(
        &self,
        query_str: &str,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut root = GraphSurgeParser::parse(Rule::graphsurge_query, query_str)
            .map_err(|e| parsing_error(format!("\n{}", e)))?;

        let graphsurge_query_rule = get_next_rule(&mut root, "graphsurge_query")?;
        let queries_rule = inner_and_get_next_rule(graphsurge_query_rule)?;

        match queries_rule.as_rule() {
            Rule::load_graph => self.parse_load_graph(queries_rule),
            Rule::write_graph => self.parse_write_graph(queries_rule),
            Rule::write_collection => self.parse_write_collection(queries_rule),
            Rule::show_queries => self.parse_show_queries(queries_rule),
            Rule::create_view_or_collection => self.parse_create_view_or_collection(queries_rule),
            Rule::create_aggregated_cube => self.parse_create_aggregated_cube(queries_rule),
            Rule::delete_collections => Ok(Box::new(DeleteCollectionsAst {})),
            Rule::set_threads => {
                let mut rules = queries_rule.into_inner();
                Ok(Box::new(SetThreads(
                    self.parse_num_usize(rules.next(), "set_threads")?,
                    self.parse_num_usize(rules.next(), "set_index")?,
                )))
            }
            Rule::serialize_graph => self.parse_serde(queries_rule, Operation::SerializeGraph),
            Rule::deserialize_graph => self.parse_serde(queries_rule, Operation::DeserializeGraph),
            Rule::load_collection => self.parse_load_collection(queries_rule),
            Rule::generate_collection => self.parse_generate_collection(queries_rule),
            Rule::generate_windowed_collection => self.parse_window_collection(queries_rule),
            Rule::serialize_collection => self.parse_serialize_collection(queries_rule),
            Rule::deserialize_collection => {
                self.parse_serde(queries_rule, Operation::DeserializeCollection)
            }
            Rule::run_computation => self.parse_run_computation(queries_rule),
            r => Err(unknown_rule_error("graphsurge_query::query", r)),
        }
    }

    fn parse_load_graph(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let mut vertex_file = None;
        let mut only_edge_files = true;
        let next_rule = get_next_rule(&mut rules, "load_graph::vertex_file?")?;
        let next_rule = if next_rule.as_rule() == Rule::vertex_file {
            let rule = inner_and_get_next_rule(next_rule)?;
            only_edge_files = false;
            vertex_file = Some(self.parse_string(Some(rule), "vertex_file::non_empty_string")?);
            get_next_rule(&mut rules, "load_graph::edge_file")?
        } else {
            next_rule
        };

        let edge_file = self.parse_string(Some(next_rule), "load_graph::non_empty_string")?;

        let mut separator = None;
        let mut comment_char = None;
        let mut has_headers = DEFAULT_HAS_HEADERS;
        let mut save_mappings_dir = None;
        let mut randomize = false;
        for rule in rules {
            match rule.as_rule() {
                Rule::separator => {
                    separator = Some(
                        self.parse_string(
                            rule.into_inner().next(),
                            "load_graph::separator::non_empty_string",
                        )?
                        .bytes()
                        .next()
                        .expect("One byte expected"),
                    );
                }
                Rule::comment_char => {
                    comment_char = Some(
                        self.parse_string(
                            rule.into_inner().next(),
                            "load_graph::comment_char::non_empty_string",
                        )?
                        .bytes()
                        .next()
                        .expect("One byte expected"),
                    );
                }
                Rule::has_headers => {
                    let mut rules = rule.into_inner();
                    let bool_rule = get_next_rule(&mut rules, "load_graph::has_headers")?;
                    has_headers = self.parse_bool(bool_rule, "load_graph::has_headers::bool")?;
                }
                Rule::save_mappings => {
                    let mut rules = rule.into_inner();
                    save_mappings_dir = Some(self.parse_string(
                        rules.next(),
                        "load_graph::save_mappings::non_empty_string",
                    )?);
                }
                Rule::keyword_randomize => {
                    randomize = true;
                }
                r => {
                    return Err(unknown_rule_error("load_graph", r));
                }
            }
        }

        Ok(Box::new(LoadGraphAst::new(
            only_edge_files,
            vertex_file,
            edge_file,
            separator,
            comment_char,
            has_headers,
            save_mappings_dir,
            randomize,
        )))
    }

    fn parse_load_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let name = self.parse_variable(rules.next(), "load_cube::variable")?;
        let m = self.parse_num_usize(rules.next(), "load_cube::num_usize")?;
        let n = self.parse_num_usize(rules.next(), "load_cube::num_usize")?;
        let dir = self.parse_string(rules.next(), "load_cube::non_empty_string")?;
        let prefix = self.parse_string(rules.next(), "load_cube::non_empty_string")?;

        let mut separator = None;
        let mut comment_char = None;
        let mut threads = None;
        let mut with_full = false;
        for rule in rules {
            match rule.as_rule() {
                Rule::separator => {
                    separator = Some(
                        self.parse_string(
                            rule.into_inner().next(),
                            "load_cube::separator::non_empty_string",
                        )?
                        .chars()
                        .next()
                        .expect("One char expected"),
                    );
                }
                Rule::comment_char => {
                    comment_char = Some(
                        self.parse_string(
                            rule.into_inner().next(),
                            "load_cube::comment_char::non_empty_string",
                        )?
                        .chars()
                        .next()
                        .expect("One char expected"),
                    );
                }
                Rule::threads => {
                    threads = Some(self.parse_num_usize(
                        rule.into_inner().next(),
                        "load_cube::threads::num_usize",
                    )?);
                }
                Rule::with_full => {
                    with_full = true;
                }
                r => {
                    return Err(unknown_rule_error("load_cube", r));
                }
            }
        }

        Ok(Box::new(LoadCubeAst::new(
            name,
            m,
            n,
            dir,
            prefix,
            separator,
            comment_char,
            threads,
            with_full,
        )))
    }

    fn parse_window_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let name = self.parse_variable(rules.next(), "generate_cube::variable")?;
        let first_view = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let small_diffs = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let large_diff = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let diff_batch_count = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let total_batch_count = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let graph_filename = self.parse_string(rules.next(), "generate_cube::non_empty_string")?;

        Ok(Box::new(WindowCubeAst::new(
            name,
            first_view,
            small_diffs,
            large_diff,
            diff_batch_count,
            total_batch_count,
            graph_filename,
        )))
    }

    fn parse_generate_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let name = self.parse_variable(rules.next(), "generate_cube::variable")?;
        let first_view = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let adds = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let dels = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let batch_count = self.parse_num_usize(rules.next(), "generate_cube::num_usize")?;
        let graph_filename = self.parse_string(rules.next(), "generate_cube::non_empty_string")?;

        Ok(Box::new(GenerateCubeAst::new(
            name,
            first_view,
            adds,
            dels,
            batch_count,
            graph_filename,
        )))
    }

    fn parse_write_graph(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let filename = self.parse_string(rules.next(), "write_graph::non_empty_string")?;

        Ok(Box::new(WriteGraphAst::new(filename)))
    }

    fn parse_write_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let name = self.parse_variable(rules.next(), "write_cube::variable")?;
        let filename = self.parse_string(rules.next(), "write_cube::non_empty_string")?;
        let threads = self.parse_num_usize(rules.next(), "write_cube::num_usize")?;
        let gb =
            self.parse_bool(get_next_rule(&mut rules, "write_cube::bool")?, "write_cube::bool")?;

        Ok(Box::new(WriteCubeAst::new(name, filename, threads, gb)))
    }

    fn parse_show_queries(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let rule = inner_and_get_next_rule(rule)?;

        match rule.as_rule() {
            Rule::keyword_computations => Ok(Box::new(ShowComputationsAst {})),
            Rule::keyword_collections => Ok(Box::new(ShowCollectionsAst {})),
            Rule::collection_data => {
                let name =
                    self.parse_string(rule.into_inner().next(), "show_queries::cube_data")?;
                Ok(Box::new(ShowCollectionDataAst::new(name)))
            }
            r => Err(unknown_rule_error("show_queries", r)),
        }
    }

    fn parse_serde(
        &self,
        rule: Pair<Rule>,
        operation: Operation,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let bin_dir = self.parse_string(rules.next(), "serialize::non_empty_string")?;

        let mut thread_count = DEFAULT_SERDE_THREADS;
        let mut block_size = None;

        if let Some(next_rule) = rules.next() {
            let rule = if operation == Operation::SerializeGraph {
                if next_rule.as_rule() == Rule::keyword_block_size {
                    block_size = Some(
                        self.parse_num_usize(rules.next(), "serialize::keyword_block_size::num")?,
                    );
                    rules.next()
                } else {
                    Some(next_rule)
                }
            } else {
                Some(next_rule)
            };

            if let Some(next_rule) = rule {
                if next_rule.as_rule() == Rule::keyword_threads {
                    thread_count =
                        self.parse_num_usize(rules.next(), "deserialize::keyword_threads::num")?;
                } else {
                    return Err(unknown_rule_error("deserialize", next_rule.as_rule()));
                };
            }
        }
        Ok(Box::new(Serde::new(operation, bin_dir, thread_count, block_size)))
    }

    fn parse_serialize_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let name = self.parse_variable(rules.next(), "serialize_collection::variable")?;
        let bin_dir = self.parse_string(rules.next(), "serialize_collection::non_empty_string")?;

        let mut thread_count = DEFAULT_SERDE_THREADS;
        let mut block_size = None;

        if let Some(next_rule) = rules.next() {
            let rule = if next_rule.as_rule() == Rule::keyword_block_size {
                block_size = Some(self.parse_num_usize(
                    rules.next(),
                    "serialize_collection::keyword_block_size::num",
                )?);
                rules.next()
            } else {
                Some(next_rule)
            };

            if let Some(next_rule) = rule {
                if next_rule.as_rule() == Rule::keyword_threads {
                    thread_count = self.parse_num_usize(
                        rules.next(),
                        "serialize_collection::keyword_threads::num",
                    )?;
                } else {
                    return Err(unknown_rule_error("serialize_collection", next_rule.as_rule()));
                };
            }
        }
        Ok(Box::new(Serde::new(
            Operation::SerializeCollection(name),
            bin_dir,
            thread_count,
            block_size,
        )))
    }

    fn parse_create_view_or_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let rule = get_next_rule(&mut rules, "create_view_or_collection")?;
        match rule.as_rule() {
            Rule::single_sections => Ok(Box::new(self.parse_sections(rule)?)),
            Rule::collection => self.parse_view_collection(rule),
            r => Err(unknown_rule_error("create_view_or_collection", r)),
        }
    }

    fn parse_create_aggregated_cube(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let sections_rule = get_next_rule(&mut rules, "create_aggregated_cube::sections")?;
        let ast = self.parse_sections(sections_rule)?;

        if ast.vertex_sections.len() != 1 {
            return Err(parsing_error(
                "Aggregated cube should have *one* 'vertices' section".to_string(),
            ));
        }
        let vertex_section_details =
            ast.vertex_sections.values().next().expect("Should be present");
        if vertex_section_details.group_clauses.len() != 1 {
            return Err(parsing_error(
                "Aggregated cube vertex should have *one* 'group by' clause".to_string(),
            ));
        }
        let group_length =
            vertex_section_details.group_clauses.iter().next().expect("Should be there").len();
        if group_length == 0 {
            return Err(parsing_error("Number of group conditions should not be 0".to_string()));
        }

        if ast.edge_sections.is_empty() {
            return Err(parsing_error(
                "Aggregated cube should have at least one 'edges' section".to_string(),
            ));
        }
        for (between_clause, edge_section_details) in ast.edge_sections.values() {
            if between_clause.is_some() {
                return Err(parsing_error(
                    "Aggregated cube edges should not have 'between' clauses".to_string(),
                ));
            }
            if !edge_section_details.group_clauses.is_empty() {
                return Err(parsing_error(
                    "Aggregated cube edges should not have 'group by' clauses".to_string(),
                ));
            }
        }

        Ok(Box::new(CreateAggregatedCubeAst::new(
            ast,
            u8::try_from(group_length).expect("Too many group clauses"),
        )))
    }

    fn parse_sections(&self, rule: Pair<Rule>) -> Result<CreateViewAst, GraphSurgeError> {
        let mut rules = rule.into_inner();
        let mut is_empty = true;

        let name = self.parse_variable(rules.next(), "sections::variable")?;

        let mut vertex_sections = HashMap::new();
        let mut vertex_labels = HashMap::new();
        let mut vertex_label_map = HashMap::new();
        for (vertices_section_rule, vertex_section_index) in
            get_next_and_inner_rules(&mut rules, "sections::vertices_sections")?.zip(1_u8..)
        {
            let vertex_section_index =
                NonZeroU8::new(vertex_section_index).expect("Should be non zero");
            let mut rules = vertices_section_rule.into_inner();

            // Parse (optional) vertex label.
            let next_rule = get_next_rule(&mut rules, "vertices_section::variable?")?;
            let next_rule = if next_rule.as_rule() == Rule::variable {
                let label = next_rule.as_str().to_string();
                if vertex_labels.contains_key(&label) {
                    // Duplicate label check
                    return Err(parsing_error(format!(
                        "Duplicate vertex label '{}' in create view",
                        label
                    )));
                }
                vertex_labels.insert(label.clone(), vertex_section_index);
                vertex_label_map.insert(vertex_section_index, label);
                get_next_rule(&mut rules, "vertices_section::view_details")?
            } else {
                next_rule
            };

            let vertex_details = self.parse_view_details(next_rule, VertexOrEdge::Vertex)?;
            if self.is_non_empty_view(&vertex_details) {
                is_empty = false;
            } else {
                return Err(parsing_error(format!(
                    "At least one vertex view definition should be present in section {}",
                    vertex_section_index
                )));
            }

            vertex_sections.insert(vertex_section_index, vertex_details);
        }

        let mut edge_sections = HashMap::new();
        let mut edge_labels = HashMap::new();
        let mut edge_label_map = HashMap::new();
        for (edge_section_rule, edge_section_index) in
            get_next_and_inner_rules(&mut rules, "sections::edges_sections")?.zip(1_u8..)
        {
            let edge_section_index =
                NonZeroU8::new(edge_section_index).expect("Should be non zero");
            let mut rules = edge_section_rule.into_inner();

            // Parse (optional) edge label.
            let next_rule = get_next_rule(&mut rules, "edge_sections::variable?")?;
            let next_rule = if next_rule.as_rule() == Rule::variable {
                let label = next_rule.as_str().to_string();
                if edge_labels.contains_key(&label) {
                    // Duplicate label check
                    return Err(parsing_error(format!(
                        "Duplicate edge label '{}' in create view",
                        label
                    )));
                }
                edge_labels.insert(label.clone(), edge_section_index);
                edge_label_map.insert(edge_section_index, label);
                get_next_rule(&mut rules, "edge_sections::between_vertices?")?
            } else {
                next_rule
            };
            // Parse (optional) between src and dst vertex labels.
            let mut src_dst_vertex_group = None;
            let next_rule = if next_rule.as_rule() == Rule::between_vertices {
                let mut inner_rules = next_rule.into_inner();

                let from_vertices_label =
                    self.parse_variable(inner_rules.next(), "between_vertices::variable[1]")?;
                let from_vertices_id =
                    *vertex_labels.get(&from_vertices_label).ok_or_else(|| {
                        parsing_error(format!(
                            "Vertex section label '{}' used but not defined",
                            from_vertices_label
                        ))
                    })?;
                let to_vertices_label =
                    self.parse_variable(inner_rules.next(), "between_vertices::variable[2]")?;
                let to_vertices_id = *vertex_labels.get(&to_vertices_label).ok_or_else(|| {
                    parsing_error(format!(
                        "Vertex section label '{}' used but not defined",
                        to_vertices_label
                    ))
                })?;
                src_dst_vertex_group = Some((from_vertices_id, to_vertices_id));

                get_next_rule(&mut rules, "edge_sections::view_details")?
            } else {
                next_rule
            };

            let edge_details = self.parse_view_details(next_rule, VertexOrEdge::Edge)?;
            if self.is_non_empty_view(&edge_details) {
                is_empty = false;
            } else {
                return Err(parsing_error(format!(
                    "At least one edge view definition should be present in section {}",
                    edge_section_index
                )));
            }

            edge_sections.insert(edge_section_index, (src_dst_vertex_group, edge_details));
        }

        let save_to = if let Some(next_rule) = rules.next() {
            Some(self.parse_string(Some(next_rule), "create_view::non_empty_string")?)
        } else {
            None
        };

        if is_empty {
            Err(parsing_error(
                "At least one vertex or edge view definition should be present".to_string(),
            ))
        } else {
            Ok(CreateViewAst::new(
                name,
                vertex_sections,
                edge_sections,
                (vertex_label_map, edge_label_map),
                save_to,
            ))
        }
    }

    fn parse_view_details(
        &self,
        rule: Pair<Rule>,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<SectionDetails, GraphSurgeError> {
        let mut rules = rule.into_inner();
        let mut view_details = SectionDetails::default();

        // Continue only if there is a next rule.
        let next_rule = match rules.next() {
            None => return Ok(view_details),
            Some(rule) => rule,
        };

        let next_rule = if next_rule.as_rule() == Rule::where_conditions {
            view_details.where_conditions =
                self.parse_where_conditions(next_rule, vertex_or_edge)?;
            // Continue only if there is a next rule.
            match rules.next() {
                None => return Ok(view_details),
                Some(rule) => rule,
            }
        } else {
            next_rule
        };

        let next_rule = if next_rule.as_rule() == Rule::group_clauses {
            view_details.group_clauses = self.parse_group_clauses(next_rule, vertex_or_edge)?;
            // Continue only if there is a next rule.
            match rules.next() {
                None => return Ok(view_details),
                Some(rule) => rule,
            }
        } else {
            next_rule
        };

        // If we reach here, aggregate_clauses exists.
        view_details.aggregate_clauses = self.parse_aggregate_clauses(next_rule)?;

        Ok(view_details)
    }

    fn is_non_empty_view(&self, view_details: &SectionDetails) -> bool {
        !view_details.where_conditions.is_empty()
            || !view_details.aggregate_clauses.is_empty()
            || !view_details.group_clauses.is_empty()
    }

    fn parse_where_conditions(
        &self,
        rule: Pair<Rule>,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<Vec<WhereCondition>, GraphSurgeError> {
        let rules = rule.into_inner();

        let mut where_conditions = Vec::new();
        for where_condition_rule in rules {
            let where_condition =
                self.parse_where_condition(where_condition_rule, vertex_or_edge)?;
            where_conditions.push(where_condition);
        }

        Ok(where_conditions)
    }

    fn parse_where_condition(
        &self,
        rule: Pair<Rule>,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<WhereCondition, GraphSurgeError> {
        let mut rule = rule.into_inner();

        let where_condition_rule = get_next_rule(&mut rule, "condition_clause")?;
        match where_condition_rule.as_rule() {
            Rule::positive_where_condition => Ok((
                false,
                self.parse_positive_where_condition(where_condition_rule, vertex_or_edge)?,
            )),
            Rule::negative_where_condition => {
                let positive_where_condition_rule = inner_and_get_next_rule(where_condition_rule)?;
                Ok((
                    true,
                    self.parse_positive_where_condition(
                        positive_where_condition_rule,
                        vertex_or_edge,
                    )?,
                ))
            }
            r => Err(unknown_rule_error("condition_clause", r)),
        }
    }

    fn parse_positive_where_condition(
        &self,
        rule: Pair<Rule>,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<Vec<WherePredicate>, GraphSurgeError> {
        let rule = rule.into_inner();

        let mut where_predicates = Vec::new();
        for where_predicate_rule in rule {
            let where_predicate =
                self.parse_where_predicate(where_predicate_rule, vertex_or_edge)?;
            where_predicates.push(where_predicate);
        }
        Ok(where_predicates)
    }

    fn parse_where_predicate(
        &self,
        rule: Pair<Rule>,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<WherePredicate, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let next_rule = get_next_rule(&mut rules, "where_clause::complex_variable")?;
        let operand1 = self.parse_complex_variable(
            next_rule,
            "where_clause::complex_variable",
            vertex_or_edge,
        )?;

        let operator = {
            let comparator_rule = get_next_rule(&mut rules, "where_clause::[operator]")?;
            match comparator_rule.as_rule() {
                Rule::char_less => Operator::Less,
                Rule::char_greater => Operator::Greater,
                Rule::char_less_equal => Operator::LessEqual,
                Rule::char_greater_equal => Operator::GreaterEqual,
                Rule::char_equal => Operator::Equal,
                Rule::char_not_equal => Operator::NotEqual,
                r => {
                    return Err(unknown_rule_error("where_clause::[operator]", r));
                }
            }
        };

        let next_rule = get_next_rule(&mut rules, "where_clause::variable_or_value")?;
        let value_or_variable_rule = inner_and_get_next_rule(next_rule)?;
        let operand2 = match value_or_variable_rule.as_rule() {
            Rule::value => RightOperand::Value(
                self.parse_value(value_or_variable_rule, "where_clause::variable_or_value::value")?,
            ),
            Rule::complex_variable => RightOperand::Variable(self.parse_complex_variable(
                value_or_variable_rule,
                "where_clause::variable_or_value::complex_variable",
                vertex_or_edge,
            )?),
            r => {
                return Err(unknown_rule_error("where_clause::variable_or_value", r));
            }
        };

        let predicate = (operand1, operator, operand2.clone());
        let closure = match vertex_or_edge {
            VertexOrEdge::Edge => get_edge_closure(operand1, operator, operand2),
            VertexOrEdge::Vertex => get_vertex_closure(operand1, operator, operand2),
        };

        Ok((predicate, closure))
    }

    fn parse_group_clauses(
        &self,
        rule: Pair<Rule>,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<Vec<GroupClause>, GraphSurgeError> {
        let rules = rule.into_inner();

        let mut group_clauses = Vec::new();
        for group_clause_rule in rules {
            let mut group_clause = Vec::new();
            for group_clause_inner_rule in group_clause_rule.into_inner() {
                let condition =
                    match group_clause_inner_rule.as_rule() {
                        Rule::variable => GroupCondition::Variable(self.get_key_id(
                            group_clause_inner_rule.as_str(),
                            "group_clause::variable",
                        )?),
                        Rule::where_predicate => GroupCondition::WherePredicate(
                            self.parse_where_predicate(group_clause_inner_rule, vertex_or_edge)?,
                        ),
                        Rule::dimension => {
                            let list: Result<Vec<WhereConditions>, _> = group_clause_inner_rule
                                .into_inner()
                                .map(|rules| self.parse_where_conditions(rules, vertex_or_edge))
                                .collect();
                            GroupCondition::List(list?)
                        }
                        r => return Err(unknown_rule_error("group_clause", r)),
                    };
                group_clause.push(condition)
            }
            group_clauses.push(group_clause);
        }

        Ok(group_clauses)
    }

    fn parse_aggregate_clauses(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Vec<AggregateClause>, GraphSurgeError> {
        let mut aggregate_clauses = Vec::new();
        for aggregate_clause_rule in rule.into_inner() {
            let mut rules = aggregate_clause_rule.into_inner();

            let property_key =
                self.parse_variable(rules.next(), "aggregate_clause::variable[1]")?;
            let operation = match self
                .parse_variable(rules.next(), "aggregate_clause::variable[2]")?
                .as_str()
            {
                "count" => AggregationOperation::Count,
                "avg" => AggregationOperation::Avg,
                f => {
                    return Err(parsing_error(format!(
                        "Aggregation function '{}' not supported",
                        f
                    )))
                }
            };
            let argument = self.parse_variable(rules.next(), "aggregate_clause::[argument]")?;
            let argument_key_id = self.get_key_id(&argument, "aggregate_clause::[argument]")?;

            aggregate_clauses.push((property_key, operation, argument_key_id));
        }
        Ok(aggregate_clauses)
    }

    fn parse_view_collection(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let name = self.parse_variable(rules.next(), "collection::variable")?;

        let mut dimensions = Vec::new();
        for dimensions_rule in get_next_and_inner_rules(&mut rules, "collection::dimensions")? {
            let dimension: Result<Vec<WhereConditions>, _> = dimensions_rule
                .into_inner()
                .map(|where_conditions_rules| {
                    self.parse_where_conditions(where_conditions_rules, VertexOrEdge::Edge)
                })
                .collect();
            dimensions.push(dimension?);
        }

        let mut manual_order = false;
        let mut materialized = false;
        let mut store_total_data = false;
        let mut hosts = Vec::new();

        for rule in rules {
            match rule.as_rule() {
                Rule::keyword_manually_ordered => {
                    manual_order = true;
                }
                Rule::keyword_materialized => {
                    materialized = true;
                }
                Rule::keyword_materialize_full_view => {
                    store_total_data = true;
                }
                Rule::hosts => {
                    for string_rule in rule.into_inner() {
                        hosts.push(self.parse_string(
                            Some(string_rule),
                            "collection::hosts::non_empty_string",
                        )?);
                    }
                }
                r => {
                    return Err(unknown_rule_error("collection", r));
                }
            }
        }

        Ok(Box::new(CreateViewCollectionAst::new(
            name,
            dimensions,
            manual_order,
            materialized,
            store_total_data,
            hosts,
        )))
    }

    fn parse_computation_property(
        &self,
        rule: Pair<Rule>,
    ) -> Result<ComputationProperties, GraphSurgeError> {
        let value = match rule.as_rule() {
            Rule::value => {
                ComputationProperties::Value(self.parse_value(rule, "computation_property::value")?)
            }
            Rule::pairs => {
                ComputationProperties::Pairs(self.parse_pairs(rule, "computation_property::pairs")?)
            }
            r => {
                return Err(unknown_rule_error("computation_property::[operand2]", r));
            }
        };
        Ok(value)
    }

    fn parse_pairs(
        &self,
        rule: Pair<Rule>,
        location: &str,
    ) -> Result<Vec<(usize, usize)>, GraphSurgeError> {
        let mut pairs = Vec::new();
        let location_num = &format!("{}::num_usize", location);

        for pair_rule in rule.into_inner() {
            let mut rule = pair_rule.into_inner();
            let left_num = self.parse_num_usize(rule.next(), location_num)?;
            let right_num = self.parse_num_usize(rule.next(), location_num)?;
            pairs.push((left_num, right_num));
        }

        Ok(pairs)
    }

    fn parse_value(
        &self,
        rule: Pair<Rule>,
        location: &str,
    ) -> Result<PropertyValue, GraphSurgeError> {
        let mut rule = rule.into_inner();
        let rule = get_next_rule(&mut rule, location)?;
        let value = match rule.as_rule() {
            Rule::num_isize => PropertyValue::Isize(self.parse_num_isize(Some(rule), location)?),
            Rule::string => {
                let type_str = self.parse_string(Some(rule), location)?;
                PropertyValue::String(type_str)
            }
            Rule::bool => {
                PropertyValue::Bool(self.parse_bool(rule, &format!("{}::bool", location))?)
            }
            Rule::pair => {
                let mut inner_rules = rule.into_inner();
                PropertyValue::Pair(
                    self.parse_num_isize(inner_rules.next(), location)?,
                    self.parse_num_isize(inner_rules.next(), location)?,
                )
            }
            r => {
                return Err(unknown_rule_error(location, r));
            }
        };
        Ok(value)
    }

    fn parse_run_computation(
        &self,
        rule: Pair<Rule>,
    ) -> Result<Box<dyn GraphSurgeQuery>, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let next_rule = get_next_rule(&mut rules, "run_computation::[type]?")?;
        let mut batch_size = None;
        let (c_type, next_rule) = match next_rule.as_rule() {
            Rule::keyword_differential => (ComputationType::Differential, rules.next()),
            Rule::keyword_arranged_differential => {
                (ComputationType::DifferentialArranged, rules.next())
            }
            Rule::keyword_adaptive => {
                batch_size =
                    Some(self.parse_num_usize(rules.next(), "run_computation::batch_size")?);
                (ComputationType::Adaptive, rules.next())
            }
            Rule::keyword_individual => (ComputationType::Individual, rules.next()),
            Rule::keyword_arranged_individual => {
                (ComputationType::IndividualArranged, rules.next())
            }
            Rule::keyword_comparedifferential => {
                (ComputationType::CompareDifferential, rules.next())
            }
            _ => (ComputationType::Adaptive, Some(next_rule)),
        };

        let computation = self.parse_variable(next_rule, "run_computation::variable[1]")?;

        let mut properties = HashMap::new();
        let next_rule = get_next_rule(&mut rules, "run_computation::computation_properties?")?;
        let next_rule = if next_rule.as_rule() == Rule::computation_properties {
            for property_rule in next_rule.into_inner() {
                let mut rule = property_rule.into_inner();
                let key = self.parse_variable(rule.next(), "computation_property key")?;
                let rule = get_next_rule(&mut rule, "computation_property value")?;
                let value = self.parse_computation_property(rule)?;
                properties.insert(key, value);
            }
            get_next_rule(&mut rules, "run_computation properties")?
        } else {
            next_rule
        };

        let mut file = None;
        let next_rule = if next_rule.as_rule() == Rule::non_empty_string {
            file = Some(self.parse_string(Some(next_rule), "run_computation file")?);
            get_next_rule(&mut rules, "run_computation (cube string)")?
        } else {
            next_rule
        };

        let cube_name = next_rule.as_str().to_string();

        let mut materialize_results = MaterializeResults::Full;
        let mut save_to = None;
        let mut hosts = Vec::new();
        let mut indices = None;

        for rule in rules {
            match rule.as_rule() {
                Rule::results => {
                    let rule = inner_and_get_next_rule(rule)?;
                    match rule.as_rule() {
                        Rule::keyword_no_results => {
                            materialize_results = MaterializeResults::None;
                        }
                        Rule::keyword_diff_results => {
                            materialize_results = MaterializeResults::Diff;
                        }
                        Rule::non_empty_string => {
                            save_to =
                                Some(self.parse_string(Some(rule), "run_computation::save_to")?);
                        }
                        r => {
                            return Err(unknown_rule_error(
                                "run_computation::[keyword_no_results]",
                                r,
                            ))
                        }
                    }
                }
                Rule::hosts => {
                    for string_rule in rule.into_inner() {
                        hosts.push(self.parse_string(
                            Some(string_rule),
                            "run_computation::hosts::non_empty_string",
                        )?)
                    }
                }
                Rule::split_indices => {
                    let mut data = HashSet::new();
                    for num_usize_rule in rule.into_inner() {
                        data.insert(self.parse_num_usize(
                            Some(num_usize_rule),
                            "create_filtered_cube::split_indices::num_usize",
                        )?);
                    }
                    indices = Some(data);
                }
                r => {
                    return Err(unknown_rule_error("run_computation", r));
                }
            }
        }

        Ok(Box::new(RunComputationAst::new(
            computation,
            file,
            properties,
            cube_name,
            c_type,
            materialize_results,
            save_to,
            hosts,
            indices,
            batch_size,
        )))
    }

    fn parse_string(
        &self,
        rule: Option<Pair<Rule>>,
        location: &str,
    ) -> Result<String, GraphSurgeError> {
        let string_rule = rule.ok_or_else(|| unwrap_error(format_args!("{}", location)))?;
        if string_rule.as_rule() != Rule::non_empty_string && string_rule.as_rule() != Rule::string
        {
            return Err(unknown_rule_error(location, string_rule.as_rule()));
        }
        Ok(string_rule
            .into_inner()
            .next()
            .ok_or_else(|| unwrap_error(format_args!("{}::inner_string", location)))?
            .as_str()
            .to_string())
    }

    fn parse_bool(&self, rule: Pair<Rule>, location: &str) -> Result<bool, GraphSurgeError> {
        match rule
            .into_inner()
            .next()
            .ok_or_else(|| unwrap_error(format_args!("{}", location)))?
            .as_rule()
        {
            Rule::bool_true => Ok(true),
            Rule::bool_false => Ok(false),
            r => Err(unknown_rule_error(location, r)),
        }
    }

    fn parse_num_isize(
        &self,
        rule: Option<Pair<Rule>>,
        location: &str,
    ) -> Result<isize, GraphSurgeError> {
        Ok(self.parse_variable(rule, location)?.parse().map_err(|e| {
            parsing_error(format!("Could not parse '{}' as isize: {:?}", location, e))
        })?)
    }

    fn parse_num_usize(
        &self,
        rule: Option<Pair<Rule>>,
        location: &str,
    ) -> Result<usize, GraphSurgeError> {
        Ok(self.parse_variable(rule, location)?.parse().map_err(|e| {
            parsing_error(format!("Could not parse '{}' as usize: {:?}", location, e))
        })?)
    }

    fn parse_complex_variable(
        &self,
        rule: Pair<Rule>,
        location: &str,
        vertex_or_edge: VertexOrEdge,
    ) -> Result<Operand, GraphSurgeError> {
        let mut rules = rule.into_inner();

        let variable_left = {
            let variable_left_rule = get_next_rule(&mut rules, location)?;
            variable_left_rule.as_str().to_string()
        };

        let next_rule = rules.next();
        let complex_variable = if let Some(variable_right_rule) = next_rule {
            let variable_right_key_id =
                self.get_key_id(variable_right_rule.as_str(), "complex_variable::variable[2]")?;
            match variable_left.as_str() {
                "u" => Operand::SourceVertex(variable_right_key_id),
                "v" => Operand::DestinationVertex(variable_right_key_id),
                v => {
                    return Err(parsing_error(format!(
                        "Unexpected operand selector '{}' at location '{}'",
                        v, location
                    )));
                }
            }
        } else if variable_left.as_str() == "e" {
            Operand::Edge
        } else {
            let variable_left_key_id =
                self.get_key_id(&variable_left, "complex_variable::variable[1]")?;
            Operand::Property(variable_left_key_id)
        };

        if vertex_or_edge == VertexOrEdge::Vertex {
            match complex_variable {
                Operand::Property(_) => (),
                p => {
                    return Err(parsing_error(format!(
                        "Vertex where condition cannot refer to edge properties '{:?}'",
                        p
                    )));
                }
            }
        }

        Ok(complex_variable)
    }

    fn parse_variable(
        &self,
        rule: Option<Pair<Rule>>,
        location: &str,
    ) -> Result<String, GraphSurgeError> {
        Ok(rule.ok_or_else(|| unwrap_error(format_args!("{}", location)))?.as_str().to_string())
    }

    fn get_key_id(&self, key_string: &str, location: &str) -> Result<KeyId, GraphSurgeError> {
        self.key_store.get_key_id(key_string).ok_or_else(|| {
            parsing_error(format!(
                "Key '{}' at '{}' not present in the store",
                key_string, location
            ))
        })
    }
}

fn inner_and_get_next_rule(rule: Pair<Rule>) -> Result<Pair<Rule>, GraphSurgeError> {
    let rule_token = rule.as_rule();
    let mut inner = rule.into_inner();
    inner.next().ok_or_else(|| unwrap_error(format_args!("{:?}", rule_token)))
}

fn get_next_and_inner_rules<'a>(
    rules: &'a mut Pairs<Rule>,
    location: &str,
) -> Result<Pairs<'a, Rule>, GraphSurgeError> {
    Ok(get_next_rule(rules, location)?.into_inner())
}

fn get_next_rule<'a>(
    rules: &'a mut Pairs<Rule>,
    location: &str,
) -> Result<Pair<'a, Rule>, GraphSurgeError> {
    rules.next().ok_or_else(|| unwrap_error(format_args!("{}", location)))
}

fn unwrap_error(location: Arguments<'_>) -> GraphSurgeError {
    parsing_error(format!("Unwrap parse error at location '{}'", location))
}

fn unknown_rule_error(location: &str, rule: Rule) -> GraphSurgeError {
    parsing_error(format!("Unknown rule '{}::{:?}'", location, rule))
}
