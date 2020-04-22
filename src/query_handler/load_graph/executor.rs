use crate::error::{load_graph_error, GraphSurgeError};
use crate::global_store::GlobalStore;
use crate::graph::key_store::KeyStore;
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::properties::Properties;
use crate::graph::properties::PropertyKeyId;
use crate::graph::Vertex;
use crate::graph::{Edge, Graph};
use crate::query_handler::load_graph::LoadGraphAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;
use crate::util::io::{get_buf_reader, GSWriter};
use crate::util::timer::GSTimer;
use csv::Reader;
use gs_analytics_api::VertexId;
use hashbrown::HashMap;
use log::{debug, info, warn};
use std::fs::File;
use std::io::BufReader;

const DEFAULT_TYPE_STRING: &str = "string";
const DEFAULT_SEPARATOR: u8 = b',';
pub const DEFAULT_HAS_HEADERS: bool = true;

type Function = dyn Fn(&str, &str, &str) -> Result<PropertyValue, GraphSurgeError>;
type PropertyValueClosure = Box<Function>;
type PropertyValueClosureIndex<'a> = &'a Function;
type ClosureMappings<'a> = HashMap<String, PropertyValueClosureIndex<'a>>;

impl GraphSurgeQuery for LoadGraphAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        global_store.graph.reset();
        global_store.key_store.reset();

        let mut total_vertices_count = 0;
        let mut total_edges_count = 0;

        let closures: Vec<PropertyValueClosure> = vec![
            Box::new(|property_string: &str, vertex_id_string: &str, file_path: &str| {
                property_string.parse().map(PropertyValue::Isize).map_err(|_| {
                    load_graph_error(format!(
                        "Could not parse property value '{}' as isize for vertex id '{}' in file '{}'",
                        property_string, vertex_id_string, file_path,
                    ))
                })
            }),
            Box::new(|property_string: &str, vertex_id_string: &str, file_path: &str| {
                property_string.parse().map(PropertyValue::Bool).map_err(|_| {
                    load_graph_error(format!(
                        "Could not parse property value '{}' as bool for vertex id '{}' in file '{}'",
                        property_string, vertex_id_string, file_path,
                    ))
                })
            }),
            Box::new(|property_string: &str, _vertex_id_string: &str, _file_path: &str| {
                Ok(PropertyValue::String(property_string.to_string()))
            }),
        ];

        let mappings = vec![
            ("int".into(), &*closures[0]),
            ("bool".into(), &*closures[1]),
            ("string".into(), &*closures[2]),
        ]
        .into_iter()
        .collect::<ClosureMappings>();

        let mut vertices_map = HashMap::new();
        if let Some(vertex_file) = &self.vertex_file {
            info!("Loading vertices from file '{}'", vertex_file);
            load_vertices(
                vertex_file,
                &mut vertices_map,
                global_store,
                &mut total_vertices_count,
                &mappings,
                &self,
            )?;
        }
        info!("Loading edges from file '{}'", self.edge_file);
        load_edges(
            &self.edge_file,
            &mut vertices_map,
            global_store,
            &mut total_vertices_count,
            &mut total_edges_count,
            &mappings,
            &self,
        )?;

        if self.randomize {
            global_store.graph.randomize_edges();
        }

        if let Some(dir) = &self.save_mappings_dir {
            let file_path = format!("{}/vertex_mappings.txt", dir);
            let mut file_buffer = GSWriter::new(file_path)?;
            file_buffer.write_file_lines(
                vertices_map
                    .iter()
                    .map(|(vertex_str, vertex_id)| format!("{},{}", vertex_str, vertex_id)),
            )?;
        }

        Ok(GraphSurgeResult::new(format!(
            "{} vertices and {} edges loaded",
            total_vertices_count, total_edges_count
        )))
    }
}

fn load_vertices(
    vertex_file: &str,
    vertices_map: &mut HashMap<String, VertexId>,
    global_store: &mut GlobalStore,
    total_vertices_count: &mut usize,
    mappings: &ClosureMappings,
    load_graph_ast: &LoadGraphAst,
) -> Result<(), GraphSurgeError> {
    let graph = &mut global_store.graph;
    let mut reader = get_csv_reader(vertex_file, load_graph_ast)?;

    let property_types = if load_graph_ast.has_headers {
        let schema_parts = reader.headers().map_err(|e| {
            load_graph_error(format!("Could not load headers from file '{}': {}", vertex_file, e))
        })?;
        let mut schema_iter = schema_parts.iter();

        if let Some(id_part) = schema_iter.next() {
            if id_part.contains(':') && !id_part.to_ascii_lowercase().contains(":id") {
                return Err(load_graph_error(format!(
                    "First column should be ':id' in file '{}'",
                    vertex_file
                )));
            }
        }
        Some(load_schema(schema_iter, vertex_file, &mut global_store.key_store, mappings)?)
    } else {
        None
    };

    let timer = GSTimer::now();
    for (index, line) in reader.records().filter_map(Result::ok).enumerate() {
        let mut line_parts = line.iter();

        let vertex_id_string = line_parts.next().ok_or_else(|| {
            load_graph_error(format!("Could not read vertex id from file '{}'", vertex_file))
        })?;
        if vertex_id_string == "" {
            return Err(load_graph_error(format!(
                "Vertex id string is empty in line '{}' in file '{}'",
                index, vertex_file
            )));
        }

        if vertices_map.contains_key(vertex_id_string) {
            return Err(load_graph_error(format!(
                "Duplicate vertex id '{}' found in file '{}'",
                vertex_id_string, vertex_file
            )));
        }

        let properties = if let Some(property_types) = &property_types {
            load_property_values(line_parts, vertex_id_string, vertex_file, property_types)?
        } else {
            Properties::default()
        };

        add_new_vertex(
            vertex_id_string.to_string(),
            properties,
            graph,
            vertices_map,
            total_vertices_count,
        );

        if index > 0 && index % 500_000 == 0 {
            info!("Processed {} vertices in {}", index, timer.elapsed().to_seconds_string());
        }
    }
    Ok(())
}

fn add_new_vertex(
    vertex_id_string: String,
    properties: Properties,
    graph: &mut Graph,
    vertices_map: &mut HashMap<String, VertexId>,
    total_vertices_count: &mut usize,
) -> VertexId {
    let vertex_id = graph.append_vertex(Vertex::new(properties));
    vertices_map.insert(vertex_id_string, vertex_id);
    *total_vertices_count += 1;
    vertex_id
}

fn load_edges(
    edge_file: &str,
    vertices_map: &mut HashMap<String, VertexId>,
    global_store: &mut GlobalStore,
    total_vertices_count: &mut usize,
    total_edges_count: &mut usize,
    mappings: &ClosureMappings,
    load_graph_ast: &LoadGraphAst,
) -> Result<(), GraphSurgeError> {
    let graph = &mut global_store.graph;
    let mut reader = get_csv_reader(edge_file, load_graph_ast)?;

    let property_types = if load_graph_ast.has_headers {
        let schema_parts = reader.headers().map_err(|e| {
            load_graph_error(format!("Could not load headers from file '{}': {}", edge_file, e))
        })?;
        let mut schema_iter = schema_parts.iter();
        if let Some(start_id_part) = schema_iter.next() {
            if start_id_part.contains(':') && !start_id_part.to_lowercase().contains(":start_id") {
                return Err(load_graph_error(format!(
                    "First column should be ':start_id' in file '{}'",
                    edge_file
                )));
            }
        }
        if let Some(end_id_part) = schema_iter.next() {
            if end_id_part.contains(':') && !end_id_part.to_lowercase().contains(":end_id") {
                return Err(load_graph_error(format!(
                    "Second column should be ':end_id' in file '{}'",
                    edge_file
                )));
            }
        }

        Some(load_schema(schema_iter, edge_file, &mut global_store.key_store, mappings)?)
    } else {
        None
    };

    let timer = GSTimer::now();
    let mut src_empty_count = 0;
    let mut dst_empty_count = 0;
    let mut src_id_missing_count = 0;
    let mut dst_id_missing_count = 0;
    for (index, line) in reader.records().filter_map(Result::ok).enumerate() {
        let mut line_parts = line.iter();

        let from_id_string = line_parts.next().ok_or_else(|| {
            load_graph_error(format!("Could not read from_vertex from file {}", edge_file))
        })?;
        if from_id_string == "" {
            debug!("From id string is empty at line '{}' in file '{}'. Ignoring", index, edge_file);
            src_empty_count += 1;
            continue;
        }
        let from_id = if let Some(from_id) = vertices_map.get(from_id_string) {
            *from_id
        } else if load_graph_ast.only_edge_files {
            add_new_vertex(
                from_id_string.to_string(),
                Properties::default(),
                graph,
                vertices_map,
                total_vertices_count,
            )
        } else {
            src_id_missing_count += 1;
            debug!(
                "Src vertex id '{}' not found for at line '{}' in file '{}'. Adding.",
                from_id_string, index, edge_file
            );
            continue;
        };

        let to_id_string = line_parts.next().ok_or_else(|| {
            load_graph_error(format!("Could not read to_vertex from file {}", edge_file))
        })?;
        if to_id_string == "" {
            debug!("To id string is empty at line '{}' in file '{}'. Ignoring", index, edge_file);
            dst_empty_count += 1;
            continue;
        }
        let to_id = if let Some(to_id) = vertices_map.get(to_id_string) {
            *to_id
        } else if load_graph_ast.only_edge_files {
            add_new_vertex(
                to_id_string.to_string(),
                Properties::default(),
                graph,
                vertices_map,
                total_vertices_count,
            )
        } else {
            dst_id_missing_count += 1;
            debug!(
                "Dst vertex id '{}' not found for at line '{}' in file '{}'. Skipping.",
                to_id_string, index, edge_file
            );
            continue;
        };

        let properties = if let Some(property_types) = &property_types {
            load_property_values(line_parts, from_id_string, edge_file, &property_types)?
        } else {
            Properties::default()
        };

        graph.append_edge(Edge::new(properties, from_id, to_id));
        *total_edges_count += 1;

        if index > 0 && index % 1_000_000 == 0 {
            info!("Processed {} edges in {}", index, timer.elapsed().to_seconds_string());
        }
    }
    if src_empty_count > 0 {
        warn!("Skipped {} edges with empty src ids", src_empty_count);
    }
    if dst_empty_count > 0 {
        warn!("Skipped {} edges with empty dst ids", dst_empty_count);
    }
    if src_id_missing_count > 0 {
        warn!("Skipped {} edges with unmapped src ids to existing vertices", src_id_missing_count);
    }
    if dst_id_missing_count > 0 {
        warn!("Skipped {} edges with unmapped dst ids to existing vertices", dst_id_missing_count);
    }
    Ok(())
}

fn load_schema<'a, 'b>(
    schema_parts: impl Iterator<Item = &'a str>,
    file_path: &str,
    key_store: &mut KeyStore,
    mappings: &'b ClosureMappings,
) -> Result<Vec<(PropertyKeyId, PropertyValueClosureIndex<'b>)>, GraphSurgeError> {
    let mut property_types = Vec::new();
    for schema in schema_parts {
        let mut parts = schema.split(':');

        let column_name = parts.next().unwrap_or_else(|| "");
        if column_name.is_empty() {
            return Err(load_graph_error(format!(
                "Empty column name found in file '{}'",
                file_path
            )));
        }
        let column_type_string = parts.next().unwrap_or_else(|| DEFAULT_TYPE_STRING);
        if column_type_string.is_empty() {
            return Err(load_graph_error(format!(
                "Empty column type found in file '{}'",
                file_path
            )));
        }
        let closure_index = *mappings.get(&column_type_string.to_lowercase()).ok_or_else(|| {
            load_graph_error(format!(
                "Unrecognized column type '{}' in file '{}'",
                column_type_string, file_path,
            ))
        })?;
        property_types.push((key_store.get_key_id_or_insert(column_name), closure_index));
    }
    Ok(property_types)
}

fn load_property_values<'a>(
    line_parts: impl Iterator<Item = &'a str>,
    vertex_id_string: &str,
    file_path: &str,
    property_types: &[(PropertyKeyId, PropertyValueClosureIndex)],
) -> Result<Properties, GraphSurgeError> {
    let mut properties = Properties::default();
    let mut count = 0;
    for (index, property_string) in line_parts.enumerate() {
        count += 1;
        if property_string == "" {
            continue;
        }
        let (property_key_id, property_closure) = property_types.get(index).ok_or_else(|| {
            load_graph_error(format!(
                "No. of columns for vertex id '{}' does not match header in file '{}'",
                vertex_id_string, file_path,
            ))
        })?;
        let property_value = property_closure(property_string, vertex_id_string, file_path)?;
        properties.add_new_property(*property_key_id, property_value);
    }
    if property_types.len() != count {
        return Err(load_graph_error(format!(
            "Total number of columns for vertex id '{}' does not match header in file '{}'",
            vertex_id_string, file_path,
        )));
    }
    Ok(properties)
}

fn get_csv_reader(
    file_path: &str,
    load_graph_ast: &LoadGraphAst,
) -> Result<Reader<BufReader<File>>, GraphSurgeError> {
    Ok(csv::ReaderBuilder::new()
        .has_headers(load_graph_ast.has_headers)
        .delimiter(load_graph_ast.separator.unwrap_or(DEFAULT_SEPARATOR))
        .double_quote(false)
        .comment(load_graph_ast.comment_char)
        .from_reader(get_buf_reader(file_path)?))
}
