use crate::computations::views::execute::execute;
use crate::computations::views::{EdgeViewOutput, ExecutionType, VertexViewOutput};
use crate::error::GraphSurgeError;
use crate::global_store::GlobalStore;
use crate::query_handler::create_view::CreateViewAst;
use crate::query_handler::{GraphSurgeQuery, GraphSurgeResult};
use crate::util::io::GSWriter;
use itertools::Itertools;
use log::info;

impl GraphSurgeQuery for CreateViewAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        let (vertices, edges, _) = execute(self.clone(), global_store, ExecutionType::SingleView)?;

        if let Some(save_path) = &self.save_to {
            info!("Writing view to '{}'...", save_path);
            let dot_path = format!("{}/{}-view.dot", save_path, self.name);
            let mut writer = GSWriter::new(dot_path)?;
            writer.write_file_line("digraph G {")?;
            let vertex_iter = vertices.iter().map(|(vertex_id, properties, property_strings)| {
                format!(
                    "v{} [label=\"{},{}\"];",
                    vertex_id,
                    properties.iter().map(|(key, value)| format!("{}={}", key, value)).join(","),
                    property_strings.iter().join(",")
                )
            });
            writer.write_file_lines(vertex_iter)?;
            let edge_iter = edges.iter().map(|((src, dst), section_properties)| {
                format!(
                    "v{} -> v{} [label=\"{}\"];",
                    src,
                    dst,
                    section_properties
                        .iter()
                        .map(|(sid, properties)| {
                            format!(
                                "sec={},{}",
                                sid,
                                properties
                                    .iter()
                                    .map(|(key, value)| format!("{}={}", key, value))
                                    .join(",")
                            )
                        })
                        .join(","),
                )
            });
            writer.write_file_lines(edge_iter)?;
            writer.write_file_line("}")?;
        }

        Ok(GraphSurgeResult::new(fmt_graph(&vertices, &edges)))
    }
}

pub(in crate::query_handler) fn fmt_graph(
    vertices: &[VertexViewOutput],
    edges: &[EdgeViewOutput],
) -> String {
    format!(
        "Vertices:\n{}\nEdges:\n{}",
        vertices
            .iter()
            .map(|(id, properties, group)| {
                format!(
                    "{} {{{}}}, [{}]",
                    id,
                    properties
                        .iter()
                        .map(|(key, value)| format!("{} = {}", key, value))
                        .collect::<Vec<_>>()
                        .join(", "),
                    group.join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        edges
            .iter()
            .map(|(srcdst, sec_properties)| {
                format!(
                    "{:?} [{}]",
                    srcdst,
                    sec_properties
                        .iter()
                        .map(|(si, properties)| format!(
                            "{{section = {}, {}}}",
                            si,
                            properties
                                .iter()
                                .map(|(key, value)| format!("{} = {}", key, value))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}
