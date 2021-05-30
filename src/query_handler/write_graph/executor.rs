use crate::error::GSError;
use crate::global_store::GlobalStore;
use crate::query_handler::write_graph::WriteGraphAst;
use crate::query_handler::GraphSurgeQuery;
use crate::util::io::GsWriter;
use crate::GraphSurgeResult;

impl GraphSurgeQuery for WriteGraphAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        let vertex_file_path = format!("{}/vertices.txt", self.dir);
        let mut vertex_file_buffer = GsWriter::new(vertex_file_path)?;
        for (vertex_id, _) in global_store.graph.vertices().iter().enumerate() {
            vertex_file_buffer.write_file_line(&format!("{}", vertex_id,))?;
        }

        let edge_file_path = format!("{}/edges.txt", self.dir);
        let mut edge_file_buffer = GsWriter::new(edge_file_path)?;
        for edge in global_store.graph.edges() {
            edge_file_buffer
                .write_file_line(&format!("{},{}", edge.src_vertex_id, edge.dst_vertex_id,))?;
        }
        Ok(GraphSurgeResult::new(format!("Graph written to '{}'", self.dir)))
    }
}
