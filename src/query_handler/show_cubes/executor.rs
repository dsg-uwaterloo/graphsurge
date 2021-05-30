use crate::error::GSError;
use crate::global_store::GlobalStore;
use crate::query_handler::show_cubes::ShowCollectionsAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;

impl GraphSurgeQuery for ShowCollectionsAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        Ok(GraphSurgeResult::new(global_store.filtered_cube_store.to_string()))
    }
}
