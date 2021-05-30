use crate::error::GSError;
use crate::global_store::GlobalStore;
use crate::query_handler::delete_cubes::DeleteCollectionsAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;

impl GraphSurgeQuery for DeleteCollectionsAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        global_store.filtered_cube_store.delete_all();
        Ok(GraphSurgeResult::new("Deleted all cubes".to_owned()))
    }
}
