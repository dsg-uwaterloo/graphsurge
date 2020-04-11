use crate::error::key_error;
use crate::error::GraphSurgeError;
use crate::filtered_cubes::materialise::get_differential_data;
use crate::global_store::GlobalStore;
use crate::query_handler::show_cube_data::ShowCollectionDataAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;
use log::info;

impl GraphSurgeQuery for ShowCollectionDataAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        let cube =
            global_store.filtered_cube_store.cubes.get(&self.name).ok_or_else(|| {
                key_error(format!("Cube '{}' has not been created yet", self.name))
            })?;
        info!("Full data: \n{}", cube.get_full_data_string());
        let diff_data = get_differential_data(&cube);
        info!("Diff data: \n{}", diff_data.cube_diff_iterators);
        Ok(GraphSurgeResult::new("done".to_string()))
    }
}
