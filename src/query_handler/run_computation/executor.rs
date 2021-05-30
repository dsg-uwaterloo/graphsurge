//use crate::computations::CType;
//use crate::computations::ComputationProperties;
use crate::error::GSError;
use crate::filtered_cubes::FilteredCube;
use crate::global_store::GlobalStore;
//use crate::graph::Graph;
use crate::query_handler::run_computation::RunComputationAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;
//use hashbrown::HashMap;
use gs_analytics_api::ComputationRuntimeData;
use log::info;

impl GraphSurgeQuery for RunComputationAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        let cube: &mut FilteredCube = global_store
            .filtered_cube_store
            .cubes
            .get_mut(&self.cube)
            .ok_or_else(|| GSError::CollectionMissing(self.cube.clone()))?;

        if let Some(_file) = &self.file {
            Err(GSError::Generic("TODO".to_owned()))
        } else {
            info!("Running static computation '{}'", self.computation);
            let computation =
                global_store.computations.get(&self.computation).ok_or_else(|| {
                    GSError::Computation(format!(
                        "Computation '{}' is not defined yet",
                        self.computation
                    ))
                })?;

            computation.execute(
                &self.properties,
                cube,
                ComputationRuntimeData::new(
                    self.c_type,
                    global_store.graph.vertex_count(),
                    self.materialize_results,
                    self.save_to.clone(),
                    global_store.threads.get(),
                    global_store.process_id,
                    self.hosts.clone(),
                    self.splits.clone(),
                    self.batch_size,
                    self.comp_multipler,
                    self.diff_multipler,
                    self.limit,
                    self.use_lr,
                ),
            )
        }
    }
}
