//use crate::computations::CType;
//use crate::computations::ComputationProperties;
use crate::error::GraphSurgeError;
use crate::error::{computation_error, key_error};
use crate::filtered_cubes::FilteredCube;
use crate::global_store::GlobalStore;
//use crate::graph::Graph;
use crate::query_handler::run_computation::RunComputationAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;
//use hashbrown::HashMap;
use crate::computations::ComputationRuntimeData;
use log::info;

impl GraphSurgeQuery for RunComputationAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        let cube: &mut FilteredCube =
            global_store.filtered_cube_store.cubes.get_mut(&self.cube).ok_or_else(|| {
                key_error(format!("Cube '{}' has not been created yet", self.cube))
            })?;

        if let Some(_file) = &self.file {
            todo!()
        } else {
            info!("Running static computation '{}'", self.computation);
            let computation =
                global_store.computations.get(&self.computation).ok_or_else(|| {
                    computation_error(format!(
                        "Computation '{}' is not defined yet",
                        self.computation
                    ))
                })?;

            computation.execute(
                &self.properties,
                cube,
                ComputationRuntimeData::new(
                    &global_store.graph,
                    self.c_type,
                    self.materialize_results,
                    &self.save_to,
                    global_store.threads.get(),
                    global_store.process_id,
                    &self.hosts,
                    &self.splits,
                    self.batch_size,
                ),
            )
        }
    }
}
