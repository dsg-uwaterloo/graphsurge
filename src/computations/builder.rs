use crate::computations::pagerank::PageRank;
use crate::computations::{bfs::Bfs, scc::Scc, spsp::Spsp, sssp::Sssp, wcc::Wcc};
use crate::computations::{Computation, ComputationProperties};
use crate::error::GSError;
use crate::filtered_cubes::FilteredCube;
use crate::GraphSurgeResult;
use gs_analytics_api::ComputationRuntimeData;
use hashbrown::hash_map::DefaultHashBuilder;
use hashbrown::HashMap;

pub trait ComputationBuilder {
    fn execute(
        &self,
        properties: &HashMap<String, ComputationProperties>,
        cube: &mut FilteredCube,
        runtime_data: ComputationRuntimeData,
    ) -> Result<GraphSurgeResult, GSError>;
}

macro_rules! create_builder {
    ($builder:ident, $name:ident) => {
        pub struct $builder;

        impl ComputationBuilder for $builder {
            fn execute(
                &self,
                properties: &HashMap<String, ComputationProperties>,
                cube: &mut FilteredCube,
                runtime_data: ComputationRuntimeData,
            ) -> Result<GraphSurgeResult, GSError> {
                $name::instance(properties)?.execute(cube, runtime_data)
            }
        }
    };
}

create_builder!(BfsBuilder, Bfs);
create_builder!(SsspBuilder, Sssp);
create_builder!(WccOptBuilder, Wcc);
create_builder!(SccBuilder, Scc);
create_builder!(SpspBuilder, Spsp);
create_builder!(PageRankBuilder, PageRank);

pub fn initialize_computations(
    computations: &mut HashMap<String, Box<dyn ComputationBuilder>, DefaultHashBuilder>,
) {
    computations.insert(String::from("bfs"), Box::new(BfsBuilder));
    computations.insert(String::from("sssp"), Box::new(SsspBuilder));
    computations.insert(String::from("wcc"), Box::new(WccOptBuilder));
    computations.insert(String::from("scc"), Box::new(SccBuilder));
    computations.insert(String::from("mpsp"), Box::new(SpspBuilder));
    computations.insert(String::from("pr"), Box::new(PageRankBuilder));
}
