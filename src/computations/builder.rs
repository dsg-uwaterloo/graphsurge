use crate::computations::pagerank::PageRank;
use crate::computations::{bfs::BFS, scc::SCC, spsp::SPSP, wcc::WCC};
use crate::computations::{Computation, ComputationProperties, ComputationRuntimeData};
use crate::error::GraphSurgeError;
use crate::filtered_cubes::FilteredCube;
use crate::GraphSurgeResult;
use hashbrown::hash_map::DefaultHashBuilder;
use hashbrown::HashMap;

pub trait ComputationBuilder {
    fn execute(
        &self,
        properties: &HashMap<String, ComputationProperties>,
        cube: &mut FilteredCube,
        runtime_data: ComputationRuntimeData,
    ) -> Result<GraphSurgeResult, GraphSurgeError>;
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
            ) -> Result<GraphSurgeResult, GraphSurgeError> {
                $name::instance(properties)?.execute(cube, runtime_data)
            }
        }
    };
}

create_builder!(BFSBuilder, BFS);
create_builder!(WCCOptBuilder, WCC);
create_builder!(SCCBuilder, SCC);
create_builder!(SPSPBuilder, SPSP);
create_builder!(PageRankBuilder, PageRank);

pub fn initialize_computations(
    computations: &mut HashMap<String, Box<dyn ComputationBuilder>, DefaultHashBuilder>,
) {
    computations.insert(String::from("bfs"), Box::new(BFSBuilder));
    computations.insert(String::from("wcc"), Box::new(WCCOptBuilder));
    computations.insert(String::from("scc"), Box::new(SCCBuilder));
    computations.insert(String::from("spsp"), Box::new(SPSPBuilder));
    computations.insert(String::from("pr"), Box::new(PageRankBuilder));
}
