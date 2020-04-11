use crate::error::GraphSurgeError;
use crate::global_store::GlobalStore;

pub mod create_aggregated_cube;
pub mod create_filtered_cube;
pub mod create_view;
pub mod delete_cubes;
pub mod generate_cube;
pub mod load_cube;
pub mod load_graph;
pub mod run_computation;
pub mod serde;
pub mod set_threads;
pub mod show_computations;
pub mod show_cube_data;
pub mod show_cubes;
pub mod window_cube;
pub mod write_cube;
pub mod write_graph;

pub trait GraphSurgeQuery: std::fmt::Display {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError>;
}

#[derive(new)]
pub struct GraphSurgeResult {
    pub result: String,
}
