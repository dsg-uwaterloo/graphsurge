use crate::filtered_cubes::timestamp::timestamp_mappings::DiffNeighborhood;
use crate::filtered_cubes::timestamp::DimensionId;
use crate::filtered_cubes::FilteredCubeEntriesEdgeId;
use gs_analytics_api::EdgeId;

pub mod edge_diff;
pub mod execute;
mod filter_matrix;
mod matrix_operation;
mod optimal_orders;
mod process_edge_diff;
mod reduce_matrices;
mod tsp;

type Bit = u8;
type FilteredMatrixRow = Vec<Bit>;
type FilteredMatrixStream = (EdgeId, Vec<FilteredMatrixRow>);
type MatrixRow = Vec<usize>;
type Matrix = Vec<MatrixRow>;
type Matrices = Vec<Matrix>;
type DimensionOrders = Vec<DimensionOrder>;
type DimensionOrder = Vec<DimensionId>;
pub struct DiffProcessingData {
    pub timestamp_index: usize,
    pub diff_neighborhood: DiffNeighborhood,
    pub indices: Vec<usize>,
    pub results: FilteredCubeEntriesEdgeId,
}
