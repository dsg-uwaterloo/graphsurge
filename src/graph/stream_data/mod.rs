use crate::graph::Graph;
use gs_analytics_api::{EdgeId, VertexId};
use log::info;
use std::convert::TryFrom;

pub mod aggregation;
pub mod edge_data;
pub mod filter;
pub mod vertex_data;

pub fn get_timely_vertex_stream(
    graph: &Graph,
    worker_index: usize,
    worker_count: usize,
) -> impl Iterator<Item = VertexId> {
    let (left_index, right_index) =
        get_worker_indices(graph.edges.len(), worker_index, worker_count);
    VertexId::try_from(left_index).expect("Overflow")
        ..VertexId::try_from(right_index).expect("Overflow")
}

pub fn get_timely_edgeid_stream(
    graph: &Graph,
    worker_index: usize,
    worker_count: usize,
) -> impl Iterator<Item = EdgeId> {
    let (left_index, right_index) =
        get_worker_indices(graph.edges.len(), worker_index, worker_count);
    if worker_index == 0 {
        info!(
            "[worker {}] loading {}/{} edges",
            worker_index,
            (right_index - left_index),
            graph.edges.len()
        );
    }
    EdgeId::try_from(left_index).expect("Overflow")
        ..EdgeId::try_from(right_index).expect("Overflow")
}

#[inline]
pub fn get_worker_indices(
    total_len: usize,
    worker_index: usize,
    worker_count: usize,
) -> (usize, usize) {
    let data_per_worker = total_len / worker_count;
    let left_index = data_per_worker * worker_index;
    let right_index = if (worker_index + 1) == worker_count {
        // Handle extra elements.
        total_len
    } else {
        data_per_worker * (worker_index + 1)
    };
    (left_index, right_index)
}
