use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::DimensionLength;
use crate::filtered_cubes::FilteredCube;
use crate::{create_generic_pointer_with_bounds, create_pointer};
use gs_analytics_api::{DiffCount, SimpleEdge};
use hashbrown::HashMap;
use log::info;
use std::fmt::Display;
use timely::progress::Timestamp;

pub type DiffEdges = Vec<(SimpleEdge, DiffCount)>;
create_pointer!(DiffEdgesPointer, DiffEdges);

pub type DiffStore = HashMap<GSTimestamp, DiffEdgesPointer>;

#[derive(Clone, Debug)]
pub enum CubeDiffIterators<T: Timestamp> {
    Outer(Vec<(T, Box<CubeDiffIterators<T>>)>),
    Inner(Vec<(T, T, DiffEdgesPointer)>),
}
create_generic_pointer_with_bounds!(DiffIteratorPointer, CubeDiffIterators, Timestamp);

impl CubeDiffIterators<GSTimestamp> {
    pub fn new(
        dimension_id: usize,
        mut timestamp: GSTimestamp,
        diff_store: &mut DiffStore,
        dimension_lengths: &[DimensionLength],
    ) -> Self {
        let total_dimensions = dimension_lengths.len();
        let dimension_length = dimension_lengths[dimension_id];
        if (dimension_id + 1) == total_dimensions {
            CubeDiffIterators::Inner(
                (0..dimension_length)
                    .map(|value| {
                        let mut current_timestamp = timestamp;
                        current_timestamp.set_value_at(dimension_id, total_dimensions, value);

                        let mut next_timestamp = timestamp;
                        next_timestamp.set_value_at(
                            dimension_id,
                            total_dimensions,
                            value.checked_add(1).expect("Dimension overflow"),
                        );
                        let diffs = diff_store.remove(&current_timestamp).expect("Value not found");
                        (current_timestamp, next_timestamp, diffs)
                    })
                    .collect(),
            )
        } else {
            CubeDiffIterators::Outer(
                (0..dimension_length)
                    .map(|value| {
                        timestamp.set_value_at(dimension_id, total_dimensions, value);
                        let next = CubeDiffIterators::new(
                            dimension_id + 1,
                            timestamp,
                            diff_store,
                            dimension_lengths,
                        );
                        timestamp.set_value_at(dimension_id, total_dimensions, value + 1);
                        (timestamp, Box::new(next))
                    })
                    .collect(),
            )
        }
    }
}

impl<T: Timestamp + Display> std::fmt::Display for CubeDiffIterators<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                CubeDiffIterators::Outer(outer) => outer
                    .iter()
                    .map(|(_, inner)| inner.to_string())
                    .collect::<Vec<String>>()
                    .join("\n"),
                CubeDiffIterators::Inner(inner) => inner
                    .iter()
                    .map(|(timestamp, _, data)| format!(
                        "{}:\n\ttotal: {}, sample: {}",
                        timestamp,
                        data.len(),
                        data.iter()
                            .take(5)
                            .map(|v| format!("{:?}", v))
                            .collect::<Vec<String>>()
                            .join(",")
                    ))
                    .collect::<Vec<String>>()
                    .join("\n"),
            }
        )
    }
}

pub struct DifferentialData {
    pub cube_diff_iterators: CubeDiffIterators<GSTimestamp>,
    pub dimensions_count: usize,
}

pub fn get_differential_data(cube: &FilteredCube) -> DifferentialData {
    let mut diff_data: DiffStore = HashMap::new();
    for (_, timestamp, (_, diff_edges), _) in &cube.data.entries {
        info!("{}: total {}", timestamp, diff_edges.len());
        diff_data.insert(*timestamp, DiffEdgesPointer::new(&diff_edges));
    }
    let dimensions_count = cube.dimension_lengths.len();
    let cube_diff_iterators = CubeDiffIterators::new(
        0,
        GSTimestamp::get_zeroth_timestamp(),
        &mut diff_data,
        &cube.dimension_lengths,
    );
    DifferentialData { cube_diff_iterators, dimensions_count }
}
