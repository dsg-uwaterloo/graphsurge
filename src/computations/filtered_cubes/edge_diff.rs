use crate::computations::filtered_cubes::{DiffProcessingData, FilteredMatrixStream};
use crate::filtered_cubes::timestamp::timestamp_mappings::GSTimestampIndex;
use gs_analytics_api::DiffCount;
use hashbrown::HashMap;
use itertools::Itertools;

#[inline]
pub fn process_edge_diff(
    edge: &FilteredMatrixStream,
    stash: &mut Vec<DiffProcessingData>,
    store_total_data: bool,
) {
    let mut cache = HashMap::new();
    for timestamp_data in stash.iter_mut() {
        let current_value = if timestamp_data
            .indices
            .iter()
            .zip_eq(edge.1.iter())
            .map(|(column_index, row)| row[*column_index])
            .all(|value| value == 1)
        {
            if store_total_data {
                timestamp_data.results.0.push(edge.0);
            }
            1
        } else {
            0
        };

        let sums = {
            let calculate_sum = |data: &[GSTimestampIndex]| {
                data.iter().fold(0, |sum, previous_timestamp_index| {
                    sum + cache.get(previous_timestamp_index).expect("Should be present")
                })
            };
            (
                calculate_sum(&timestamp_data.diff_neighborhood.0), // positive sums.
                calculate_sum(&timestamp_data.diff_neighborhood.1), // negative sums.
            )
        };
        let previous_value: DiffCount = sums.0 - sums.1;
        let new_value = current_value - previous_value;

        if new_value != 0 {
            timestamp_data.results.1.push((edge.0, new_value));
        }
        cache.insert(timestamp_data.timestamp_index, new_value + previous_value);
    }
}

#[cfg(test)]
mod tests {
    use crate::computations::filtered_cubes::edge_diff::process_edge_diff;
    use crate::computations::filtered_cubes::execute::get_results_stash;
    use itertools::Itertools;

    #[test]
    fn test() {
        let edge = (
            100,
            vec![
                vec![0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1],
                vec![0, 1, 1, 0, 1, 1],
                vec![0, 1, 1, 1, 0, 1, 1],
            ],
        );
        let dimension_lengths = vec![10, 5, 6];
        let orders =
            vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5, 6]];
        let mut results_stash = get_results_stash(&orders, &dimension_lengths);

        process_edge_diff(&edge, &mut results_stash, true);
        let diffs = results_stash.into_iter().map(|diff_data| diff_data.results.1).collect_vec();

        // The expected diff vec is mostly empty except for the values indicated in the vec below.
        let mut expected_diffs = (0..300).map(|_| vec![]).collect_vec();
        vec![
            (0, [(100, 1)]),
            (3, [(100, -1)]),
            (4, [(100, 1)]),
            (12, [(100, -1)]),
            (15, [(100, 1)]),
            (16, [(100, -1)]),
            (18, [(100, 1)]),
            (21, [(100, -1)]),
            (22, [(100, 1)]),
            (210, [(100, -1)]),
            (213, [(100, 1)]),
            (214, [(100, -1)]),
            (222, [(100, 1)]),
            (225, [(100, -1)]),
            (226, [(100, 1)]),
            (228, [(100, -1)]),
            (231, [(100, 1)]),
            (232, [(100, -1)]),
            (240, [(100, 1)]),
            (243, [(100, -1)]),
            (244, [(100, 1)]),
            (252, [(100, -1)]),
            (255, [(100, 1)]),
            (256, [(100, -1)]),
            (258, [(100, 1)]),
            (261, [(100, -1)]),
            (262, [(100, 1)]),
        ]
        .into_iter()
        .for_each(|(index, vec)| expected_diffs[index].extend(vec.iter()));

        assert_eq!(diffs, expected_diffs);
    }
}
