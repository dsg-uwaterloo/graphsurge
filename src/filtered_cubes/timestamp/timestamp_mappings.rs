use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::DimensionLength;
use gs_analytics_api::GsTimestampIndex;
use hashbrown::HashMap;
use itertools::Itertools;

pub type AddGSTimestampIndex = GsTimestampIndex;
pub type SubtractGSTimestampIndex = GsTimestampIndex;
pub type DiffNeighborhood = (Vec<AddGSTimestampIndex>, Vec<SubtractGSTimestampIndex>);
pub type TimestampToIndexMap = HashMap<GSTimestamp, GsTimestampIndex>;
pub type TimestampMappings = (Vec<(DiffNeighborhood, GSTimestamp)>, TimestampToIndexMap);

pub fn get_timestamp_mappings(dimension_lengths: &[DimensionLength]) -> TimestampMappings {
    let mut timestamp_mappings = TimestampMappings::default();
    for timestamp in GSTimestamp::all_timestamps(dimension_lengths) {
        let timestamp_index = timestamp_mappings.0.len();
        timestamp_mappings.1.insert(timestamp, timestamp_index);
        let neighborhood = timestamp.get_diff_neighborhood();
        let mapped_neighborhood = (
            map_timestamp_to_index(&neighborhood.0, &timestamp_mappings.1),
            map_timestamp_to_index(&neighborhood.1, &timestamp_mappings.1),
        );
        timestamp_mappings.0.push((mapped_neighborhood, timestamp));
    }
    timestamp_mappings
}

fn map_timestamp_to_index(
    timestamps: &[GSTimestamp],
    index_map: &TimestampToIndexMap,
) -> Vec<GsTimestampIndex> {
    timestamps
        .iter()
        .map(|ts| index_map.get(ts).copied().expect("Timestamp index map should exist."))
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use crate::filtered_cubes::timestamp::timestamp_mappings::{
        get_timestamp_mappings, DiffNeighborhood,
    };
    use crate::filtered_cubes::DimensionLength;
    use itertools::Itertools;

    #[test]
    fn test1() {
        let dimension_lengths = vec![5];
        let expected_data = vec![
            (vec![], vec![]),
            (vec![0], vec![]),
            (vec![1], vec![]),
            (vec![2], vec![]),
            (vec![3], vec![]),
        ]
        .into_iter()
        .collect_vec();
        assert_mappings(&dimension_lengths, &expected_data);
    }

    #[cfg(feature = "nd-timestamps")]
    #[test]
    fn test2() {
        let dimension_lengths = vec![3, 3];
        let expected_data = vec![
            (vec![], vec![]),
            (vec![0], vec![]),
            (vec![1], vec![]),
            (vec![0], vec![]),
            (vec![3, 1], vec![0]),
            (vec![4, 2], vec![1]),
            (vec![3], vec![]),
            (vec![6, 4], vec![3]),
            (vec![7, 5], vec![4]),
        ]
        .into_iter()
        .collect_vec();
        assert_mappings(&dimension_lengths, &expected_data);
    }

    fn assert_mappings(
        dimension_lengths: &[DimensionLength],
        expected_mapping: &[DiffNeighborhood],
    ) {
        let mappings = get_timestamp_mappings(&dimension_lengths);
        assert_eq!(
            mappings.0.into_iter().map(|(diffs, _)| diffs).collect_vec().as_slice(),
            expected_mapping
        );
    }
}
