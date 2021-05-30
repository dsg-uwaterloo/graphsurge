use crate::computations::filtered_cubes::execute::execute;
use crate::error::GSError;
use crate::filtered_cubes::timestamp::timestamp_mappings::get_timestamp_mappings;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::{DimensionLength, DimensionLengths, FilteredCube};
use crate::global_store::GlobalStore;
use crate::query_handler::create_filtered_cube::CreateViewCollectionAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;
use gs_analytics_api::FilteredCubeData;
use log::info;
use std::convert::TryFrom;

impl GraphSurgeQuery for CreateViewCollectionAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        if global_store.filtered_cube_store.cubes.contains_key(&self.name) {
            return Err(GSError::CollectionAlreadyExists(self.name.clone()));
        }

        let dimension_lengths: DimensionLengths = self
            .dimensions
            .iter()
            .map(|vec| DimensionLength::try_from(vec.len()).expect("DimensionLength overflow"))
            .collect();
        let timestamp_mappings = get_timestamp_mappings(&dimension_lengths);

        info!("Dimension lengths: {:?}", dimension_lengths);

        let data = execute(
            self.dimensions.clone(),
            dimension_lengths.clone(),
            &global_store.graph,
            self.manual_order,
            self.store_total_data,
            &timestamp_mappings,
            global_store.threads.get(),
            global_store.process_id,
            &self.hosts,
        )?;
        let filtered_cube_data = FilteredCubeData::new(data);

        let mut cube =
            FilteredCube::new(timestamp_mappings, dimension_lengths, None, filtered_cube_data);
        print_totals(&cube);

        if self.materialized {
            cube.prepare_differential_data();
        }

        global_store.filtered_cube_store.cubes.insert(self.name.clone(), cube);
        Ok(GraphSurgeResult::new(format!("Cube '{}' created successfully", self.name)))
    }
}

pub fn print_totals(cube: &FilteredCube) {
    let mut total_data_count = 0;
    let mut total_diff_count = 0;
    let mut add_diff_count = 0;
    let mut del_diff_count = 0;
    let mut partial_diff_count = 0;
    let zeroth_timestamp = GSTimestamp::get_zeroth_timestamp();
    for (_, timestamp, (full_edges, diff_edges), (adds, dels)) in &cube.data.entries {
        info!("{}: ({}, {} + {} = {})", timestamp, full_edges.len(), adds, dels, diff_edges.len());
        total_data_count += full_edges.len();
        total_diff_count += diff_edges.len();
        add_diff_count += adds;
        del_diff_count += dels;
        assert_eq!(
            diff_edges.len(),
            adds + dels,
            "Stored counts ({}+{}={}) do not match actual count '{}'",
            adds,
            dels,
            adds + dels,
            diff_edges.len()
        );
        if *timestamp != zeroth_timestamp {
            partial_diff_count += diff_edges.len();
        }
    }
    info!("Total data = {}", total_data_count);
    info!("Total diffs = {} + {} = {}", add_diff_count, del_diff_count, total_diff_count);
    info!("Partial diffs = {}", partial_diff_count);
}

#[cfg(test)]
mod tests {
    use crate::filtered_cubes::timestamp::GSTimestamp;
    use crate::global_store::GlobalStore;
    use crate::process_query;

    #[test]
    fn test_filtered_matrix() {
        let mut global_store = GlobalStore::default();

        let mut graph_query = "
            load graph with vertices from 'data/small_mutiple_types/vertices.txt'
            and edges from 'data/small_mutiple_types/edges.txt'
            comment '#';"
            .to_owned();
        process_query(&mut global_store, &mut graph_query).expect("Graph not loaded");

        let mut cube_query = "
            create view collection my_cube
            where [u.type = 'v1'],[u.type = 'v2'],[type = 'e1']
            manually_ordered
            materialize_full_view;"
            .to_owned();
        process_query(&mut global_store, &mut cube_query).expect("Cube not created");

        let created_cube =
            global_store.filtered_cube_store.cubes.get("my_cube").expect("Cube not found");

        let expected_data = vec![
            (
                0,
                GSTimestamp::new(&[0]),
                (
                    vec![(0, 5), (1, 6), (1, 7), (2, 8)],
                    vec![((0, 5), 1), ((1, 6), 1), ((1, 7), 1), ((2, 8), 1)],
                ),
                (4, 0),
            ),
            (
                1,
                GSTimestamp::new(&[1]),
                (
                    vec![(9, 0), (8, 2), (4, 3)],
                    vec![
                        ((0, 5), -1),
                        ((1, 6), -1),
                        ((1, 7), -1),
                        ((2, 8), -1),
                        ((9, 0), 1),
                        ((8, 2), 1),
                        ((4, 3), 1),
                    ],
                ),
                (3, 4),
            ),
            (
                2,
                GSTimestamp::new(&[2]),
                (
                    vec![(0, 5), (1, 6), (1, 7), (2, 8)],
                    vec![
                        ((0, 5), 1),
                        ((1, 6), 1),
                        ((1, 7), 1),
                        ((2, 8), 1),
                        ((9, 0), -1),
                        ((8, 2), -1),
                        ((4, 3), -1),
                    ],
                ),
                (4, 3),
            ),
        ];

        assert_eq!(created_cube.data.entries, expected_data);
    }
}
