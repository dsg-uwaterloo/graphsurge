use crate::error::GSError;
use crate::filtered_cubes::timestamp::timestamp_mappings::get_timestamp_mappings;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::{DimensionLength, FilteredCube};
use crate::global_store::GlobalStore;
use crate::query_handler::create_filtered_cube::executor::print_totals;
use crate::query_handler::generate_cube::executor::partial_shuffle;
use crate::query_handler::window_cube::WindowCubeAst;
use crate::query_handler::GraphSurgeQuery;
use crate::GraphSurgeResult;
use graph_map::GraphMMap;
use gs_analytics_api::{FilteredCubeData, VertexId};
use itertools::Itertools;
use log::info;
use rand::prelude::SliceRandom;
use std::convert::TryFrom;

impl GraphSurgeQuery for WindowCubeAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        if global_store.filtered_cube_store.cubes.contains_key(&self.name) {
            return Err(GSError::CollectionAlreadyExists(self.name.clone()));
        }

        info!("Generating '{}'", self.name);
        let mut cube_data = Vec::new();

        let graph = GraphMMap::new(&self.graph_filename);
        let mut edges = (0..graph.nodes())
            .flat_map(|node| {
                graph
                    .edges(node)
                    .iter()
                    .map(move |neighbor| (VertexId::try_from(node).expect("Overflow"), *neighbor))
            })
            .collect::<Vec<_>>();
        info!("Loaded {} edges", edges.len(),);
        let mut rng = &mut rand::thread_rng();
        info!("Shuffling...");
        edges.shuffle(&mut rng);

        let mut ts = 0;
        let mut add_start_index;
        let mut add_end_index = 0;
        let mut del_start_index = 0;
        let mut del_end_index = self.first_view;
        for indexi in 0..self.total_batch_count {
            for indexj in 0..self.diff_batch_count {
                let adds = if indexi == 0 && indexj == 0 {
                    self.first_view
                } else if indexj == 0 {
                    self.large_diff / 2
                } else {
                    self.small_diffs / 2
                };
                let dels = if indexi == 0 && indexj == 0 { 0 } else { adds };
                add_start_index = add_end_index;
                add_end_index += adds;
                info!(
                    "Processing batch {} of {}, {} of {}",
                    indexi + 1,
                    self.total_batch_count,
                    indexj + 1,
                    self.diff_batch_count
                );
                if add_start_index >= edges.len() || add_end_index > edges.len() {
                    info!("Edges exhausted for addition {}-{}!", add_start_index, add_end_index);
                    std::process::exit(1);
                }
                let mut data = edges[add_start_index..add_end_index]
                    .iter()
                    .map(|edge| (*edge, 1))
                    .collect_vec();
                let adds_len = data.len();
                info!(
                    "Generated {} additions ({}-{}) for 0,0,{}",
                    adds_len, add_start_index, add_end_index, ts
                );

                if del_start_index >= edges.len() || del_end_index > edges.len() {
                    info!("Edges exhausted for deletion {}-{}!", del_start_index, del_end_index);
                    std::process::exit(1);
                }
                let (deletions, _) =
                    partial_shuffle(&mut edges[del_start_index..del_end_index], &mut rng, dels);
                let dels_len = deletions.len();
                info!(
                    "Generated {} deletions ({}-{}) for 0,0,{}",
                    dels_len, del_start_index, del_end_index, ts
                );
                data.extend(deletions.iter().map(|edge| (*edge, -1)));

                cube_data.push((
                    ts,
                    GSTimestamp::new(&[DimensionLength::try_from(ts).expect("Overflow")]),
                    (Vec::new(), data),
                    (adds_len, dels_len),
                ));
                ts += 1;
                del_start_index += dels;
                del_end_index = add_end_index;
            }
        }

        let dimension_lengths = vec![DimensionLength::try_from(ts).expect("Overflow")];
        let timestamp_mappings = get_timestamp_mappings(&dimension_lengths);
        let cube = FilteredCube::new(
            timestamp_mappings,
            dimension_lengths,
            None,
            FilteredCubeData::new(cube_data),
        );
        print_totals(&cube);

        global_store.filtered_cube_store.cubes.insert(self.name.clone(), cube);
        Ok(GraphSurgeResult::new(format!("Cube '{}' loaded successfully", self.name)))
    }
}
