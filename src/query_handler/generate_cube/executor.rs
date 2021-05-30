use crate::error::GSError;
use crate::filtered_cubes::timestamp::timestamp_mappings::get_timestamp_mappings;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::{DimensionLength, FilteredCube};
use crate::global_store::GlobalStore;
use crate::query_handler::create_filtered_cube::executor::print_totals;
use crate::query_handler::generate_cube::GenerateCubeAst;
use crate::query_handler::GraphSurgeQuery;
use crate::GraphSurgeResult;
use graph_map::GraphMMap;
use gs_analytics_api::{FilteredCubeData, SimpleEdge, VertexId};
use itertools::Itertools;
use log::info;
use rand::prelude::SliceRandom;
use rand::Rng;
use std::convert::TryFrom;

impl GraphSurgeQuery for GenerateCubeAst {
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
        let initial_edges = edges[..self.first_view].iter().map(|edge| (*edge, 1)).collect_vec();
        let inital_len = initial_edges.len();
        cube_data.push((0, GSTimestamp::new(&[0]), (Vec::new(), initial_edges), (inital_len, 0)));

        for index in 0..self.batch_count {
            info!("Processing batch {} of {}", index + 1, self.batch_count);

            let next_start_index = self.first_view + self.adds * index;
            let next_end_index = self.first_view + self.adds * (index + 1);
            if next_start_index >= edges.len() || next_end_index > edges.len() {
                info!("Edges exhausted for addition {}-{}!", next_start_index, next_end_index,);
                std::process::exit(1);
            }
            let mut data =
                edges[next_start_index..next_end_index].iter().map(|edge| (*edge, 1)).collect_vec();
            let adds_len = data.len();
            info!(
                "Generated {} additions ({}-{}) for 0,0,{}",
                adds_len,
                next_start_index,
                next_end_index,
                index + 1
            );

            let next_start_index = self.dels * index;
            let next_end_index = self.first_view + self.adds * index;
            if next_start_index >= edges.len() || next_end_index > edges.len() {
                info!("Edges exhausted for deletion {}-{}!", next_start_index, next_end_index,);
                std::process::exit(1);
            }
            let (deletions, _) =
                partial_shuffle(&mut edges[next_start_index..next_end_index], &mut rng, self.dels);
            let dels_len = deletions.len();
            info!(
                "Generated {} deletions ({}-{}) for 0,0,{}",
                dels_len,
                next_start_index,
                next_end_index,
                index + 1
            );
            data.extend(deletions.iter().map(|edge| (*edge, -1)));

            cube_data.push((
                index + 1,
                GSTimestamp::new(&[DimensionLength::try_from(index + 1).expect("Overflow")]),
                (Vec::new(), data),
                (adds_len, dels_len),
            ));
        }

        let dimension_lengths =
            vec![DimensionLength::try_from(self.batch_count + 1).expect("Overflow")];
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

pub fn partial_shuffle<'a, R>(
    slice: &'a mut [SimpleEdge],
    rng: &mut R,
    amount: usize,
) -> (&'a mut [SimpleEdge], &'a mut [SimpleEdge])
where
    R: Rng + ?Sized,
{
    // This applies Durstenfeld's algorithm for the
    // [Fisher–Yates shuffle](https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm)
    // for an unbiased permutation, but exits early after choosing `amount`
    // elements.

    let len = slice.len();
    let end = if amount >= len { len } else { amount };

    for i in 0..end {
        // invariant: elements with index > i have been locked in place.
        slice.swap(i, rng.gen_range(i..len));
    }
    slice.split_at_mut(end)
}
