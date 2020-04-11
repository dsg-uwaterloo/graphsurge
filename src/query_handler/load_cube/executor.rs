use crate::error::{create_cube_error, GraphSurgeError};
use crate::filtered_cubes::timestamp::timestamp_mappings::get_timestamp_mappings;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::{
    CubeDataEntries, DimensionLength, DimensionLengths, FilteredCube, FilteredCubeData,
};
use crate::global_store::GlobalStore;
use crate::query_handler::create_filtered_cube::executor::print_totals;
use crate::query_handler::load_cube::LoadCubeAst;
use crate::query_handler::GraphSurgeQuery;
use crate::GraphSurgeResult;
use graph_map::GraphMMap;
use gs_analytics_api::{DiffCount, SimpleEdge, VertexId};
use itertools::Itertools;
use log::info;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufRead, BufReader, Error};
use std::path::Path;
use timely::PartialOrder;

const DEFAULT_SEPARATOR: char = ',';

impl GraphSurgeQuery for LoadCubeAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        if global_store.filtered_cube_store.cubes.contains_key(&self.name) {
            return Err(create_cube_error(format!(
                "Cube name '{}' already exists in store",
                self.name
            )));
        }

        info!("Loading data from {}", self.dir);

        if self.threads.is_some() {
            info!("Ignoring threads...")
        }

        let dimension_lengths: DimensionLengths = vec![self.m, self.n]
            .into_iter()
            .map(|length| DimensionLength::try_from(length).expect("DimensionLength overflow"))
            .collect();
        let mut filtered_cube_data: Vec<CubeDataEntries<GSTimestamp>> = Vec::new();
        let timestamp_mappings = get_timestamp_mappings(&dimension_lengths);

        for (ts_index, (_, timestamp)) in timestamp_mappings.0.iter().enumerate() {
            let i = timestamp.get_value_at(0, 2);
            let j = timestamp.get_value_at(1, 2);

            let gmap_path = format!("{}/{}{}_{}.gmap", self.dir, self.prefix, i, j);
            let (batch, adds, dels) = if Path::new(&format!("{}.offsets", gmap_path)).exists() {
                let graph = GraphMMap::new(&gmap_path);
                let batch = (0..graph.nodes())
                    .flat_map(|node| {
                        graph.edges(node).iter().map(move |&neighbor| {
                            (
                                (
                                    VertexId::try_from(node).expect("overflow"),
                                    VertexId::try_from(neighbor).expect("overflow"),
                                ),
                                1,
                            )
                        })
                    })
                    .collect_vec();
                let len = batch.len();
                (batch, len, 0)
            } else {
                let reader = BufReader::new(
                    File::open(format!("{}/{}{}_{}.txt", self.dir, self.prefix, i, j))
                        .expect("IO error"),
                );
                let mut adds = 0;
                let mut dels = 0;
                let batch = reader
                    .lines()
                    .flat_map(|line| {
                        let process_line = |line: Result<String, Error>| {
                            if let Ok(line) = line {
                                if let Some(comment_char) = &self.comment_char {
                                    if line.starts_with(*comment_char) {
                                        return None;
                                    }
                                }
                                let mut split =
                                    line.split(self.separator.unwrap_or(DEFAULT_SEPARATOR));
                                let u: VertexId = split
                                    .next()
                                    .expect("u not found")
                                    .parse()
                                    .expect("u not parsed");
                                let v: VertexId = split
                                    .next()
                                    .expect("v not found")
                                    .parse()
                                    .expect("v not parsed");
                                let diff: DiffCount = split
                                    .next()
                                    .expect("diff not found")
                                    .parse()
                                    .expect("diff not parsed");
                                if diff != 0 {
                                    return Some(((u, v), diff));
                                }
                            }

                            None
                        };
                        let result = process_line(line);
                        if let Some((_, change)) = &result {
                            if *change > 0 {
                                adds += 1;
                            } else {
                                dels += 1;
                            }
                        }
                        result
                    })
                    .collect_vec();
                (batch, adds, dels)
            };
            info!("Loaded {} updates at {}", batch.len(), timestamp);
            let full = if self.with_full {
                let mut previous = Vec::new();
                for (_, ts, (_, diff_data), _) in &filtered_cube_data {
                    if ts.less_than(timestamp) {
                        previous.push(diff_data);
                    }
                }
                batch
                    .iter()
                    .copied()
                    .merge(previous.into_iter().flat_map(|v| v.iter().copied()))
                    .into_group_map()
                    .into_iter()
                    .filter_map(|(e, diffs)| {
                        let sum: isize = diffs.iter().sum();
                        if sum == 0 {
                            None
                        } else if sum == 1 {
                            Some(e)
                        } else {
                            panic!("Should not be {:?}, {}", e, sum)
                        }
                    })
                    .collect()
            } else {
                Vec::<SimpleEdge>::new()
            };

            filtered_cube_data.push((ts_index, *timestamp, (full, batch), (adds, dels)));
        }
        info!("Total updates = {}", filtered_cube_data.len());
        let cube = FilteredCube::new(
            timestamp_mappings,
            dimension_lengths,
            None,
            FilteredCubeData::new(filtered_cube_data),
        );
        print_totals(&cube);

        global_store.filtered_cube_store.cubes.insert(self.name.clone(), cube);
        Ok(GraphSurgeResult::new(format!("Cube '{}' loaded successfully", self.name)))
    }
}

#[cfg(test)]
mod tests {
    use crate::filtered_cubes::timestamp::GSTimestamp;
    use crate::filtered_cubes::CubeDataEntries;
    use crate::global_store::GlobalStore;
    use crate::process_query;

    #[test]
    fn test_2d() {
        assert(
            2,
            2,
            "data/test_data/bfs/2d_small",
            "batch-0_",
            &[
                (
                    0,
                    GSTimestamp::new(&[0, 0]),
                    (
                        vec![],
                        vec![
                            ((6, 4), 1),
                            ((4, 5), 1),
                            ((4, 2), 1),
                            ((5, 2), 1),
                            ((2, 3), 1),
                            ((5, 9), 1),
                            ((3, 9), 1),
                            ((9, 8), 1),
                        ],
                    ),
                    (8, 0),
                ),
                (1, GSTimestamp::new(&[0, 1]), (vec![], vec![((6, 5), 1), ((5, 9), -1)]), (1, 1)),
                (2, GSTimestamp::new(&[1, 0]), (vec![], vec![((2, 9), 1), ((4, 5), -1)]), (1, 1)),
                (3, GSTimestamp::new(&[1, 1]), (vec![], vec![((9, 3), 1), ((2, 3), -1)]), (1, 1)),
            ],
        );
    }

    #[test]
    fn test_1d() {
        assert(
            1,
            4,
            "data/test_data/bfs/1d_small",
            "batch-0_",
            &[
                (
                    0,
                    GSTimestamp::new(&[0, 0]),
                    (
                        vec![],
                        vec![
                            ((2, 3), 1),
                            ((3, 9), 1),
                            ((4, 2), 1),
                            ((4, 5), 1),
                            ((5, 2), 1),
                            ((5, 9), 1),
                            ((6, 4), 1),
                            ((9, 8), 1),
                        ],
                    ),
                    (8, 0),
                ),
                (1, GSTimestamp::new(&[0, 1]), (vec![], vec![((2, 9), 1), ((4, 5), -1)]), (1, 1)),
                (
                    2,
                    GSTimestamp::new(&[0, 2]),
                    (vec![], vec![((9, 3), 1), ((6, 5), 1), ((5, 9), -1), ((2, 3), -1)]),
                    (2, 2),
                ),
                (
                    3,
                    GSTimestamp::new(&[0, 3]),
                    (vec![], vec![((4, 5), 1), ((2, 3), 1), ((2, 9), -1), ((9, 3), -1)]),
                    (2, 2),
                ),
            ],
        );
    }

    fn assert(
        m: usize,
        n: usize,
        dir: &str,
        prefix: &str,
        expected_data: &[CubeDataEntries<GSTimestamp>],
    ) {
        let mut global_store = GlobalStore::default();

        let cube_name = "my_cube";
        let mut cube_query = format!(
            "load cube {name} {m} {n} from '{dir}' with prefix '{prefix}';",
            name = cube_name,
            m = m,
            n = n,
            dir = dir,
            prefix = prefix
        );
        process_query(&mut global_store, &mut cube_query).expect("Cube not loaded");

        let cube = global_store.filtered_cube_store.cubes.get(cube_name).expect("Cube not found");

        assert_eq!(&cube.data.entries[..], expected_data);
    }
}
