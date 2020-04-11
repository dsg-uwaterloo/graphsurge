use crate::computations::dataflow_arranged_adaptive::arranged_adaptive_run;
use crate::computations::dataflow_differential_arranged::differential_run_arranged;
use crate::filtered_cubes::materialise::{DiffEdgesPointer, DiffIteratorPointer};
use crate::filtered_cubes::timestamp::timestamp_mappings::GSTimestampIndex;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::{CubePointer, FilteredCubeData};
use crate::graph::properties::property_value::PropertyValue;
use crate::query_handler::run_computation::MaterializeResults;
use crate::util::io::GSWriter;
use crate::util::timer::GSDuration;
use crate::{
    computations::dataflow_differential::differential_run,
    error::{computation_error, GraphSurgeError},
    filtered_cubes::{materialise::CubeDiffIterators, FilteredCube},
    graph::Graph,
    query_handler::GraphSurgeResult,
    util::timer::GSTimer,
};
use gs_analytics_api::{BasicComputation, ComputationTypes, DiffCount, GraphsurgeComputation};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use log::info;
use std::fmt::Debug;
use std::hash::Hash;

pub mod bfs;
pub mod builder;
pub mod dataflow_arranged_adaptive;
pub mod dataflow_differential;
pub mod dataflow_differential_arranged;
pub mod filtered_cubes;
pub mod pagerank;
pub mod scc;
pub mod spsp;
pub mod views;
pub mod wcc;

pub type TimelyTimeStamp = usize;
pub type DifferentialRunOutput<C, T> =
    Vec<Result<(DifferentialResults<C, T>, Times<T>, GSDuration, SplitIndices), String>>;
pub type SplitIndices = HashSet<usize>;
pub type Times<T> = Vec<(T, (GSDuration, GSDuration, GSDuration, GSDuration))>;
pub type ProcessedResults<C> = (DifferentialResults<C, GSTimestamp>, TimeResults, SplitIndices);
pub type TimeResults = (HashMap<GSTimestamp, [Vec<GSDuration>; 4]>, Vec<GSDuration>);
pub type DifferentialResults<C, T> = HashMap<T, DifferentialResult<C>>;
pub type DifferentialResult<C> = Vec<(<C as ComputationTypes>::Result, DiffCount)>;
pub type ComputationResultSet<C> = HashSet<(<C as ComputationTypes>::Result, DiffCount)>;
pub type ComputationResults<C> = HashMap<GSTimestampIndex, (ComputationResultSet<C>, GSTimestamp)>;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ComputationType {
    Differential,
    DifferentialArranged,
    Adaptive,
    Individual,
    IndividualArranged,
    CompareDifferential,
}

impl ComputationType {
    pub fn description(self) -> &'static str {
        match self {
            ComputationType::Differential => "differential-with-diffs",
            ComputationType::DifferentialArranged => "arranged-with-diffs",
            ComputationType::Adaptive => "adaptive",
            ComputationType::Individual => "differentially-individually",
            ComputationType::IndividualArranged => "arranged-individually",
            ComputationType::CompareDifferential => "compare-differential",
        }
    }
}

impl std::fmt::Display for ComputationType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            if let ComputationType::CompareDifferential = self {
                String::new()
            } else {
                format!(" {:?}", self).to_lowercase()
            }
        )
    }
}

#[derive(new, Copy, Clone)]
pub struct ComputationRuntimeData<'a> {
    pub graph: &'a Graph,
    pub c_type: ComputationType,
    pub materialize_results: MaterializeResults,
    pub save_to: &'a Option<String>,
    pub threads: usize,
    pub process_id: usize,
    pub hosts: &'a [String],
    pub splits: &'a Option<HashSet<usize>>,
    pub batch_size: Option<usize>,
}

pub enum ComputationProperties {
    Value(PropertyValue),
    Pairs(Vec<(usize, usize)>),
}

impl std::fmt::Display for ComputationProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ComputationProperties::Value(v) => write!(f, "{}", v),
            ComputationProperties::Pairs(ps) => write!(
                f,
                "[{}]",
                ps.iter().map(|p| format!("{:?}", p)).collect::<Vec<_>>().join(",")
            ),
        }
    }
}

impl ComputationProperties {
    pub fn get_type(&self) -> String {
        match self {
            ComputationProperties::Value(v) => v.value_type().to_string(),
            ComputationProperties::Pairs(_) => "Pairs".to_string(),
        }
    }
}

pub trait Computation:
    BasicComputation + GraphsurgeComputation + Send + Sync + Clone + 'static
{
    fn execute(
        &self,
        cube: &mut FilteredCube,
        runtime_data: ComputationRuntimeData,
    ) -> Result<GraphSurgeResult, GraphSurgeError> {
        match runtime_data.c_type {
            ComputationType::Individual
            | ComputationType::DifferentialArranged
            | ComputationType::IndividualArranged => (), // Nothing to do.
            ComputationType::Differential
            | ComputationType::Adaptive
            | ComputationType::CompareDifferential => {
                cube.prepare_differential_data();
            }
        }
        match runtime_data.c_type {
            ComputationType::Differential
            | ComputationType::DifferentialArranged
            | ComputationType::Adaptive => {
                differential_diff_execute(cube, self, runtime_data, runtime_data.c_type)?;
                Ok(GraphSurgeResult::new("DD with diffs done".to_string()))
            }
            ComputationType::Individual | ComputationType::IndividualArranged => {
                differential_individual_execute(
                    cube,
                    self,
                    runtime_data,
                    runtime_data.c_type == ComputationType::IndividualArranged,
                )?;
                Ok(GraphSurgeResult::new("DD individually done".to_string()))
            }
            ComputationType::CompareDifferential => {
                execute_compare_differential(&cube, self, runtime_data)
            }
        }
    }
}

impl<T: BasicComputation + GraphsurgeComputation + Send + Sync + Clone + 'static> Computation
    for T
{
}

pub fn execute_compare_differential<T: Computation>(
    cube: &FilteredCube,
    computation: &T,
    runtime_data: ComputationRuntimeData,
) -> Result<GraphSurgeResult, GraphSurgeError> {
    let r1 =
        differential_diff_execute(cube, computation, runtime_data, ComputationType::Differential)?;
    let r2 = differential_individual_execute(cube, computation, runtime_data, false)?;
    compare::<T>(r1, r2)
}

fn compare<T: Computation>(
    mut r1: ComputationResults<T>,
    mut r2: ComputationResults<T>,
) -> Result<GraphSurgeResult, GraphSurgeError> {
    info!("Result Counts:");
    for (timestamp_index, (values, timestamp)) in r1.iter().sorted_by_key(|&(ts_index, _)| ts_index)
    {
        info!(
            "{}: total {},{}",
            timestamp,
            values.len(),
            r2.get(timestamp_index).map_or(0, |(values, _)| values.len())
        );
    }
    info!("Checking result values...");
    let mut alright = true;
    let print_count = 3;
    for (timestamp_index1, (values1, timestamp1)) in r1.drain() {
        if let Some((values2, _)) = r2.remove(&timestamp_index1) {
            let diff1 = values1.difference(&values2).collect_vec();
            let diff2 = values2.difference(&values1).collect_vec();
            if !diff1.is_empty() {
                alright = false;
                info!(
                    "{} values [e.g.: {:?}] not present in right results",
                    diff1.len(),
                    diff1.iter().sorted().take(print_count).collect::<Vec<_>>()
                );
            }
            if !diff2.is_empty() {
                alright = false;
                info!(
                    "{} values [e.g.: {:?}] not present in left results",
                    diff2.len(),
                    diff2.iter().sorted().take(print_count).collect::<Vec<_>>()
                );
            }
        } else {
            alright = false;
            info!("Key {} missing in right results", timestamp1);
        }
    }
    if !r2.is_empty() {
        alright = false;
        info!(
            "{} keys [e,g,: {:?}] not present in left results",
            r2.len(),
            r2.keys().take(print_count).collect::<Vec<_>>()
        );
    }
    if alright {
        Ok(GraphSurgeResult::new("Results match".to_string()))
    } else {
        Err(computation_error("Results do not match".to_string()))
    }
}

fn differential_individual_execute<C: Computation>(
    cube: &FilteredCube,
    computation: &C,
    runtime_data: ComputationRuntimeData,
    is_arranged: bool,
) -> Result<ComputationResults<C>, GraphSurgeError>
where
    C::Result: Hash,
{
    info!("Differential individually:");

    let mut full_results = Vec::new();
    let timer = GSTimer::now();

    let mut total_worker_time = GSDuration::default();
    let mut merged_times = HashMap::new();
    let fixed_timestamp = GSTimestamp::default();
    let next_timestamp = GSTimestamp::new(&[1]);
    let materialize_results = runtime_data.materialize_results != MaterializeResults::None;
    for (timestamp_index, timestamp, (data, _), _) in &cube.data.entries {
        info!("Starting computation for {}", timestamp);
        let data = data.iter().map(|&x| (x, 1)).collect_vec();
        let results = if is_arranged {
            let len = data.len();
            let cube_data =
                FilteredCubeData::new(vec![(0, fixed_timestamp, (Vec::new(), data), (len, 0))]);
            differential_run_arranged(
                CubePointer::new(&cube_data),
                computation.clone(),
                fixed_timestamp,
                materialize_results,
                runtime_data.threads,
                runtime_data.process_id,
                runtime_data.hosts,
            )?
        } else {
            let diff_iterators = CubeDiffIterators::Inner(vec![(
                fixed_timestamp,
                next_timestamp,
                DiffEdgesPointer::new(&data),
            )]);
            differential_run(
                DiffIteratorPointer::new(&diff_iterators),
                fixed_timestamp,
                computation.clone(),
                runtime_data.threads,
                materialize_results,
            )?
        };

        let (mut results, (mut all_times, worker_times), _) =
            process_results::<C>(results, materialize_results)?;

        let results = if let Some(result) = results.remove(&fixed_timestamp) {
            result.into_iter().collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        full_results.push((*timestamp_index, (results, *timestamp)));
        total_worker_time += worker_times.into_iter().max().expect("Max not found");
        merged_times.insert(*timestamp, all_times.remove(&fixed_timestamp).expect("TS missing"));
    }
    process_times(
        (merged_times, vec![total_worker_time]),
        fixed_timestamp,
        &cube,
        "differential individually",
    );
    info!("Done with differential individually in {}", timer.elapsed().to_seconds_string());
    if runtime_data.materialize_results != MaterializeResults::None {
        print_save_results::<C>(&full_results, runtime_data.save_to)?;
    }

    Ok(full_results.into_iter().collect())
}

fn differential_diff_execute<C: Computation>(
    cube: &FilteredCube,
    computation: &C,
    runtime_data: ComputationRuntimeData,
    ctype: ComputationType,
) -> Result<ComputationResults<C>, GraphSurgeError>
where
    C::Result: Hash,
{
    info!("Running {}:", ctype.description());

    let timer = GSTimer::now();
    let zeroth_ts = GSTimestamp::get_zeroth_timestamp();
    let materialize_results = runtime_data.materialize_results != MaterializeResults::None;
    #[allow(clippy::wildcard_enum_match_arm)]
    let results = match ctype {
        ComputationType::Differential => {
            let diff_data = cube.differential_data.as_ref().expect("Should have been materialized");
            differential_run(
                DiffIteratorPointer::new(&diff_data.cube_diff_iterators),
                zeroth_ts,
                computation.clone(),
                runtime_data.threads,
                materialize_results,
            )?
        }
        ComputationType::DifferentialArranged => differential_run_arranged(
            CubePointer::new(&cube.data),
            computation.clone(),
            zeroth_ts,
            materialize_results,
            runtime_data.threads,
            runtime_data.process_id,
            runtime_data.hosts,
        )?,
        ComputationType::Adaptive => arranged_adaptive_run(
            CubePointer::new(&cube.data),
            runtime_data.splits.clone(),
            computation.clone(),
            materialize_results,
            runtime_data.threads,
            runtime_data.process_id,
            runtime_data.hosts,
            runtime_data.batch_size,
        )?,
        _ => unreachable!(),
    };

    let (mut diff_results, all_times, split_indices) =
        process_results::<C>(results, materialize_results)?;

    if materialize_results {
        let empty_vec = Vec::new();
        for (_, timestamp) in &cube.timestamp_mappings.0 {
            let data = diff_results.get(&timestamp).unwrap_or(&empty_vec); // Empty timestamps.
            info!("{}: {} results", timestamp, data.len());
        }
    }
    process_times(all_times, zeroth_ts, &cube, ctype.description());
    info!("Done with {} in {}", ctype.description(), timer.elapsed().to_seconds_string());

    if let Some(save_path) = runtime_data.save_to {
        info!("Writing diff results to '{}'...", save_path);
        let empty_vec = Vec::new();
        for (_, timestamp) in &cube.timestamp_mappings.0 {
            let data = diff_results.get(&timestamp).unwrap_or(&empty_vec); // Empty timestamps.
            info!("Writing {}, len={}", timestamp, data.len());
            let timestamp_path =
                format!("{}/results-diff-{}.txt", save_path, timestamp.get_str('_'));
            let mut writer = GSWriter::new(timestamp_path)?;
            writer.write_file_lines(data.iter().map(|(v, diff)| format!("{:?}, {:+}", v, diff)))?;
        }
    }

    let mut full_results = Vec::<(GSTimestampIndex, (ComputationResultSet<C>, GSTimestamp))>::new();
    if runtime_data.materialize_results == MaterializeResults::Full {
        info!("Materializing full results:");
        let timer = GSTimer::now();
        for (timestamp_index, (diff_neighbors, timestamp)) in
            cube.timestamp_mappings.0.iter().enumerate()
        {
            let diff_data = diff_results.remove(&timestamp).unwrap_or_default(); // Some timestamps might have 0 results.
            let full_result = if split_indices.contains(&timestamp_index) {
                diff_data.into_iter().collect()
            } else {
                let previous_results = {
                    // Declare a closure that obtains previous results for a given set of timestamps.
                    let get_previous_results = |data: &[GSTimestampIndex]| {
                        data.iter()
                            .map(|&timestamp_index_positive| {
                                (full_results
                                    .get(timestamp_index_positive)
                                    .expect("Previous timestamp should have results")
                                    .1)
                                    .0
                                    .iter()
                            })
                            .collect_vec()
                    };
                    // Get the previous positive and negative results for the current timestamp.
                    (
                        get_previous_results(&diff_neighbors.0),
                        get_previous_results(&diff_neighbors.1),
                    )
                };
                diff_data
               .into_iter()
                   // Add the previous positive results.
                   .merge(previous_results.0.into_iter().flatten().map(|&(e, diff)| (e, diff)))
                   // Subtract the previous negative results.
                   .merge(previous_results.1.into_iter().flatten().map(|&(e, diff)| (e, -diff)))
                   .into_group_map()
                   .into_iter()
                   .filter_map(|(e, diffs)| {
                       let sum: isize = diffs.iter().sum();
                       if sum == 0 {
                           None
                       } else {
                           Some((e, sum))
                       }
                   })
                   .collect()
            };
            full_results.push((timestamp_index, (full_result, *timestamp)));
        }
        info!("Results materialized in {}", timer.elapsed().to_seconds_string());
        print_save_results::<C>(&full_results, runtime_data.save_to)?;
    }

    Ok(full_results.into_iter().collect())
}

fn process_results<C: ComputationTypes>(
    worker_results: DifferentialRunOutput<C, GSTimestamp>,
    materialize_results: bool,
) -> Result<ProcessedResults<C>, GraphSurgeError> {
    let mut final_results = HashMap::new();
    let mut merged_times = HashMap::new();
    let mut total_times = Vec::new();
    let mut merged_results = HashMap::new();
    let mut all_split_indices = HashSet::new();

    for (index, worker_result) in worker_results.into_iter().enumerate() {
        let (results, times, total_time, split_indices) = worker_result.map_err(|e| {
            computation_error(format!("Results from differential has errors: {:?}", e))
        })?;
        if materialize_results {
            let mut count = 0;
            for (time, vec) in results {
                count += vec.len();
                merged_results.entry(time).or_insert_with(Vec::new).extend(vec.into_iter());
            }
            info!("Worker {:>2} found {} results", index, count);
        }
        for (timestamp, (merged, loaded, stable, total)) in times {
            let entry = merged_times
                .entry(timestamp)
                .or_insert_with(|| [Vec::new(), Vec::new(), Vec::new(), Vec::new()]);
            entry[0].push(merged);
            entry[1].push(loaded);
            entry[2].push(stable);
            entry[3].push(total);
        }
        total_times.push(total_time);
        all_split_indices.extend(split_indices);
    }
    if materialize_results {
        for (timestamp_string, values) in merged_results {
            let data = values
                .into_iter()
                .into_group_map()
                .into_iter()
                .filter_map(|(k, v)| {
                    let sum: isize = v.into_iter().sum();
                    if sum == 0 {
                        None
                    } else {
                        Some((k, sum))
                    }
                })
                .collect_vec();
            final_results.insert(timestamp_string, data);
        }
    }
    Ok((final_results, (merged_times, total_times), all_split_indices))
}

fn process_times(
    (merged_times, total_times): TimeResults,
    zeroth_ts: GSTimestamp,
    cube: &FilteredCube,
    msg: &str,
) {
    let mut all_times = [[GSDuration::default(); 4]; 3];
    for (_, timestamp) in &cube.timestamp_mappings.0 {
        let loaded_times = merged_times
            .get(timestamp)
            .expect("TS not found")
            .iter()
            .map(|times| *times.iter().max().expect("Max not found"))
            .collect::<Vec<_>>();
        let (merged_max, loaded_max, stable_max, total_max) =
            (loaded_times[0], loaded_times[1], loaded_times[2], loaded_times[3]);
        info!(
            "{} times: {}: loaded = {}, stable = {}, total = {}",
            msg,
            timestamp,
            loaded_max.to_seconds_string(),
            stable_max.to_seconds_string(),
            total_max.to_seconds_string()
        );
        if *timestamp == zeroth_ts {
            all_times[0][0] = merged_max;
            all_times[0][1] = loaded_max;
            all_times[0][2] = stable_max;
            all_times[0][3] = total_max;
        } else {
            all_times[1][0] += merged_max;
            all_times[1][1] += loaded_max;
            all_times[1][2] += stable_max;
            all_times[1][3] += total_max;
        }
        all_times[2][0] += merged_max;
        all_times[2][1] += loaded_max;
        all_times[2][2] += stable_max;
        all_times[2][3] += total_max;
    }
    info!(
        "Computation of {} worker time: {}",
        msg,
        total_times.iter().max().expect("Max not found").to_seconds_string()
    );
    for (max_times, strn) in all_times.iter().zip_eq(["initial", "rest", "total"].iter()) {
        for (max_time, strn2) in
            max_times.iter().zip_eq(["merged", "loaded", "stable", "total"].iter())
        {
            info!(
                "Computation of {} {:>7} {:>6}: {}",
                msg,
                strn,
                strn2,
                max_time.to_seconds_string()
            );
        }
    }
}

/// Common function to print full results and optionally save the results to disk.
fn print_save_results<T: Computation>(
    full_results: &[(GSTimestampIndex, (ComputationResultSet<T>, GSTimestamp))],
    save_to: &Option<String>,
) -> Result<(), GraphSurgeError> {
    for (_, (results, timestamp)) in full_results {
        info!("{}: {} results", timestamp, results.len());
    }

    if let Some(save_path) = save_to {
        info!("Writing materialized results to '{}'...", save_path);
        for (_, (results, timestamp)) in full_results {
            info!("Writing {}, len={}", timestamp, results.len());
            let timestamp_path =
                format!("{}/results-full-{}.txt", save_path, timestamp.get_str('_'));
            let mut writer = GSWriter::new(timestamp_path)?;
            writer.write_file_lines(
                results.iter().map(|(data, change)| format!("{:?}, {:+}", data, change)),
            )?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::computations::bfs::BFS;
    use crate::computations::dataflow_arranged_adaptive::arranged_adaptive_run;
    use crate::computations::dataflow_differential::differential_run;
    use crate::computations::dataflow_differential_arranged::differential_run_arranged;
    use crate::computations::scc::SCC;
    use crate::computations::wcc::WCC;
    use crate::computations::{
        differential_diff_execute, process_results, Computation, ComputationRuntimeData,
        ComputationType, DifferentialResults, SplitIndices,
    };
    use crate::filtered_cubes::materialise::DiffIteratorPointer;
    use crate::filtered_cubes::timestamp::GSTimestamp;
    use crate::filtered_cubes::CubePointer;
    use crate::global_store::GlobalStore;
    use crate::process_query;
    use crate::query_handler::run_computation::MaterializeResults;
    use gs_analytics_api::{ComputationTypes, DiffCount};
    use hashbrown::{HashMap, HashSet};

    #[test]
    fn test_bfs_2d() {
        let expected_diff_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((4, 1), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 4), 1),
                    ((6, 0), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 1]),
                vec![
                    ((5, 2), -1),
                    ((9, 3), -1),
                    ((5, 1), 1),
                    ((9, 4), 1),
                    ((8, 5), 1),
                    ((8, 4), -1),
                ],
            ),
            (GSTimestamp::new(&[1, 0]), vec![((5, 2), -1)]),
            (
                GSTimestamp::new(&[1, 1]),
                vec![
                    ((8, 4), 1),
                    ((8, 5), -1),
                    ((3, 3), -1),
                    ((3, 4), 1),
                    ((9, 4), -1),
                    ((9, 3), 1),
                    ((5, 2), 1),
                ],
            ),
        ];

        let expected_full_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((6, 0), 1),
                    ((4, 1), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 4), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 1]),
                vec![
                    ((6, 0), 1),
                    ((4, 1), 1),
                    ((5, 1), 1),
                    ((2, 2), 1),
                    ((3, 3), 1),
                    ((9, 4), 1),
                    ((8, 5), 1),
                ],
            ),
            (
                GSTimestamp::new(&[1, 0]),
                vec![((6, 0), 1), ((4, 1), 1), ((2, 2), 1), ((3, 3), 1), ((9, 3), 1), ((8, 4), 1)],
            ),
            (
                GSTimestamp::new(&[1, 1]),
                vec![
                    ((6, 0), 1),
                    ((4, 1), 1),
                    ((5, 1), 1),
                    ((2, 2), 1),
                    ((9, 3), 1),
                    ((3, 4), 1),
                    ((8, 4), 1),
                ],
            ),
        ];

        let computation = BFS::new(6);
        create_cube_and_assert(
            &computation,
            2,
            2,
            "data/test_data/bfs/2d_small",
            &expected_diff_results,
            &expected_full_results,
            10000,
        );
    }

    #[test]
    fn test_bfs_1d() {
        let expected_diff_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((4, 1), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 4), 1),
                    ((6, 0), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (GSTimestamp::new(&[0, 1]), vec![((5, 2), -1)]),
            (GSTimestamp::new(&[0, 2]), vec![((5, 1), 1), ((3, 4), 1), ((3, 3), -1)]),
            (
                GSTimestamp::new(&[0, 3]),
                vec![
                    ((8, 4), -1),
                    ((9, 4), 1),
                    ((8, 5), 1),
                    ((9, 3), -1),
                    ((3, 3), 1),
                    ((3, 4), -1),
                ],
            ),
        ];

        let expected_full_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((6, 0), 1),
                    ((4, 1), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 4), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 3]),
                vec![
                    ((6, 0), 1),
                    ((4, 1), 1),
                    ((5, 1), 1),
                    ((2, 2), 1),
                    ((3, 3), 1),
                    ((9, 4), 1),
                    ((8, 5), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 1]),
                vec![((6, 0), 1), ((4, 1), 1), ((2, 2), 1), ((3, 3), 1), ((9, 3), 1), ((8, 4), 1)],
            ),
            (
                GSTimestamp::new(&[0, 2]),
                vec![
                    ((6, 0), 1),
                    ((4, 1), 1),
                    ((5, 1), 1),
                    ((2, 2), 1),
                    ((9, 3), 1),
                    ((3, 4), 1),
                    ((8, 4), 1),
                ],
            ),
        ];

        let computation = BFS::new(6);
        create_cube_and_assert(
            &computation,
            1,
            4,
            "data/test_data/bfs/1d_small",
            &expected_diff_results,
            &expected_full_results,
            10010,
        );
    }

    #[test]
    fn test_wcc_2d() {
        let expected_diff_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((4, 2), 1),
                    ((3, 2), 1),
                    ((9, 2), 1),
                    ((8, 2), 1),
                    ((6, 2), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[1, 0]),
                vec![
                    ((9, 2), -1),
                    ((9, 3), 1),
                    ((8, 2), -1),
                    ((8, 3), 1),
                    ((3, 2), -1),
                    ((3, 3), 1),
                ],
            ),
            (GSTimestamp::new(&[0, 1]), vec![((6, 2), -1), ((6, 4), 1), ((4, 2), -1), ((4, 4), 1)]),
            (GSTimestamp::new(&[1, 1]), vec![]),
        ];

        let expected_full_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((4, 2), 1),
                    ((3, 2), 1),
                    ((9, 2), 1),
                    ((8, 2), 1),
                    ((6, 2), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[1, 0]),
                vec![
                    ((4, 2), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 3), 1),
                    ((6, 2), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 1]),
                vec![
                    ((4, 4), 1),
                    ((3, 2), 1),
                    ((9, 2), 1),
                    ((8, 2), 1),
                    ((6, 4), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[1, 1]),
                vec![
                    ((4, 4), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 3), 1),
                    ((6, 4), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
        ];

        let computation = WCC {};
        create_cube_and_assert(
            &computation,
            2,
            2,
            "data/test_data/wcc/2d_small",
            &expected_diff_results,
            &expected_full_results,
            10020,
        );

        let computation = WCC {};
        create_cube_and_assert(
            &computation,
            2,
            2,
            "data/test_data/wcc/2d_small",
            &expected_diff_results,
            &expected_full_results,
            10030,
        );
    }

    #[test]
    fn test_wcc_1d() {
        let expected_diff_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((4, 2), 1),
                    ((3, 2), 1),
                    ((9, 2), 1),
                    ((8, 2), 1),
                    ((6, 2), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 1]),
                vec![
                    ((9, 2), -1),
                    ((9, 3), 1),
                    ((8, 2), -1),
                    ((8, 3), 1),
                    ((3, 2), -1),
                    ((3, 3), 1),
                ],
            ),
            (GSTimestamp::new(&[0, 2]), vec![((6, 4), 1), ((4, 2), -1), ((4, 4), 1), ((6, 2), -1)]),
            (
                GSTimestamp::new(&[0, 3]),
                vec![
                    ((9, 3), -1),
                    ((3, 2), 1),
                    ((8, 2), 1),
                    ((9, 2), 1),
                    ((8, 3), -1),
                    ((3, 3), -1),
                ],
            ),
        ];

        let expected_full_results = vec![
            (
                GSTimestamp::new(&[0, 0]),
                vec![
                    ((4, 2), 1),
                    ((3, 2), 1),
                    ((9, 2), 1),
                    ((8, 2), 1),
                    ((6, 2), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 1]),
                vec![
                    ((4, 2), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 3), 1),
                    ((6, 2), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 3]),
                vec![
                    ((4, 4), 1),
                    ((3, 2), 1),
                    ((9, 2), 1),
                    ((8, 2), 1),
                    ((6, 4), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
            (
                GSTimestamp::new(&[0, 2]),
                vec![
                    ((4, 4), 1),
                    ((3, 3), 1),
                    ((9, 3), 1),
                    ((8, 3), 1),
                    ((6, 4), 1),
                    ((2, 2), 1),
                    ((5, 2), 1),
                ],
            ),
        ];

        let computation = WCC {};
        create_cube_and_assert(
            &computation,
            1,
            4,
            "data/test_data/wcc/1d_small",
            &expected_diff_results,
            &expected_full_results,
            10040,
        );

        let computation = WCC {};
        create_cube_and_assert(
            &computation,
            1,
            4,
            "data/test_data/wcc/1d_small",
            &expected_diff_results,
            &expected_full_results,
            10050,
        );
    }

    #[test]
    fn test_scc_1() {
        let expected_results = vec![(
            GSTimestamp::new(&[0, 0]),
            vec![
                ((2, 5), 1),
                ((7, 6), 1),
                ((4, 3), 1),
                ((1, 2), 1),
                ((5, 1), 1),
                ((4, 8), 1),
                ((8, 4), 1),
                ((3, 4), 1),
                ((6, 7), 1),
            ],
        )];

        let computation = SCC {};
        create_cube_and_assert(
            &computation,
            1,
            1,
            "data/test_data/scc/1",
            &expected_results,
            &expected_results,
            10060,
        );
    }

    #[test]
    fn test_scc_2() {
        let expected_results = vec![(
            GSTimestamp::new(&[0, 0]),
            vec![
                ((4, 5), 1),
                ((4, 6), 1),
                ((3, 0), 1),
                ((5, 4), 1),
                ((8, 9), 1),
                ((2, 3), 1),
                ((0, 2), 1),
                ((6, 5), 1),
                ((1, 3), 1),
                ((9, 8), 1),
                ((0, 1), 1),
            ],
        )];

        let computation = SCC {};
        create_cube_and_assert(
            &computation,
            1,
            1,
            "data/test_data/scc/2",
            &expected_results,
            &expected_results,
            10070,
        );
    }

    #[test]
    fn test_scc_3() {
        let expected_results = vec![(
            GSTimestamp::new(&[0, 0]),
            vec![
                ((4, 3), 1),
                ((6, 8), 1),
                ((2, 4), 1),
                ((1, 0), 1),
                ((2, 1), 1),
                ((5, 6), 1),
                ((14, 12), 1),
                ((9, 10), 1),
                ((6, 7), 1),
                ((3, 2), 1),
                ((0, 2), 1),
                ((7, 8), 1),
                ((8, 5), 1),
                ((13, 14), 1),
                ((10, 9), 1),
                ((12, 13), 1),
            ],
        )];

        let computation = SCC {};
        create_cube_and_assert(
            &computation,
            1,
            1,
            "data/test_data/scc/3",
            &expected_results,
            &expected_results,
            10080,
        );
    }

    #[test]
    fn test_scc_adaptive() {
        let computation = SCC {};

        for (index, (splits, expected_results, expect_splits)) in vec![
            (
                Some(HashSet::new()),
                vec![
                    (
                        GSTimestamp::new(&[0, 0]),
                        vec![
                            ((4, 5), 1),
                            ((4, 6), 1),
                            ((3, 0), 1),
                            ((5, 4), 1),
                            ((8, 9), 1),
                            ((2, 3), 1),
                            ((0, 2), 1),
                            ((6, 5), 1),
                            ((1, 3), 1),
                            ((9, 8), 1),
                            ((0, 1), 1),
                        ],
                    ),
                    (
                        GSTimestamp::new(&[0, 1]),
                        vec![
                            ((1, 3), -1),
                            ((6, 5), -1),
                            ((2, 5), 1),
                            ((0, 2), -1),
                            ((7, 6), 1),
                            ((6, 7), 1),
                            ((4, 5), -1),
                            ((4, 8), 1),
                            ((2, 3), -1),
                            ((4, 3), 1),
                            ((3, 0), -1),
                            ((8, 4), 1),
                            ((5, 4), -1),
                            ((8, 9), -1),
                            ((0, 1), -1),
                            ((5, 1), 1),
                            ((4, 6), -1),
                            ((1, 2), 1),
                            ((3, 4), 1),
                            ((9, 8), -1),
                        ],
                    ),
                    (GSTimestamp::new(&[0, 2]), vec![((7, 6), -1), ((6, 7), -1)]),
                    (
                        GSTimestamp::new(&[0, 3]),
                        vec![
                            ((8, 9), 1),
                            ((4, 6), 1),
                            ((2, 5), -1),
                            ((4, 8), -1),
                            ((4, 5), 1),
                            ((4, 3), -1),
                            ((5, 4), 1),
                            ((3, 0), 1),
                            ((8, 4), -1),
                            ((5, 1), -1),
                            ((0, 2), 1),
                            ((9, 8), 1),
                            ((1, 3), 1),
                            ((0, 1), 1),
                            ((6, 5), 1),
                            ((1, 2), -1),
                            ((3, 4), -1),
                            ((2, 3), 1),
                        ],
                    ),
                ],
                HashSet::new(),
            ),
            (
                Some(vec![2, 3].into_iter().collect()),
                vec![
                    (
                        GSTimestamp::new(&[0, 0]),
                        vec![
                            ((4, 5), 1),
                            ((4, 6), 1),
                            ((3, 0), 1),
                            ((5, 4), 1),
                            ((8, 9), 1),
                            ((2, 3), 1),
                            ((0, 2), 1),
                            ((6, 5), 1),
                            ((1, 3), 1),
                            ((9, 8), 1),
                            ((0, 1), 1),
                        ],
                    ),
                    (
                        GSTimestamp::new(&[0, 1]),
                        vec![
                            ((1, 3), -1),
                            ((6, 5), -1),
                            ((2, 5), 1),
                            ((0, 2), -1),
                            ((7, 6), 1),
                            ((6, 7), 1),
                            ((4, 5), -1),
                            ((4, 8), 1),
                            ((2, 3), -1),
                            ((4, 3), 1),
                            ((3, 0), -1),
                            ((8, 4), 1),
                            ((5, 4), -1),
                            ((8, 9), -1),
                            ((0, 1), -1),
                            ((5, 1), 1),
                            ((4, 6), -1),
                            ((1, 2), 1),
                            ((3, 4), 1),
                            ((9, 8), -1),
                        ],
                    ),
                    (
                        GSTimestamp::new(&[0, 2]),
                        vec![
                            ((1, 2), 1),
                            ((2, 5), 1),
                            ((3, 4), 1),
                            ((4, 3), 1),
                            ((8, 4), 1),
                            ((4, 8), 1),
                            ((5, 1), 1),
                        ],
                    ),
                    (
                        GSTimestamp::new(&[0, 3]),
                        vec![
                            ((5, 4), 1),
                            ((0, 2), 1),
                            ((4, 5), 1),
                            ((8, 9), 1),
                            ((1, 3), 1),
                            ((2, 3), 1),
                            ((9, 8), 1),
                            ((0, 1), 1),
                            ((4, 6), 1),
                            ((6, 5), 1),
                            ((3, 0), 1),
                        ],
                    ),
                ],
                vec![2, 3].into_iter().collect(),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            create_cube_and_assert_adaptive(
                &computation,
                1,
                4,
                "data/test_data/scc/4",
                &expected_results,
                &splits,
                &expect_splits,
                index,
            );
        }
    }

    type ExpectedResults<C> = (GSTimestamp, Vec<(<C as ComputationTypes>::Result, DiffCount)>);

    fn create_cube_and_assert<C: Computation>(
        computation: &C,
        m: usize,
        n: usize,
        dir: &str,
        expected_diff_results: &[ExpectedResults<C>],
        expected_full_results: &[ExpectedResults<C>],
        port_offset: usize,
    ) {
        let mut global_store = GlobalStore::default();

        let cube_name = "my_cube";
        let mut cube_query = format!(
            "load cube {name} {m} {n} from '{dir}' with prefix 'batch-0_';",
            name = cube_name,
            m = m,
            n = n,
            dir = dir,
        );
        process_query(&mut global_store, &mut cube_query).expect("Cube not loaded");

        let cube =
            global_store.filtered_cube_store.cubes.get_mut(cube_name).expect("Expected cube");

        cube.prepare_differential_data();

        let expected_diff_results = expected_diff_results
            .iter()
            .map(|(ts, values)| (*ts, values.iter().copied().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>();

        let expected_full_results = expected_full_results
            .iter()
            .map(|(ts, values)| (*ts, values.iter().copied().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>();

        for &threads in &[1, 4] {
            let results = differential_run(
                DiffIteratorPointer::new(
                    &cube.differential_data.as_ref().expect("Data expected").cube_diff_iterators,
                ),
                GSTimestamp::get_zeroth_timestamp(),
                computation.clone(),
                threads,
                true,
            )
            .expect("Computation failed");
            let (computed_results, _, _) =
                process_results::<C>(results, true).expect("Processing failed");

            assert_results_match::<C>(
                computed_results,
                &expected_diff_results,
                &format!("Failed to match results of computation using {} threads", threads),
            );

            let results = differential_run_arranged(
                CubePointer::new(&cube.data),
                computation.clone(),
                GSTimestamp::get_zeroth_timestamp(),
                true,
                threads,
                0,
                &[],
            )
            .expect("Computation failed");
            let (computed_results_arranged, _, _) =
                process_results::<C>(results, true).expect("Processing failed");

            assert_results_match::<C>(
                computed_results_arranged,
                &expected_diff_results,
                &format!(
                    "Failed to match results of arranged computation using {} threads",
                    threads
                ),
            );
        }

        let threads = 2;
        let hosts =
            [format!("localhost:{}", port_offset), format!("localhost:{}", port_offset + 1)];

        let cdata = cube.data.clone();
        let comp = computation.clone();
        let hosts2 = hosts.clone();
        std::thread::spawn(move || {
            differential_run_arranged(
                CubePointer::new(&cdata),
                comp,
                GSTimestamp::get_zeroth_timestamp(),
                true,
                threads,
                1,
                &hosts2,
            )
            .expect("Computation failed");
        });

        let results = differential_run_arranged(
            CubePointer::new(&cube.data),
            computation.clone(),
            GSTimestamp::get_zeroth_timestamp(),
            true,
            threads,
            0,
            &hosts,
        )
        .expect("Computation failed");
        let (computed_results_arranged_multiprocess, _, _) =
            process_results::<C>(results, true).expect("Processing failed");

        assert_results_match::<C>(
            computed_results_arranged_multiprocess,
            &expected_diff_results,
            &format!(
                "Failed to match results of arranged computation using {} threads and 2 processes",
                threads
            ),
        );

        let full_results = differential_diff_execute(
            cube,
            computation,
            ComputationRuntimeData::new(
                &global_store.graph,
                ComputationType::Differential,
                MaterializeResults::Full,
                &None,
                1,
                0,
                &[],
                &None,
                None,
            ),
            ComputationType::Differential,
        )
        .expect("Computation failed");
        assert_eq!(
            full_results
                .into_iter()
                .map(|(_, (data, timestamp))| (timestamp, data.into_iter().collect::<HashSet<_>>()))
                .collect::<HashMap<_, _>>(),
            expected_full_results
        );
    }

    fn create_cube_and_assert_adaptive<C: Computation>(
        computation: &C,
        m: usize,
        n: usize,
        dir: &str,
        expected_diff_results: &[ExpectedResults<C>],
        specified_splits: &Option<SplitIndices>,
        expected_splits: &SplitIndices,
        index: usize,
    ) {
        let mut global_store = GlobalStore::default();

        let cube_name = "my_cube";
        let mut cube_query = format!(
            "load cube {name} {m} {n} from '{dir}' with prefix 'batch-0_';",
            name = cube_name,
            m = m,
            n = n,
            dir = dir,
        );
        process_query(&mut global_store, &mut cube_query).expect("Cube not loaded");

        let cube =
            global_store.filtered_cube_store.cubes.get_mut(cube_name).expect("Expected cube");

        cube.prepare_differential_data();

        let expected_diff_results = expected_diff_results
            .iter()
            .map(|(ts, values)| (*ts, values.iter().copied().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>();

        for &threads in &[1, 4] {
            let results = arranged_adaptive_run(
                CubePointer::new(&cube.data),
                specified_splits.clone(),
                computation.clone(),
                true,
                threads,
                0,
                &[],
                Some(2),
            )
            .expect("Computation failed");
            let (computed_results_arranged, _, obtained_splits) =
                process_results::<C>(results, true).expect("Processing failed");

            assert_results_match::<C>(
                computed_results_arranged,
                &expected_diff_results,
                &format!(
                    "Failed to match results of arranged computation using {} threads in {}",
                    threads, index
                ),
            );

            assert_eq!(&obtained_splits, expected_splits);
        }
    }

    fn assert_results_match<C: Computation>(
        obtained: DifferentialResults<C, GSTimestamp>,
        expected: &HashMap<GSTimestamp, HashSet<(C::Result, DiffCount)>>,
        msg: &str,
    ) {
        assert_eq!(
            &obtained
                .into_iter()
                .map(|(ts, values)| (ts, values.into_iter().collect::<HashSet<_>>()))
                .collect::<HashMap<_, _>>(),
            expected,
            "{}",
            msg,
        );
    }
}
