use crate::computations::dataflow_arranged_adaptive::differential_run_adaptive;
use crate::computations::dataflow_differential_1_stage::differential_run_1_stage;
use crate::computations::dataflow_differential_2_stages::differential_run_2_stage;
use crate::filtered_cubes::materialise::{DiffEdgesPointer, DiffIteratorPointer};
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::CubePointer;
use crate::graph::properties::property_value::PropertyValue;
use crate::util::io::GsWriter;
use crate::util::timer::GsDuration;
use crate::{
    computations::dataflow_differential_basic::differential_run_basic,
    error::GSError,
    filtered_cubes::{materialise::CubeDiffIterators, FilteredCube},
    query_handler::GraphSurgeResult,
    util::timer::GsTimer,
};
use gs_analytics_api::{
    BasicComputation, ComputationRuntimeData, ComputationType, ComputationTypes, DiffCount,
    FilteredCubeData, GraphsurgeComputation, GsTimestampIndex, MaterializeResults,
    TimelyComputation,
};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use log::info;
use std::hash::Hash;

pub mod bfs;
pub mod builder;
pub mod dataflow_arranged_adaptive;
pub mod dataflow_differential_1_stage;
pub mod dataflow_differential_2_stages;
pub mod dataflow_differential_basic;
pub mod filtered_cubes;
pub mod pagerank;
pub mod scc;
pub mod spsp;
pub mod sssp;
pub mod views;
pub mod wcc;

#[cfg(test)]
mod tests;

pub type DifferentialRunOutput<C, T> =
    Vec<Result<(DifferentialResults<C, T>, Times<T>, GsDuration, SplitIndices), String>>;
pub type SplitIndices = HashSet<usize>;
pub type Times<T> = Vec<(T, (GsDuration, GsDuration, GsDuration, GsDuration))>;
pub type ProcessedResults<C> = (DifferentialResults<C, GSTimestamp>, TimeResults, SplitIndices);
pub type TimeResults = (HashMap<GSTimestamp, [Vec<GsDuration>; 4]>, Vec<GsDuration>);
pub type DifferentialResults<C, T> = HashMap<T, DifferentialResult<C>>;
pub type DifferentialResult<C> = Vec<(<C as ComputationTypes>::Result, DiffCount)>;
pub type ComputationResultSet<C> = HashSet<(<C as ComputationTypes>::Result, DiffCount)>;
pub type ComputationResults<C> = HashMap<GsTimestampIndex, (ComputationResultSet<C>, GSTimestamp)>;

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
            ComputationProperties::Pairs(_) => "Pairs".to_owned(),
        }
    }
}

pub trait Computation: BasicComputation + GraphsurgeComputation + TimelyComputation {
    fn execute(
        &self,
        cube: &mut FilteredCube,
        runtime_data: ComputationRuntimeData,
    ) -> Result<GraphSurgeResult, GSError> {
        match runtime_data.c_type {
            ComputationType::IndividualBasic
            | ComputationType::OneStageDifferential
            | ComputationType::TwoStageDifferential
            | ComputationType::Individual
            | ComputationType::Timely => (), // Nothing to do.
            ComputationType::Basic
            | ComputationType::Adaptive
            | ComputationType::CompareDifferential => {
                cube.prepare_differential_data();
            }
        }
        match runtime_data.c_type {
            ComputationType::Basic
            | ComputationType::OneStageDifferential
            | ComputationType::TwoStageDifferential
            | ComputationType::Adaptive => {
                differential_diff_execute(cube, self, &runtime_data)?;
            }
            ComputationType::IndividualBasic | ComputationType::Individual => {
                differential_individual_execute(cube, self, &runtime_data)?;
            }
            ComputationType::CompareDifferential => {
                return execute_compare_differential(&cube, self, &runtime_data)
            }
            ComputationType::Timely => timely_execute(&cube, self, &runtime_data)?,
        }
        Ok(GraphSurgeResult::new(format!("{} done", runtime_data.c_type.description())))
    }
}

impl<T: BasicComputation + GraphsurgeComputation + TimelyComputation> Computation for T {}

pub fn execute_compare_differential<T: Computation>(
    cube: &FilteredCube,
    computation: &T,
    runtime_data: &ComputationRuntimeData,
) -> Result<GraphSurgeResult, GSError> {
    let r1 = differential_diff_execute(cube, computation, runtime_data)?;
    let r2 = differential_individual_execute(cube, computation, runtime_data)?;
    compare::<T>(r1, r2)
}

fn compare<T: Computation>(
    mut r1: ComputationResults<T>,
    mut r2: ComputationResults<T>,
) -> Result<GraphSurgeResult, GSError> {
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
        Ok(GraphSurgeResult::new("Results match".to_owned()))
    } else {
        Err(GSError::ResultsMismatch)
    }
}

fn differential_individual_execute<C: Computation>(
    cube: &FilteredCube,
    computation: &C,
    runtime_data: &ComputationRuntimeData,
) -> Result<ComputationResults<C>, GSError>
where
    C::Result: Hash,
{
    info!("Running {}:", runtime_data.c_type.description());
    let timer = GsTimer::now();

    let mut full_results = Vec::new();
    let is_arranged = runtime_data.c_type == ComputationType::Individual;
    let mut total_worker_time = GsDuration::default();
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
            differential_run_2_stage(
                CubePointer::new(&cube_data),
                computation.clone(),
                fixed_timestamp,
                &runtime_data,
            )?
        } else {
            let diff_iterators = CubeDiffIterators::Inner(vec![(
                fixed_timestamp,
                next_timestamp,
                DiffEdgesPointer::new(&data),
            )]);
            differential_run_basic(
                DiffIteratorPointer::new(&diff_iterators),
                fixed_timestamp,
                computation.clone(),
                &runtime_data,
            )?
        };

        let (mut results, (mut all_times, worker_times), _) =
            process_results::<C>(results, materialize_results)?;

        let results = results
            .remove(&fixed_timestamp)
            .map_or_else(HashSet::new, |result| result.into_iter().collect::<HashSet<_>>());
        full_results.push((*timestamp_index, (results, *timestamp)));
        total_worker_time += worker_times.into_iter().max().expect("Max not found");
        merged_times.insert(*timestamp, all_times.remove(&fixed_timestamp).expect("TS missing"));
    }
    process_times(
        (merged_times, vec![total_worker_time]),
        fixed_timestamp,
        &cube,
        runtime_data.c_type.description(),
    );
    info!(
        "Done with {} in {}",
        runtime_data.c_type.description(),
        timer.elapsed().seconds_string()
    );
    if runtime_data.materialize_results != MaterializeResults::None {
        print_save_results::<C>(&full_results, &runtime_data.save_to)?;
    }

    Ok(full_results.into_iter().collect())
}

fn differential_diff_execute<C: Computation>(
    cube: &FilteredCube,
    computation: &C,
    runtime_data: &ComputationRuntimeData,
) -> Result<ComputationResults<C>, GSError>
where
    C::Result: Hash,
{
    info!("Running {}:", runtime_data.c_type.description());

    let timer = GsTimer::now();
    let zeroth_ts = GSTimestamp::get_zeroth_timestamp();
    let materialize_results = runtime_data.materialize_results != MaterializeResults::None;
    #[allow(clippy::wildcard_enum_match_arm)]
    let results = match runtime_data.c_type {
        ComputationType::Basic => {
            // high level operators + insert with incrementing timely timestamps.
            let diff_data = cube.differential_data.as_ref().expect("Should have been materialized");
            differential_run_basic(
                DiffIteratorPointer::new(&diff_data.cube_diff_iterators),
                zeroth_ts,
                computation.clone(),
                &runtime_data,
            )?
        }
        ComputationType::TwoStageDifferential => {
            // arranged operators + separate insert/computation stages + no timely timestamps.
            differential_run_2_stage(
                CubePointer::new(&cube.data),
                computation.clone(),
                zeroth_ts,
                &runtime_data,
            )?
        }
        ComputationType::OneStageDifferential => {
            // arranged operators + insert with incrementing timely timestamps.
            differential_run_1_stage(
                CubePointer::new(&cube.data),
                computation.clone(),
                zeroth_ts,
                &runtime_data,
            )?
        }
        ComputationType::Adaptive => {
            // arranged operators + insert/computation stages + no timely timestamps.
            differential_run_adaptive(
                CubePointer::new(&cube.data),
                computation.clone(),
                &runtime_data,
            )?
        }
        c => return Err(GSError::UnknownComputation(c.to_string())),
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
    process_times(all_times, zeroth_ts, &cube, runtime_data.c_type.description());
    info!(
        "Done with {} in {}",
        runtime_data.c_type.description(),
        timer.elapsed().seconds_string()
    );

    if let Some(save_path) = &runtime_data.save_to {
        info!("Writing diff results to '{}'...", save_path);
        let empty_vec = Vec::new();
        for (_, timestamp) in &cube.timestamp_mappings.0 {
            let data = diff_results.get(&timestamp).unwrap_or(&empty_vec); // Empty timestamps.
            info!("Writing {}, len={}", timestamp, data.len());
            let timestamp_path =
                format!("{}/results-diff-{}.txt", save_path, timestamp.get_str('_'));
            let mut writer = GsWriter::new(timestamp_path)?;
            writer.write_file_lines(data.iter().map(|(v, diff)| format!("{:?}, {:+}", v, diff)))?;
        }
    }

    let mut full_results = Vec::<(GsTimestampIndex, (ComputationResultSet<C>, GSTimestamp))>::new();
    if runtime_data.materialize_results == MaterializeResults::Full {
        info!("Materializing full results:");
        let timer = GsTimer::now();
        for (timestamp_index, (diff_neighbors, timestamp)) in
            cube.timestamp_mappings.0.iter().enumerate()
        {
            let diff_data = diff_results.remove(&timestamp).unwrap_or_default(); // Some timestamps might have 0 results.
            let full_result = if split_indices.contains(&timestamp_index) {
                diff_data.into_iter().collect()
            } else {
                let previous_results = {
                    // Declare a closure that obtains previous results for a given set of timestamps.
                    let get_previous_results = |data: &[GsTimestampIndex]| {
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
        info!("Results materialized in {}", timer.elapsed().seconds_string());
        print_save_results::<C>(&full_results, &runtime_data.save_to)?;
    }

    Ok(full_results.into_iter().collect())
}

fn timely_execute<C: Computation>(
    cube: &FilteredCube,
    computation: &C,
    runtime_data: &ComputationRuntimeData,
) -> Result<(), GSError>
where
    C::Result: Hash,
{
    if runtime_data.c_type != ComputationType::Timely {
        return Err(GSError::TypeMismatch("Timely".to_owned(), runtime_data.c_type.to_string()));
    }
    info!("Running {}:", runtime_data.c_type.description());

    let timer = GsTimer::now();
    let results = computation.timely_computation(&cube.data, runtime_data);

    info!(
        "Done with {} in {}",
        runtime_data.c_type.description(),
        timer.elapsed().seconds_string()
    );

    if let Some(save_path) = &runtime_data.save_to {
        info!("Writing diff results to '{}'...", save_path);
        let empty_vec = Vec::new();
        for (_, timestamp) in &cube.timestamp_mappings.0 {
            let data = results.get(timestamp).unwrap_or(&empty_vec); // Empty timestamps.
            info!("Writing {}, len={}", timestamp, data.len());
            let timestamp_path =
                format!("{}/results-diff-{}.txt", save_path, timestamp.get_str('_'));
            let mut writer = GsWriter::new(timestamp_path)?;
            writer.write_file_lines(data.iter().map(|v| format!("{:?}", v)))?;
        }
    }

    Ok(())
}

fn process_results<C: ComputationTypes>(
    worker_results: DifferentialRunOutput<C, GSTimestamp>,
    materialize_results: bool,
) -> Result<ProcessedResults<C>, GSError> {
    let mut final_results = HashMap::new();
    let mut merged_times = HashMap::new();
    let mut total_times = Vec::new();
    let mut merged_results = HashMap::new();
    let mut all_split_indices = HashSet::new();

    for (index, worker_result) in worker_results.into_iter().enumerate() {
        let (results, times, total_time, split_indices) =
            worker_result.map_err(GSError::TimelyResults)?;
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
    let mut all_times = [[GsDuration::default(); 4]; 3];
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
            loaded_max.seconds_string(),
            stable_max.seconds_string(),
            total_max.seconds_string()
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
        total_times.iter().max().expect("Max not found").seconds_string()
    );
    for (max_times, strn) in all_times.iter().zip_eq(["initial", "rest", "total"].iter()) {
        for (max_time, strn2) in
            max_times.iter().zip_eq(["merged", "loaded", "stable", "total"].iter())
        {
            info!("Computation of {} {:>7} {:>6}: {}", msg, strn, strn2, max_time.seconds_string());
        }
    }
}

/// Common function to print full results and optionally save the results to disk.
fn print_save_results<T: Computation>(
    full_results: &[(GsTimestampIndex, (ComputationResultSet<T>, GSTimestamp))],
    save_to: &Option<String>,
) -> Result<(), GSError> {
    for (_, (results, timestamp)) in full_results {
        info!("{}: {} results", timestamp, results.len());
    }

    if let Some(save_path) = save_to {
        info!("Writing materialized results to '{}'...", save_path);
        for (_, (results, timestamp)) in full_results {
            info!("Writing {}, len={}", timestamp, results.len());
            let timestamp_path =
                format!("{}/results-full-{}.txt", save_path, timestamp.get_str('_'));
            let mut writer = GsWriter::new(timestamp_path)?;
            writer.write_file_lines(
                results.iter().map(|(data, change)| format!("{:?}, {:+}", data, change)),
            )?;
        }
    }

    Ok(())
}
