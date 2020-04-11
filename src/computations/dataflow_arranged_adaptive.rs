#![allow(clippy::cast_precision_loss)]

use crate::computations::{Computation, DifferentialRunOutput, SplitIndices};
use crate::error::computation_error;
use crate::error::GraphSurgeError;
use crate::filtered_cubes::timestamp::NextTS;
use crate::filtered_cubes::CubePointer;
use crate::graph::stream_data::get_worker_indices;
use crate::util::memory_usage::print_memory_usage;
use crate::util::timer::{GSDuration, GSTimer};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::Threshold;
use differential_dataflow::AsCollection;
use gs_analytics_api::ComputationInput;
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use log::info;
use serde::export::fmt::Display;
use std::convert::TryFrom;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::event::Event::Messages;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::Configuration;

const BATCH: usize = 10;
const MULTIPLIER: f64 = 1.1;

pub fn arranged_adaptive_run<
    C: Computation,
    T: Timestamp + NextTS + Refines<()> + Lattice + Display + Copy,
>(
    cube_data: CubePointer<T>,
    pre_specified_splits: Option<SplitIndices>,
    computation: C,
    materialize_results: bool,
    threads: usize,
    _process_id: usize,
    _hosts: &[String],
    batch_size: Option<usize>,
) -> Result<DifferentialRunOutput<C, T>, GraphSurgeError> {
    print_memory_usage(format_args!("starting timely"));

    // Create channels for worker 0 to send decisions to the other workers.
    let channels = (0..(threads - 1)).map(|_| crossbeam_channel::bounded(1)).collect_vec();

    let worker_results = timely::execute(Configuration::Process(threads), move |worker| {
        let worker_index = worker.index();
        let worker_count = worker.peers();
        let mut all_times = HashMap::new();
        let mut actual_splits = HashSet::new();
        let mut individual_data = (Vec::new(), Vec::new());
        let mut diff_data = (Vec::new(), Vec::new());
        let mut results = HashMap::new();
        let timer = GSTimer::now();

        let mut loop_indv = Some(0);
        let mut loop_diffs = Some((1, 2));
        let mut index_consider = 2;

        'outer: loop {
            let mut probe = Handle::new();
            let (mut edge_input, mut edge_cap, result_stream) =
                worker.dataflow::<T, _, _>(|scope| {
                    let (edge_session, edge_stream) = scope.new_unordered_input();
                    let edge_stream = edge_stream.as_collection();

                    let forward_edges = edge_stream.arrange_by_key();
                    let reverse_edges =
                        forward_edges.as_collection(|&k, &v| (v, k)).arrange_by_key();
                    let nodes = forward_edges
                        .flat_map_ref(|src, dst| Some(*src).into_iter().chain(Some(*dst)))
                        .distinct()
                        .arrange_by_self();
                    let input_stream = ComputationInput::new(nodes, forward_edges, reverse_edges);

                    let results = computation.graph_analytics(&input_stream).probe_with(&mut probe);

                    (
                        edge_session.0,
                        edge_session.1,
                        if materialize_results { Some(results.inner.capture()) } else { None },
                    )
                });

            if let Some(indv_index) = loop_indv {
                let (_, view_timestamp, (_, _), _) = &cube_data.entries[indv_index];
                edge_cap.downgrade(&view_timestamp);
                if worker_index == 0 {
                    info!("Running {} as {}", view_timestamp, view_timestamp);
                }
                let timer = GSTimer::now();
                let mut count: isize = 0;
                let mut diffs = 0;
                {
                    let mut session = edge_input.session(edge_cap.clone());
                    for (_, _, (_, diff_data), (adds, dels)) in &cube_data.entries[..=indv_index] {
                        count += isize::try_from(*adds).expect("overflow")
                            - isize::try_from(*dels).expect("overflow");
                        let (left_index, right_index) =
                            get_worker_indices(diff_data.len(), worker_index, worker_count);
                        diffs += right_index - left_index;
                        {
                            for (edge, change) in &diff_data[left_index..right_index] {
                                session.give((*edge, *view_timestamp, *change));
                            }
                        }
                    }
                }
                let loaded = timer.elapsed();
                if worker_index == 0 {
                    info!(
                        "[worker {:>2}] individual loaded {} diffs at timestamp {} (as {}) in {}",
                        worker_index,
                        diffs,
                        view_timestamp,
                        view_timestamp,
                        loaded.to_seconds_string()
                    );
                }
                let next_timestamp = view_timestamp.next();
                let timer2 = GSTimer::now();
                edge_cap.downgrade(&next_timestamp);
                while probe.less_than(edge_cap.time()) {
                    worker.step();
                }
                let stable = timer2.elapsed();
                let total = loaded + stable;
                if worker_index == 0 {
                    info!(
                        "Done with {} (as {}, next {}) in {}",
                        view_timestamp,
                        view_timestamp,
                        next_timestamp,
                        total.to_seconds_string()
                    );
                }
                all_times.insert(*view_timestamp, (GSDuration::default(), loaded, stable, total));
                let runtime = total.as_secs_f64();
                individual_data.0.push(count as LRDataPoint);
                individual_data.1.push(runtime);
                if worker_index == 0 {
                    info!("ML result: index = {} : {} -> {:.3}", indv_index, count, runtime);
                }

                if materialize_results {
                    for r in result_stream.as_ref().expect("Result stream missing").try_iter() {
                        if let Messages(_, entries) = r {
                            // println!("{} -> {:?}", view_timestamp, entries);
                            for (data, ts, change) in entries {
                                results.entry(ts).or_insert_with(Vec::new).push((data, change));
                            }
                        }
                    }
                }
            }

            if let Some((diff_start, diff_end)) = loop_diffs {
                let timer = GSTimer::now();
                let mut count = 0;
                let (_, start_ts, (_, _), _) = &cube_data.entries[diff_start];
                let (_, end_ts, (_, _), _) = &cube_data.entries[diff_end - 1];
                edge_cap.downgrade(&start_ts);
                {
                    let mut session = edge_input.session(edge_cap.clone());
                    for (_, view_timestamp, (_, diff_data), (adds, dels)) in
                        &cube_data.entries[diff_start..diff_end]
                    {
                        if worker_index == 0 {
                            info!("diffs Running {} as {}", view_timestamp, start_ts);
                        }
                        all_times.insert(*view_timestamp, Default::default());
                        count += *adds + *dels;
                        let (left_index, right_index) =
                            get_worker_indices(diff_data.len(), worker_index, worker_count);
                        {
                            for (edge, change) in &diff_data[left_index..right_index] {
                                session.give((*edge, *view_timestamp, *change));
                            }
                        }
                        if worker_index == 0 {
                            info!(
                                "[worker {:>2}] diffs loaded {} diffs at timestamp {} \
                                (as {}) in {}",
                                worker_index,
                                right_index - left_index,
                                view_timestamp,
                                start_ts,
                                timer.elapsed().to_seconds_string()
                            );
                        }
                    }
                }
                let loaded = timer.elapsed();
                let next_timestamp = end_ts.next();
                let timer2 = GSTimer::now();
                edge_cap.downgrade(&next_timestamp);
                while probe.less_than(edge_cap.time()) {
                    worker.step();
                }
                let stable = timer2.elapsed();
                let total = loaded + stable;
                if worker_index == 0 {
                    info!(
                        "diffs: Done with {}..{} (as {}, next {}) in {}",
                        start_ts,
                        end_ts,
                        start_ts,
                        next_timestamp,
                        total.to_seconds_string()
                    );
                }
                all_times.insert(*start_ts, (GSDuration::default(), loaded, stable, total));
                let runtime = total.as_secs_f64();
                diff_data.0.push(count as LRDataPoint);
                diff_data.1.push(runtime);
                if worker_index == 0 {
                    info!(
                        "ML result: index = {}..{} : {} -> {:.3}",
                        diff_start, diff_end, count, runtime
                    );
                }

                if materialize_results {
                    for r in result_stream.as_ref().expect("Result stream missing").try_iter() {
                        if let Messages(_, entries) = r {
                            // println!("{} -> {:?}", view_timestamp, entries);
                            for (data, ts, change) in entries {
                                results.entry(ts).or_insert_with(Vec::new).push((data, change));
                            }
                        }
                    }
                }
            }

            loop {
                let start = index_consider;
                let mut end = index_consider + batch_size.unwrap_or(BATCH);
                if end > cube_data.entries.len() {
                    end = cube_data.entries.len();
                }
                let decisions = if worker_index == 0 {
                    let mut running_total: isize = cube_data.entries.iter().take(start).fold(
                        0,
                        |acc, (_, _, (_, _), (additions, deletions))| {
                            acc + isize::try_from(*additions).expect("Overflow")
                                - isize::try_from(*deletions).expect("Overflow")
                        },
                    );
                    let mut decisions = Vec::new();
                    for index in start..end {
                        let (_, _, (_, _), (additions, deletions)) = &cube_data.entries[index];
                        let total = isize::try_from(*additions + *deletions).expect("Overflow");
                        running_total += isize::try_from(*additions).expect("Overflow")
                            - isize::try_from(*deletions).expect("Overflow");
                        let decision = if let Some(specified_splits) = &pre_specified_splits {
                            let decision = specified_splits.contains(&index);
                            info!(
                                "ML result: index = {} diffp = {} indvp = {} \
                                pre_decided_split = {}",
                                index, total, running_total, decision,
                            );
                            decision
                        } else {
                            let closure =
                                |totals: &[LRDataPoint],
                                 runtimes: &[LRDataPoint],
                                 new_total: isize| {
                                    let mut lr = LinearRegression::new();
                                    lr.fit(totals, runtimes);
                                    let mut prediction = lr.predict(new_total as LRDataPoint);
                                    let mut is_avg = false;
                                    if !prediction.is_finite() {
                                        is_avg = true;
                                        let avg_runtime = totals
                                            .iter()
                                            .zip_eq(runtimes.iter())
                                            .fold(0.0, |acc, (total, runtime)| {
                                                acc + (runtime / total)
                                            })
                                            / totals.len() as LRDataPoint;
                                        prediction = avg_runtime * (new_total as LRDataPoint);
                                    }
                                    (prediction, is_avg)
                                };

                            let (predicted_diff_time, _is_avg_diff) =
                                closure(&diff_data.0, &diff_data.1, total);
                            let (predicted_indv_time, _is_avg_indv) =
                                closure(&individual_data.0, &individual_data.1, running_total);
                            let decision = predicted_indv_time < predicted_diff_time * MULTIPLIER;
                            info!(
                                "ML result: index = {} diffp = {} predict = {:.3} indvp = {} \
                                predict = {:.3} decision_should_split = {}",
                                index,
                                total,
                                predicted_diff_time,
                                running_total,
                                predicted_indv_time,
                                decision,
                            );
                            decision
                        };
                        if decision {
                            actual_splits.insert(index);
                        }
                        decisions.push(decision);
                    }
                    for (sender, _) in &channels {
                        sender.send(decisions.clone()).expect("Error sending msg");
                    }
                    decisions
                } else {
                    let (_, receiver) = &channels[worker_index - 1];
                    receiver.recv().expect("Error receiving message")
                };

                let mut index = 0;
                while index < decisions.len() && !decisions[index] {
                    index += 1;
                }

                if index > 0 {
                    let timer = GSTimer::now();
                    let mut count = 0;
                    let diff_start = index_consider;
                    let diff_end = index_consider + index;
                    let (_, start_ts, (_, _), _) = &cube_data.entries[diff_start];
                    let (_, end_ts, (_, _), _) = &cube_data.entries[diff_end - 1];
                    edge_cap.downgrade(&start_ts);
                    {
                        let mut session = edge_input.session(edge_cap.clone());
                        for (_, view_timestamp, (_, diff_data), (adds, dels)) in
                            &cube_data.entries[diff_start..diff_end]
                        {
                            if worker_index == 0 {
                                info!("diffs2: Running {} as {}", view_timestamp, start_ts);
                            }
                            all_times.insert(*view_timestamp, Default::default());
                            count += *adds + *dels;
                            let (left_index, right_index) =
                                get_worker_indices(diff_data.len(), worker_index, worker_count);
                            {
                                for (edge, change) in &diff_data[left_index..right_index] {
                                    session.give((*edge, *view_timestamp, *change));
                                }
                            }
                            if worker_index == 0 {
                                info!(
                                    "[worker {:>2}] diffs2 loaded {} diffs at timestamp {}\
                                    (as {}) in {}",
                                    worker_index,
                                    right_index - left_index,
                                    view_timestamp,
                                    start_ts,
                                    timer.elapsed().to_seconds_string()
                                );
                            }
                        }
                    }
                    let loaded = timer.elapsed();
                    let next_timestamp = end_ts.next();
                    let timer2 = GSTimer::now();
                    edge_cap.downgrade(&next_timestamp);
                    while probe.less_than(edge_cap.time()) {
                        worker.step();
                    }
                    let stable = timer2.elapsed();
                    let total = loaded + stable;
                    if worker_index == 0 {
                        info!(
                            "[worker {:>2}] diffs2 done with {}..{} (as {}, next {}) in {}",
                            worker_index,
                            start_ts,
                            end_ts,
                            start_ts,
                            next_timestamp,
                            total.to_seconds_string()
                        );
                    }
                    all_times.insert(*start_ts, (GSDuration::default(), loaded, stable, total));
                    let runtime = total.as_secs_f64();
                    diff_data.0.push(count as LRDataPoint);
                    diff_data.1.push(runtime);
                    if worker_index == 0 {
                        info!(
                            "ML result: index = {}..{} : {} -> {:.3}",
                            diff_start, diff_end, count, runtime
                        );
                    }

                    if materialize_results {
                        for r in result_stream.as_ref().expect("Result stream missing").try_iter() {
                            if let Messages(_, entries) = r {
                                // println!("{} -> {:?}", view_timestamp, entries);
                                for (data, ts, change) in entries {
                                    results.entry(ts).or_insert_with(Vec::new).push((data, change));
                                }
                            }
                        }
                    }
                }

                index_consider += index;
                if index_consider >= cube_data.entries.len() {
                    break 'outer;
                }
                if index < decisions.len() {
                    loop_indv = Some(index_consider);
                    loop_diffs = None;
                    index_consider += 1;
                    continue 'outer;
                }
            }
        }

        let worker_time = timer.elapsed();
        info!("Worker {:>2} finished in total {}", worker_index, worker_time.to_seconds_string());
        (results, all_times.into_iter().collect_vec(), worker_time, actual_splits)
    })
    .map_err(|e| computation_error(format!("Timely error: {:?}", e)))?
    .join();
    print_memory_usage(format_args!("done with timely"));

    Ok(worker_results)
}

pub type LRDataPoint = f64;

#[derive(Default)]
pub struct LinearRegression {
    pub coefficient: Option<LRDataPoint>,
    pub intercept: Option<LRDataPoint>,
}

impl LinearRegression {
    pub fn new() -> LinearRegression {
        LinearRegression::default()
    }

    pub fn fit(&mut self, x_values: &[LRDataPoint], y_values: &[LRDataPoint]) {
        let x = covariance(x_values, y_values);
        let y = variance(x_values);
        let b1 = x / y;
        let b0 = mean(y_values) - b1 * mean(x_values);

        self.intercept = Some(b0);
        self.coefficient = Some(b1);
    }

    pub fn predict(&self, x: LRDataPoint) -> LRDataPoint {
        if self.coefficient.is_none() || self.intercept.is_none() {
            panic!("fit(..) must be called first");
        }

        let b0 = self.intercept.expect("intercept");
        let b1 = self.coefficient.expect("coefficient");

        b0 + b1 * x
    }

    pub fn predict_list(&self, x_values: &[LRDataPoint]) -> Vec<LRDataPoint> {
        let mut predictions = Vec::new();

        for x in x_values.iter() {
            predictions.push(self.predict(*x));
        }

        predictions
    }

    pub fn evaluate(&self, x_test: &[LRDataPoint], y_test: &[LRDataPoint]) -> LRDataPoint {
        if self.coefficient.is_none() || self.intercept.is_none() {
            panic!("fit(..) must be called first");
        }

        let y_predicted = self.predict_list(x_test);
        Self::root_mean_squared_error(y_test, &y_predicted)
    }

    fn root_mean_squared_error(actual: &[LRDataPoint], predicted: &[LRDataPoint]) -> LRDataPoint {
        let mut sum_error = 0.0;
        let length = actual.len();

        for i in 0..length {
            sum_error += LRDataPoint::powf(predicted[i] - actual[i], 2.0);
        }

        let mean_error = sum_error / length as LRDataPoint;
        mean_error.sqrt()
    }
}

pub fn mean(values: &[LRDataPoint]) -> LRDataPoint {
    if values.is_empty() {
        return 0.0;
    }

    values.iter().sum::<LRDataPoint>() / (values.len() as LRDataPoint)
}

pub fn variance(values: &[LRDataPoint]) -> LRDataPoint {
    if values.is_empty() {
        return 0.0;
    }

    let mean = mean(values);
    values.iter().map(|x| LRDataPoint::powf(x - mean, 2.0)).sum::<LRDataPoint>()
        / values.len() as LRDataPoint
}

pub fn covariance(x_values: &[LRDataPoint], y_values: &[LRDataPoint]) -> LRDataPoint {
    if x_values.len() != y_values.len() {
        panic!("x_values and y_values must be of equal length.");
    }

    let length: usize = x_values.len();

    if length == 0_usize {
        return 0.0;
    }

    let mut covariance: LRDataPoint = 0.0;
    let mean_x = mean(x_values);
    //println!("mean_x = {}", mean_x);
    let mean_y = mean(y_values);
    //println!("mean_y = {}", mean_y);

    for i in 0..length {
        covariance += (x_values[i] - mean_x) * (y_values[i] - mean_y)
    }

    covariance / length as LRDataPoint
}
