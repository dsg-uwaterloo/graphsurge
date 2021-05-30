use crate::computations::{Computation, DifferentialRunOutput, SplitIndices};
use crate::computations::{DifferentialResults, Times};
use crate::error::GSError;
use crate::filtered_cubes::materialise::{CubeDiffIterators, DiffIteratorPointer};
use crate::graph::stream_data::get_worker_indices;
use crate::util::memory_usage::print_memory_usage;
use crate::util::timer::{GsDuration, GsTimer};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;
use gs_analytics_api::{ComputationRuntimeData, DiffCount, SimpleEdge};
use hashbrown::HashMap;
use log::info;
use std::fmt::Display;
use std::sync::mpsc::Receiver;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::event::Event::Messages;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::unordered_input::ActivateCapability;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Configuration;

type ResultStream<D, T> = Receiver<Event<T, (D, T, DiffCount)>>;

pub fn differential_run_basic<
    C: Computation,
    T: Timestamp + Refines<()> + Lattice + Display + Copy,
>(
    diff_iterators: DiffIteratorPointer<T>,
    starting_timestamp: T,
    computation: C,
    runtime_data: &ComputationRuntimeData,
) -> Result<DifferentialRunOutput<C, T>, GSError> {
    let materialize_results = runtime_data.should_materialize_results();
    let threads = runtime_data.threads;
    print_memory_usage(format_args!("starting differential workers"));
    let worker_results = timely::execute(Configuration::Process(threads), move |mut worker| {
        let worker_index = worker.index();
        let worker_count = worker.peers();
        let timer = GsTimer::now();

        let mut probe = Handle::new();
        let (mut edge_input, mut edge_cap, result_stream) = worker.dataflow(|scope| {
            let ((edge_input, edge_cap), edges) = scope.new_unordered_input();
            let edges = edges.as_collection();

            let result = computation.basic_computation(&edges).probe_with(&mut probe);

            (
                edge_input,
                edge_cap,
                if materialize_results { Some(result.inner.capture()) } else { None },
            )
        });

        edge_cap.downgrade(&starting_timestamp);

        let mut all_times = Vec::new();
        let mut results = HashMap::new();
        insert_edges::<_, C, T>(
            &*diff_iterators,
            &mut edge_input,
            edge_cap,
            &mut worker,
            &probe,
            &result_stream,
            &mut results,
            worker_index,
            worker_count,
            materialize_results,
            &mut all_times,
        );

        let worker_time = timer.elapsed();
        info!("Worker {:>2} finished in total {}", worker_index, worker_time.to_seconds_string());

        (results, all_times, worker_time, SplitIndices::default())
    })
    .map_err(GSError::Timely)?
    .join();
    print_memory_usage(format_args!("done with timely"));

    Ok(worker_results)
}

fn insert_edges<A: Allocate, C: Computation, T: Timestamp + Display + Copy>(
    edges: &CubeDiffIterators<T>,
    edge_input: &mut UnorderedHandle<T, (SimpleEdge, T, DiffCount)>,
    mut edge_cap: ActivateCapability<T>,
    worker: &mut Worker<A>,
    probe: &Handle<T>,
    result_stream: &Option<ResultStream<C::Result, T>>,
    results: &mut DifferentialResults<C, T>,
    worker_index: usize,
    worker_count: usize,
    materialize_results: bool,
    all_times: &mut Times<T>,
) {
    match edges {
        CubeDiffIterators::Outer(outer) => {
            for (next_timestamp, row) in outer.iter() {
                let edge_cap_next = edge_cap.delayed(next_timestamp);
                if worker_index == 0 {
                    info!("Preparing for {}", next_timestamp);
                }
                insert_edges::<_, C, T>(
                    row,
                    edge_input,
                    edge_cap,
                    worker,
                    probe,
                    result_stream,
                    results,
                    worker_index,
                    worker_count,
                    materialize_results,
                    all_times,
                );
                edge_cap = edge_cap_next;
            }
        }
        CubeDiffIterators::Inner(inner) => {
            for (current_timestamp, next_timestamp, diff) in inner.iter() {
                info!("[worker {:>2}] running {}", worker_index, current_timestamp);
                if worker_index == 0 {
                    print_memory_usage(format_args!("running {}", current_timestamp));
                }
                let (left_index, right_index) =
                    get_worker_indices(diff.len(), worker_index, worker_count);
                let timer = GsTimer::now();
                {
                    let mut session = edge_input.session(edge_cap.clone());
                    for (edge, change) in &diff[left_index..right_index] {
                        session.give((*edge, *current_timestamp, *change));
                    }
                }
                let loaded = timer.elapsed();
                info!(
                    "[worker {:>2}] loaded {} diffs [{:?}] at timestamp {} in {}",
                    worker_index,
                    right_index - left_index,
                    diff[left_index],
                    current_timestamp,
                    loaded.to_seconds_string()
                );
                if worker_index == 0 {
                    print_memory_usage(format_args!("loaded {}", current_timestamp));
                    info!("Downgrading to {}", next_timestamp);
                }
                let timer2 = GsTimer::now();
                edge_cap.downgrade(&next_timestamp);
                while probe.less_than(edge_cap.time()) {
                    worker.step();
                }
                let stable = timer2.elapsed();
                let total = loaded + stable;
                info!(
                    "[worker {:>2}] done with {} (next: {}) in {}",
                    worker_index,
                    current_timestamp,
                    next_timestamp,
                    total.to_seconds_string()
                );
                if worker_index == 0 {
                    print_memory_usage(format_args!("computer {}", current_timestamp));
                }
                all_times
                    .push((*current_timestamp, (GsDuration::default(), loaded, stable, total)));

                if materialize_results {
                    for r in result_stream.as_ref().expect("Result stream missing").try_iter() {
                        if let Messages(timestamp, entries) = r {
                            results.entry(timestamp).or_insert_with(Vec::new).extend(
                                entries.into_iter().map(|(data, _, change)| (data, change)),
                            );
                        }
                    }
                }
            }
        }
    }
}
