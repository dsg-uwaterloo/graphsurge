use crate::computations::{Computation, DifferentialRunOutput, SplitIndices};
use crate::error::GSError;
use crate::filtered_cubes::CubePointer;
use crate::graph::stream_data::get_worker_indices;
use crate::util::memory_usage::print_memory_usage;
use crate::util::timer::{GsDuration, GsTimer};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::Threshold;
use differential_dataflow::AsCollection;
use gs_analytics_api::{ComputationInput, ComputationRuntimeData};
use hashbrown::HashMap;
use itertools::Itertools;
use log::info;
use std::fmt::Display;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::event::Event::Messages;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::operators::Exchange;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

pub fn differential_run_2_stage<
    C: Computation,
    T: Timestamp + Refines<()> + Lattice + Display + Copy,
>(
    cube_data: CubePointer<T>,
    computation: C,
    starting_timestamp: T,
    runtime_data: &ComputationRuntimeData,
) -> Result<DifferentialRunOutput<C, T>, GSError> {
    print_memory_usage(format_args!("starting differential workers"));
    let materialize_results = runtime_data.should_materialize_results();
    let threads = runtime_data.threads;
    let worker_results = timely::execute(runtime_data.timely_config(), move |worker| {
        let worker_index = worker.index();
        let worker_count = worker.peers();
        let worker_timer = GsTimer::now();

        let ((mut edge_input, edge_cap), mut forward_edges, mut reverse_edges, mut nodes) = worker
            .dataflow(|scope| {
                let (edge_session, edge_stream) = scope.new_unordered_input();
                let edge_stream = edge_stream.as_collection();

                let forward_edges = edge_stream.arrange_by_key();
                let reverse_edges = forward_edges.as_collection(|&k, &v| (v, k)).arrange_by_key();
                let nodes = forward_edges
                    .flat_map_ref(|src, dst| Some(*src).into_iter().chain(Some(*dst)))
                    .distinct()
                    .arrange_by_self();

                (edge_session, forward_edges.trace, reverse_edges.trace, nodes.trace)
            });

        if worker_index == 0 {
            info!("Inserting data");
        }

        let mut all_times = HashMap::new();
        let timer = GsTimer::now();
        {
            let mut session = edge_input.session(edge_cap);
            for (_, timestamp, (_, data), _) in &cube_data.entries {
                let (left_index, right_index) =
                    get_worker_indices(data.len(), worker_index, worker_count);
                for (edge, change) in &data[left_index..right_index] {
                    session.give((*edge, *timestamp, *change));
                }
                if worker_index == 0 {
                    info!(
                        "[worker {:>2}] loaded {} diffs at timestamp {} in {}",
                        worker_index,
                        right_index - left_index,
                        timestamp,
                        timer.elapsed().to_seconds_string()
                    );
                }
                all_times.insert(*timestamp, Default::default());
            }
        }
        while worker.step() {}
        let load_time = timer.elapsed();
        if worker_index == 0 {
            info!(
                "[worker {:>2}] All data loaded in {}",
                worker_index,
                load_time.to_seconds_string()
            );
            print_memory_usage(format_args!("data loaded"));
        }

        let result_stream = worker.dataflow::<T, _, _>(|scope| {
            let nodes = nodes.import(scope);
            let forward = forward_edges.import(scope);
            let reverse = reverse_edges.import(scope);
            let input_stream = ComputationInput::new(nodes, forward, reverse);

            let results = computation.graph_analytics(&input_stream);

            if materialize_results {
                let stream = results.inner.exchange(move |_| {
                    if threads == 1 {
                        0
                    } else {
                        (worker_index % threads) as u64
                    }
                });
                Some(stream.capture())
            } else {
                None
            }
        });

        let timer = GsTimer::now();
        while worker.step() {}
        let comp_time = timer.elapsed();
        if worker_index == 0 {
            info!("Computation finished in {}", comp_time.to_seconds_string());
        }

        let mut results = HashMap::new();
        if materialize_results {
            for r in result_stream.expect("Results stream missing").try_iter() {
                if let Messages(_, entries) = r {
                    for (data, ts, change) in entries {
                        results.entry(ts).or_insert_with(Vec::new).push((data, change));
                    }
                }
            }
        }

        let worker_time = worker_timer.elapsed();
        if worker_index == 0 {
            info!("Workers finished in total {}", worker_time.to_seconds_string());
        }

        all_times.insert(
            starting_timestamp,
            (GsDuration::default(), load_time, comp_time, load_time + comp_time),
        );
        (results, all_times.into_iter().collect_vec(), worker_time, SplitIndices::default())
    })
    .map_err(GSError::Timely)?
    .join();
    print_memory_usage(format_args!("done with timely"));

    Ok(worker_results)
}
