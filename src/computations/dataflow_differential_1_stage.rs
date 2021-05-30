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
use gs_analytics_api::{ComputationInput, ComputationRuntimeData, GsTs};
use hashbrown::HashMap;
use itertools::Itertools;
use log::info;
use std::fmt::Display;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

pub fn differential_run_1_stage<
    C: Computation,
    T: Timestamp + Refines<()> + Lattice + Display + Copy + GsTs,
>(
    cube_data: CubePointer<T>,
    computation: C,
    _starting_timestamp: T,
    runtime_data: &ComputationRuntimeData,
) -> Result<DifferentialRunOutput<C, T>, GSError> {
    print_memory_usage(format_args!("starting differential workers"));
    let worker_results = timely::execute(runtime_data.timely_config(), move |worker| {
        let worker_index = worker.index();
        let worker_count = worker.peers();
        let worker_timer = GsTimer::now();

        let mut probe = Handle::new();
        let (mut edge_input, mut edge_cap) = worker.dataflow(|scope| {
            let (edge_session, edge_stream) = scope.new_unordered_input();
            let edge_stream = edge_stream.as_collection();

            let forward_edges = edge_stream.arrange_by_key();
            let reverse_edges = forward_edges.as_collection(|&k, &v| (v, k)).arrange_by_key();
            let nodes = forward_edges
                .flat_map_ref(|src, dst| Some(*src).into_iter().chain(Some(*dst)))
                .distinct()
                .arrange_by_self();

            let input_stream = ComputationInput::new(nodes, forward_edges, reverse_edges);
            computation.graph_analytics(&input_stream).probe_with(&mut probe);

            edge_session
        });

        if worker_index == 0 {
            info!("Inserting data");
        }

        let mut all_times = Vec::new();
        for (_, timestamp, (_, data), _) in &cube_data.entries {
            let (left_index, right_index) =
                get_worker_indices(data.len(), worker_index, worker_count);
            let timer = GsTimer::now();
            {
                let mut session = edge_input.session(edge_cap.clone());
                for (edge, change) in &data[left_index..right_index] {
                    session.give((*edge, *timestamp, *change));
                }
            }
            let loaded = timer.elapsed();
            if worker_index == 0 {
                info!(
                    "[worker {:>2}] loaded {} diffs at timestamp {} in {}",
                    worker_index,
                    right_index - left_index,
                    *timestamp,
                    loaded.to_seconds_string()
                );
            }
            let timer2 = GsTimer::now();
            edge_cap.downgrade(&timestamp.next());
            while probe.less_than(edge_cap.time()) {
                worker.step();
            }
            let stable = timer2.elapsed();
            let total = loaded + stable;
            if worker_index == 0 {
                info!(
                    "[worker {:>2}] done with {} (next: {}) in {}",
                    worker_index,
                    *timestamp,
                    timestamp.next(),
                    total.to_seconds_string()
                );
            }
            all_times.push((*timestamp, (GsDuration::default(), loaded, stable, total)));
        }

        let worker_time = worker_timer.elapsed();
        if worker_index == 0 {
            info!("Workers finished in total {}", worker_time.to_seconds_string());
        }
        (
            HashMap::default(),
            all_times.into_iter().collect_vec(),
            worker_time,
            SplitIndices::default(),
        )
    })
    .map_err(GSError::Timely)?
    .join();
    print_memory_usage(format_args!("done with timely"));

    Ok(worker_results)
}
