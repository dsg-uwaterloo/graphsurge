use crate::computations::filtered_cubes::filter_matrix::FilteredMatrix;
use crate::computations::filtered_cubes::matrix_operation::MatrixOperation;
use crate::computations::filtered_cubes::optimal_orders::OptimalOrder;
use crate::computations::filtered_cubes::process_edge_diff::EdgeDiff;
use crate::computations::filtered_cubes::reduce_matrices::ReduceMatrices;
use crate::computations::filtered_cubes::{DiffProcessingData, DimensionOrder};
use crate::computations::views::monitor::MonitorStream;
use crate::error::GSError;
use crate::filtered_cubes::timestamp::timestamp_mappings::{
    get_timestamp_mappings, TimestampMappings,
};
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::{DimensionLength, DimensionLengths};
use crate::graph::stream_data::get_timely_edgeid_stream;
use crate::graph::Graph;
use crate::graph::GraphPointer;
use crate::query_handler::create_filtered_cube::Dimension;
use crate::util::memory_usage::print_memory_usage;
use crossbeam_utils::thread;
use hashbrown::HashMap;
use itertools::Itertools;
use log::info;

use gs_analytics_api::CubeDataEntries;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::event::Event::Messages;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::exchange::Exchange;
#[allow(unused_imports)]
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::Configuration;

pub fn execute(
    dimensions: Vec<Dimension>,
    dimension_lengths: DimensionLengths,
    graph: &Graph,
    manual_order: bool,
    store_total_data: bool,
    timestamp_mappings: &TimestampMappings,
    threads_per_process: usize,
    process_id: usize,
    hosts: &[String],
) -> Result<Vec<CubeDataEntries<GSTimestamp>>, GSError> {
    let graph_pointer = GraphPointer::new(&graph);

    let config = if hosts.len() > 1 {
        let c_hosts = hosts.to_vec();
        info!("Process {} w/ {} threads, hosts = {:?}", process_id, threads_per_process, c_hosts);
        Configuration::Cluster {
            threads: threads_per_process,
            process: process_id,
            addresses: c_hosts,
            report: false,
            log_fn: Box::new(|_| None),
        }
    } else {
        Configuration::Process(threads_per_process)
    };

    info!("Starting execution for new filtered cube...");
    print_memory_usage(format_args!("starting differential workers"));
    let worker_threads = timely::execute(config, move |worker| {
        let timer = std::time::Instant::now();

        let mut edge_input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let worker_index = worker.index();
        let worker_count = worker.peers();

        let (filtered_edge_stream, order_stream) = worker.dataflow(|scope| {
            let edge_stream = edge_input.to_stream(scope);

            let filtered_matrix_stream = edge_stream
                .filtered_matrix(dimensions.clone(), graph_pointer)
                .monitor(500_000, "filtered_edge_stream", worker_index);

            let order_stream =
                if manual_order || dimension_lengths.iter().all(|&length| length < 3) {
                    // If requested or all lengths are less than 3, order can be simply
                    // specified in order instead of a costly computation.
                    //
                    // Only do this for worker 0 to mirror the reduce operation in the else
                    // case that generates the final orders.
                    if worker_index == 0 {
                        vec![dimension_lengths
                            .iter()
                            .map(|&length| (1..=length).collect_vec())
                            .collect()]
                    } else {
                        vec![]
                    }
                    .into_iter()
                    .to_stream(scope)
                } else {
                    // Add 1 to all dimension lengths to account for the extra 0 added to all edges.
                    let dimension_lengths_plus_one =
                        dimension_lengths.iter().map(|length| length + 1).collect_vec();
                    filtered_matrix_stream
                            .create_ordering_matrices(dimension_lengths_plus_one.clone(),worker_index)
                            .exchange(|_| 0_u64) // Send all matrices to worker 0.
                            .reduce_matrices(dimension_lengths_plus_one,worker_index)
                            .generate_optimal_orders()
                }
                .flat_map(|orders| orders.into_iter().enumerate())
                .inspect(move |(index, order)| {
                    if worker_index == 0 {
                        info!("order {} = {:?}", index, order);
                    }
                })
                // Send all matrices to worker 0.
                .broadcast();
            filtered_matrix_stream.probe_with(&mut probe);
            order_stream.probe_with(&mut probe);

            (filtered_matrix_stream.capture(), order_stream.capture())
        });

        for edgeid in get_timely_edgeid_stream(&*graph_pointer, worker_index, worker_count) {
            edge_input.send(edgeid);
        }
        edge_input.close();
        if worker_index == 0 {
            print_memory_usage(format_args!("loaded edges"));
        }

        while !probe.done() {
            worker.step();
        }

        if worker_index == 0 {
            info!("Done on worker {:>2} in {:?}. Processing diffs...", worker_index, timer.elapsed());
            print_memory_usage(format_args!("processed edges"));
        }

        let mut orders = (0..dimensions.len()).map(|_| Vec::new()).collect_vec();
        for (index, mut order) in order_stream.into_iter().flat_map(fnn) {
            std::mem::swap(&mut orders[index], &mut order);
        }
        if worker_index == 0 {
            print_memory_usage(format_args!("loaded orders"));
        }

        let mut probe = ProbeHandle::new();

        let output_stream = worker.dataflow(|scope| {
            let edge_stream = filtered_edge_stream.into_iter().flat_map(fnn).to_stream(scope);
            let output_stream = edge_stream
                .process_edge(orders, dimension_lengths.clone(), store_total_data)
                .exchange(move |_| {
                    if threads_per_process == 1 {
                        0
                    } else {
                        (worker_index % threads_per_process) as u64
                    }
                })
                .probe_with(&mut probe);
            output_stream.capture()
        });
        if worker_index == 0 {
            print_memory_usage(format_args!("processing diffs"));
        }

        while !probe.done() {
            worker.step();
        }
        if worker_index == 0 {
            print_memory_usage(format_args!("done with diffs"));
        }

        output_stream.into_iter().flat_map(fnn).collect_vec()
    })
        .map_err(GSError::Timely)?;
    let worker_results = worker_threads.join();
    print_memory_usage(format_args!("done with timely"));

    let mut full_results = HashMap::new();
    let edges = &graph.edges();
    for result in worker_results {
        let maps = result.map_err(GSError::TimelyResults)?;
        for map in maps {
            for (key, values) in map {
                let entry = full_results.entry(key).or_insert_with(|| (Vec::new(), Vec::new()));
                entry.0.extend(values.0);
                entry.1.extend(values.1);
            }
        }
    }

    let mut final_results = Vec::new();
    // The sorted ordering from `timestamp_mappings` should be maintained in `final_results` as well.
    for timestamps in &timestamp_mappings.0.iter().enumerate().chunks(threads_per_process) {
        thread::scope(|s| {
            let mut threads = Vec::new();
            for (timestamp_index, (_, timestamp)) in timestamps {
                let data =
                    full_results.remove(&timestamp_index).unwrap_or((Vec::new(), Vec::new()));
                let thread = s.spawn(move |_| {
                    let full_data = data
                        .0
                        .into_iter()
                        .map(|edge_id| {
                            let edge = &edges[edge_id as usize];
                            (edge.src_vertex_id, edge.dst_vertex_id)
                        })
                        .collect_vec();
                    let mut additions = 0;
                    let mut deletions = 0;
                    let diff_data = data
                        .1
                        .into_iter()
                        .map(|(edge_id, diff)| {
                            let edge = &edges[edge_id as usize];
                            if diff > 0 {
                                additions += 1;
                            } else {
                                deletions += 1;
                            }
                            ((edge.src_vertex_id, edge.dst_vertex_id), diff)
                        })
                        .collect_vec();
                    (timestamp_index, *timestamp, (full_data, diff_data), (additions, deletions))
                });
                threads.push(thread);
            }
            for thread in threads.drain(..) {
                final_results
                    .push(thread.join().unwrap_or_else(|_| panic!("Error joining thread")));
            }
        })
        .expect("Error mapping results");
    }

    Ok(final_results)
}

fn fnn<T, D>(r: Event<T, D>) -> impl Iterator<Item = D> {
    if let Messages(_, entries) = r {
        entries.into_iter()
    } else {
        Vec::new().into_iter()
    }
}

pub fn get_results_stash(
    orders: &[DimensionOrder],
    dimension_lengths: &[DimensionLength],
) -> Vec<DiffProcessingData> {
    let timestamp_mappings = get_timestamp_mappings(dimension_lengths);
    orders
        .iter()
        .map(|order| (0..).zip(order.iter().copied()))
        .multi_cartesian_product()
        .map(|values| {
            let timestamp = GSTimestamp::new(&values.iter().map(|&(id, _)| id).collect_vec());
            let timestamp_index =
                timestamp_mappings.1.get(&timestamp).copied().expect("Should be there");
            let indices = values.iter().map(|&(_, index)| index as usize).collect_vec();
            DiffProcessingData {
                timestamp_index,
                diff_neighborhood: timestamp_mappings.0[timestamp_index].0.clone(),
                indices,
                results: (vec![], vec![]),
            }
        })
        .collect_vec()
}

//fn print_matrix(matrix: &Matrix) {
//    info!(
//        "{} {}\n[[{}]]",
//        matrix.nrows(),
//        matrix.ncols(),
//        matrix
//            .row_iter()
//            .map(|row| row
//                .iter()
//                .map(ToString::to_string)
//                .collect::<Vec<_>>()
//                .join(", "))
//            .collect::<Vec<_>>()
//            .join("],\n[")
//    );
//}
