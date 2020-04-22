use crate::computations::views::aggregate_edges::AggregateEdges;
use crate::computations::views::aggregate_vertices::AggregateVertices;
use crate::computations::views::filter_group_map_vertices::FilterGroupMapVertices;
use crate::computations::views::filter_map_edges::FilterMapEdges;
use crate::computations::views::group_reduce_vertices::GroupReduceVertices;
use crate::computations::views::map_edges_to_groups::MapEdgesBetweenGroups;
use crate::computations::views::monitor::MonitorStream;
use crate::computations::views::reduce_edges::ReduceEdges;
use crate::computations::views::tree_aggregate_vertices::TreeAggregateVertices;
use crate::computations::views::tree_map_edges_to_groups::TreeMapEdgesBetweenGroups;
use crate::computations::views::tree_map_vertices::TreeMapVertices;
use crate::computations::views::tree_reduce_edges::TreeReduceEdges;
use crate::computations::views::tree_reduce_vertices::TreeReduceVertices;
use crate::computations::views::{
    EdgeViewOutput, ExecutionType, GroupsLength, VertexGroupMapOutput, VertexReverseGroupOutput,
    VertexViewOutput,
};
use crate::error::{create_view_error, GraphSurgeError};
use crate::global_store::GlobalStore;
use crate::graph::stream_data::{get_timely_edgeid_stream, get_timely_vertex_stream};
use crate::graph::GraphPointer;
use crate::query_handler::create_view::CreateViewAst;
use differential_dataflow::hashable::Hashable;
use itertools::Itertools;
use log::info;

use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::event::Event::Messages;
use timely::dataflow::operators::exchange::Exchange;
#[allow(unused_imports)]
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::partition::Partition;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::Configuration;

pub type ViewResults = (
    Vec<VertexViewOutput>,
    Vec<EdgeViewOutput>,
    Vec<Vec<(Vec<VertexViewOutput>, Vec<EdgeViewOutput>)>>,
);

pub fn execute(
    create_view_ast: CreateViewAst,
    global_store: &GlobalStore,
    execution_type: ExecutionType,
) -> Result<ViewResults, GraphSurgeError> {
    let graph_pointer = GraphPointer::new(&global_store.graph);

    info!("Starting execution for new view...");
    let worker_results =
        timely::execute(Configuration::Process(global_store.threads.get()), move |worker| {
            let timer = std::time::Instant::now();

            let mut vertex_input = InputHandle::new();
            let mut edge_input = InputHandle::new();
            let mut probe = ProbeHandle::new();

            let peers_count = worker.peers();
            let worker_index = worker.index();
            let worker_count = worker.peers();

            let (vertex_stream, edge_stream, aggregated_output_streams) =
                worker.dataflow(|scope| {
                    let ast = create_view_ast.clone();
                    let edge_stream = edge_input.to_stream(scope);
                    let vertex_stream = vertex_input.to_stream(scope);

                    // This will be reused below for both vertex and edge streams.
                    info!("Grouping vertices...");
                    let grouped_vertex_stream = vertex_stream
                        .monitor(500_000, "vertex_stream", worker_index)
                        // Apply where clauses and map vertices to their groups.
                        .filter_group_map_vertices(
                            ast.vertex_sections.clone(),
                            graph_pointer,
                        )
                        .monitor(500_000, "filter_group_map_vertices", worker_index)
                        // Shuffle groups to workers.
                        .exchange(|(_, gv, _): &VertexGroupMapOutput| gv.hashed())
                        // Reduce vertex groups.
                        .group_reduce_vertices(peers_count, worker_index)
                        .monitor(100, "group_reduce_vertices", worker_index);

                    info!("Aggregating vertices...");
                    let vertex_output_stream = grouped_vertex_stream
                        .aggregate_vertices(ast.vertex_sections.clone(), graph_pointer)
                        .monitor(100, "aggregate_vertices", worker_index);
                    //.inspect(move |x| info!("w={}, v={:?}", worker_index, x));

                    info!("Computing reverse vertex mappings...");
                    let reverse_group_map_stream = grouped_vertex_stream
                        .flat_map(|(si, cvi, _, vdata)| {
                            vdata.into_iter().map(move |vertex_id| (vertex_id, si, cvi))
                        })
                        // Shuffle the vertices to corresponding workers.
                        .exchange(|(vertex_id, _, _): &VertexReverseGroupOutput| {
                            vertex_id.hashed()
                        });

                    info!("Processing edges...");
                    let edge_output_stream = edge_stream
                        .monitor(500_000, "edge_stream", worker_index)
                        .filter_map_edges(ast.edge_sections.clone(), graph_pointer)
                        .monitor(500_000, "filter_map_edges", worker_index)
                        // Create edges between grouped nodes.
                        .map_edges_to_groups(reverse_group_map_stream,graph_pointer)
                        .monitor(500_000, "map_edges_to_groups", worker_index)
                        .reduce_edges()
                        .monitor(100, "reduce_edges", worker_index)
                        .aggregate_edges(ast.edge_sections, graph_pointer)
                        .monitor(100, "aggregate_edges", worker_index);
                    //.inspect(move |x| info!("w={}, e={:?}", worker_index, x));

                    let mut streams = Vec::new();
                    if let ExecutionType::AggregatedCube(groups_length) = execution_type {
                        info!("Starting creating of aggregated cube...");
                        let mut starting_vertex_stream =
                            vertex_output_stream.map_in_place(|(_, _, group)| {
                                // Replace 1st 2 items from the group (si and gci).
                                group.splice(0..2, Vec::new().into_iter());
                            });
                        for level in (1..groups_length).rev() {
                            info!("Creating cube level {}...", level);
                            // Calculate number of combinations of specified groups for each level.
                            // Using combination formula: `groups_length` choose `level`.
                            let combinations_count: GroupsLength = ((level + 1)..=groups_length)
                                .product::<GroupsLength>()
                                / (2..=(groups_length - level)).product::<GroupsLength>();

                            let tree_grouped_vertex_stream = starting_vertex_stream
                                .tree_map_vertices(level)
                                .tree_reduce_vertices(peers_count, worker_index);

                            let tree_vertex_output_streams = tree_grouped_vertex_stream
                                .tree_aggregate_vertices()
                                .partition(u64::from(combinations_count), move |(oi, data)| {
                                    (u64::from(oi % combinations_count), data)
                                });

                            let tree_reverse_group_map_streams = tree_grouped_vertex_stream
                                .flat_map(|(nvi, oi, _, vdata)| {
                                    vdata.into_iter().map(move |(ovi, _)| (oi, (ovi, nvi)))
                                })
                                // Shuffle the vertices to corresponding workers.
                                .exchange(|(_, (vertex_id, _))| vertex_id.hashed())
                                .partition(u64::from(combinations_count), move |(oi, data)| {
                                    (u64::from(oi % combinations_count), data)
                                });

                            let mut tree_edge_output_streams = Vec::new();
                            for tree_reverse_group_map_stream in tree_reverse_group_map_streams {
                                let tree_edge_output_stream = edge_output_stream
                                    // Create edges between grouped nodes.
                                    .tree_map_edges_to_groups(tree_reverse_group_map_stream)
                                    .tree_reduce_edges();
                                tree_edge_output_streams.push(tree_edge_output_stream);
                            }

                            starting_vertex_stream = tree_vertex_output_streams[0].clone();
                            streams.push((tree_vertex_output_streams, tree_edge_output_streams));
                        }
                    }

                    // Attach timely probe that helps track dataflow progress.
                    vertex_output_stream.probe_with(&mut probe);
                    edge_output_stream.probe_with(&mut probe);

                    let aggregated_output_streams = streams
                        .iter()
                        .map(|(vss, ess)| {
                            (
                                vss.iter()
                                    .map(|vs| {
                                        vs.probe_with(&mut probe);
                                        vs.capture()
                                    })
                                    .collect::<Vec<_>>(),
                                ess.iter()
                                    .map(|es| {
                                        es.probe_with(&mut probe);
                                        es.capture()
                                    })
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .collect::<Vec<_>>();

                    let v = vertex_output_stream.capture();
                    let e = edge_output_stream.capture();
                    // Output the vertex and edge streams.
                    (v, e, aggregated_output_streams)
                });

            for vertex in get_timely_vertex_stream(&*graph_pointer, worker_index, worker_count) {
                vertex_input.send(vertex);
            }
            vertex_input.close();
            for edgeid in get_timely_edgeid_stream(&*graph_pointer, worker_index, worker_count) {
                edge_input.send(edgeid);
            }
            edge_input.close();

            while !probe.done() {
                worker.step();
            }

            info!("Done on worker {} in {:?}", worker_index, timer.elapsed());
            let mut vertex_results = Vec::new();
            for r in vertex_stream {
                if let Messages(_timestamp, entries) = r {
                    vertex_results.extend(entries.into_iter());
                }
            }
            let mut edge_results = Vec::new();
            for r in edge_stream {
                if let Messages(_timestamp, entries) = r {
                    edge_results.extend(entries.into_iter());
                }
            }
            let mut aggregated_output_results = Vec::new();
            for (vertex_streams, edge_streams) in aggregated_output_streams {
                let results = vertex_streams
                    .into_iter()
                    .zip_eq(edge_streams)
                    .map(|(vertex_stream, edge_stream)| {
                        let mut vertex_results = Vec::new();
                        for r in vertex_stream {
                            if let Messages(_timestamp, entries) = r {
                                vertex_results.extend(entries.into_iter());
                            }
                        }
                        let mut edge_results = Vec::new();
                        for r in edge_stream {
                            if let Messages(_timestamp, entries) = r {
                                edge_results.extend(entries.into_iter());
                            }
                        }
                        (vertex_results, edge_results)
                    })
                    .collect::<Vec<_>>();
                aggregated_output_results.push(results);
            }
            (vertex_results, edge_results, aggregated_output_results)
        })
        .map_err(|e| create_view_error(format!("Timely error: {:?}", e)))?
        .join();

    let mut all_vertex_results = Vec::new();
    let mut all_edge_results = Vec::new();
    let mut all_aggregated_output_results = Vec::new();
    for result in worker_results {
        let (vertex_results, edge_results, aggregated_output_results) = result
            .map_err(|e| create_view_error(format!("Results from timely has errors: {:?}", e)))?;
        all_vertex_results.extend(vertex_results);
        all_edge_results.extend(edge_results);
        all_aggregated_output_results.extend(aggregated_output_results);
    }
    Ok((all_vertex_results, all_edge_results, all_aggregated_output_results))
}
