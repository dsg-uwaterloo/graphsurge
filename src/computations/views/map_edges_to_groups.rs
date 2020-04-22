use crate::computations::views::{
    CreatedVertexId, EdgeMapOutput, EdgeMapState, FilterMapEdgesOutput, SectionIndex,
    TimelyInnerTimeStamp, VertexReverseGroupOutput,
};
use crate::computations::TimelyTimeStamp;
use crate::graph::Graph;
use crate::graph::GraphPointer;
use differential_dataflow::hashable::Hashable;
use gs_analytics_api::VertexId;
use hashbrown::HashMap;

use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::concat::Concat;
use timely::dataflow::operators::enterleave::{Enter, Leave};
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::feedback::{ConnectLoop, Feedback};
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;
use timely::dataflow::{Scope, Stream};
use timely::order::Product;

pub(in crate::computations::views) trait MapEdgesBetweenGroups<
    S: Scope<Timestamp = TimelyTimeStamp>,
>
{
    fn map_edges_to_groups(
        &self,
        reverse_group_map_stream: Stream<S, VertexReverseGroupOutput>,
        graph_pointer: GraphPointer,
    ) -> Stream<S, EdgeMapOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> MapEdgesBetweenGroups<S>
    for Stream<S, FilterMapEdgesOutput>
{
    fn map_edges_to_groups(
        &self,
        reverse_group_map_stream: Stream<S, VertexReverseGroupOutput>,
        graph_pointer: GraphPointer,
    ) -> Stream<S, EdgeMapOutput> {
        let edge_stream = self
            // Map edge based on `src` vertex id, to send to the worker that should have the group
            // mapping for `src`. IMPORTANT: The code below relies on this mapping.
            .exchange(move |&(_, _, edge)| (graph_pointer.edges()[edge as usize].src_vertex_id).hashed())
            // Map to same data type as the loop.
            .map(|e| (EdgeMapState::None, (0, 0), e));

        // Map edges in an iterative inner subscope to ensure that all final output tuples have
        // the same outer timestamps.
        self.scope().iterative(|subscope| {
            let (loop_handle, loop_stream) = subscope.feedback(Product { outer: 0, inner: 1 });
            let loop_stream = loop_stream.concat(&edge_stream.enter(subscope));

            let mut is_stream_done = false;
            let mut vertex_stash = HashMap::new();
            let mut edge_stash = Vec::new();

            let output_stream = reverse_group_map_stream
                .enter(subscope)
                .binary_notify(
                    &loop_stream,
                    Pipeline,
                    Pipeline,
                    "EdgeMap",
                    None,
                    move |input1, input2, output, notificator| {
                        input1.for_each(|time, input1_data| {
                            if !is_stream_done {
                                notificator.notify_at(time.retain());
                            }
                            input1_data.replace(Vec::new()).into_iter().for_each(
                                |(vi, si, cvi)| {
                                    vertex_stash.entry(vi).or_insert_with(Vec::new).push((si, cvi));
                                },
                            );
                        });
                        input2.for_each(|time, input2_data| {
                            let mut edges = input2_data.replace(Vec::new());
                            if is_stream_done {
                                // All vertex tuples have arrived. Directly process the edges.
                                let session = output.session(&time);
                                process_edges(&*graph_pointer, &mut edges, &vertex_stash, session);
                            } else {
                                // Store the edges until all vertex tuples have arrived.
                                edge_stash.extend(edges.into_iter());
                            }
                        });
                        notificator.for_each(|time, _cnt, _not| {
                            is_stream_done = true;
                            // Process the stored edges.
                            let session = output.session(&time);
                            process_edges(&*graph_pointer, &mut edge_stash, &vertex_stash, session);
                        });
                    },
                )
                .exchange(|(edge_state, (src_hash, dst_hash), _): &EdgeMapOutput| {
                    match edge_state {
                        // Both mappings found. Hash to the final destination.
                        EdgeMapState::Done => (src_hash, dst_hash).hashed(),
                        // Only source vertex mapping found. Shuffle to the worker that potentially
                        // has the `dst` mapping.
                        EdgeMapState::SrcOnly(_) => dst_hash.hashed(),
                        // Only the starting state.
                        EdgeMapState::None => panic!("Should not be reached"),
                    }
                });

            output_stream.connect_loop(loop_handle);

            output_stream.filter(|(state, _, _)| *state == EdgeMapState::Done).leave()
        })
    }
}

fn process_edges<P: Push<Bundle<TimelyInnerTimeStamp, EdgeMapOutput>>>(
    graph: &Graph,
    edges: &mut Vec<EdgeMapOutput>,
    vertex_stash: &HashMap<VertexId, Vec<(SectionIndex, CreatedVertexId)>>,
    mut session: Session<TimelyInnerTimeStamp, EdgeMapOutput, P>,
) {
    for (state, (src_mapped_id, dst_mapped_id), edge_data) in edges.drain(..) {
        let between_cond = &edge_data.1;
        match state {
            EdgeMapState::None => {
                let edge = &graph.edges()[edge_data.2 as usize];
                let (src, dst) = (edge.src_vertex_id, edge.dst_vertex_id);
                let src_map = vertex_stash.get(&src);
                let dst_map = vertex_stash.get(&dst);
                if let Some(src_values) = src_map {
                    for (src_section_index, src_map_id) in src_values {
                        if let Some(dst_values) = dst_map {
                            for (dst_section_index, dst_map_id) in dst_values {
                                if between_cond.0 == 0
                                    || (between_cond.0 == *src_section_index
                                        && between_cond.1 == *dst_section_index)
                                    || (between_cond.1 == *src_section_index
                                        && between_cond.0 == *dst_section_index)
                                {
                                    session.give((
                                        EdgeMapState::Done,
                                        (*src_map_id, *dst_map_id),
                                        edge_data,
                                    ));
                                }
                            }
                        } else if between_cond.0 == 0
                            || between_cond.0 == *src_section_index
                            || between_cond.1 == *src_section_index
                        {
                            session.give((
                                EdgeMapState::SrcOnly(*src_section_index),
                                (*src_map_id, dst),
                                edge_data,
                            ));
                        } else {
                            //Nothing to do.
                        }
                    }
                }
            }
            EdgeMapState::SrcOnly(src_section_index) => {
                if let Some(dst_values) = vertex_stash.get(&dst_mapped_id) {
                    for (dst_section_index, dst_map_id) in dst_values {
                        if between_cond.0 == 0
                            || (between_cond.0 == src_section_index
                                && between_cond.1 == *dst_section_index)
                            || (between_cond.1 == src_section_index
                                && between_cond.0 == *dst_section_index)
                        {
                            session.give((
                                EdgeMapState::Done,
                                (src_mapped_id, *dst_map_id),
                                edge_data,
                            ));
                        }
                    }
                }
            }
            EdgeMapState::Done => {}
        }
    }
}
