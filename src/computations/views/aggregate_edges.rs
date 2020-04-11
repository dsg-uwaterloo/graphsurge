use crate::computations::views::{AggregatedEdgeOutput, EdgeReduceOutput};
use crate::computations::TimelyTimeStamp;
use crate::graph::stream_data::aggregation::get_edge_aggregates;
use crate::graph::GraphPointer;
use crate::query_handler::create_view::EdgeSections;
use std::num::NonZeroU8;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait AggregateEdges<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn aggregate_edges(
        &self,
        edge_sections: EdgeSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, AggregatedEdgeOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> AggregateEdges<S> for Stream<S, EdgeReduceOutput> {
    fn aggregate_edges(
        &self,
        edge_sections: EdgeSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, AggregatedEdgeOutput> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "AggregateEdges", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for (created_vertex_ids, edge_stream) in vector.drain(..) {
                        let mut aggregates = Vec::new();
                        for (section_index, edge_data) in edge_stream {
                            let mut section_aggregates = Vec::new();
                            if let Some((_, section_details)) = edge_sections
                                .get(&NonZeroU8::new(section_index).expect("Should not be zero"))
                            {
                                for (name, operation, property_key_id) in
                                    &section_details.aggregate_clauses
                                {
                                    let property_key_id = *property_key_id;
                                    section_aggregates.push((
                                        name.clone(),
                                        get_edge_aggregates(
                                            *operation,
                                            property_key_id,
                                            &edge_data,
                                            graph_pointer,
                                        ),
                                    ));
                                }
                            }
                            aggregates.push((section_index, section_aggregates));
                        }
                        session.give((created_vertex_ids, aggregates));
                    }
                })
            }
        })
    }
}
