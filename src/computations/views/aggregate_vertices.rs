use crate::computations::views::{AggregatedVertexOutput, VertexGroupReduceOutput};
use crate::graph::stream_data::aggregation::get_aggregates;
use crate::graph::GraphPointer;
use crate::query_handler::create_view::VertexSections;
use gs_analytics_api::TimelyTimeStamp;
use std::num::NonZeroU8;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait AggregateVertices<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn aggregate_vertices(
        &self,
        vertex_sections: VertexSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, AggregatedVertexOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> AggregateVertices<S>
    for Stream<S, VertexGroupReduceOutput>
{
    fn aggregate_vertices(
        &self,
        vertex_sections: VertexSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, AggregatedVertexOutput> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "AggregateVertices", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for (section_index, created_vertex_id, gv, data) in vector.drain(..) {
                        let mut aggregates = Vec::new();
                        if let Some(section_details) = vertex_sections
                            .get(&NonZeroU8::new(section_index).expect("Should not be zero"))
                        {
                            for (name, operation, property_key_id) in
                                &section_details.aggregate_clauses
                            {
                                aggregates.push((
                                    name.clone(),
                                    get_aggregates(
                                        *operation,
                                        *property_key_id,
                                        &data,
                                        graph_pointer,
                                    ),
                                ));
                            }
                        }
                        session.give((created_vertex_id, aggregates, gv));
                    }
                });
            }
        })
    }
}
