use crate::computations::views::{
    QueryProperties, QueryProperty, TreeAggregatedVertexOutput, TreeVertexReduceOutput,
};
use crate::graph::properties::property_value::PropertyValue;
use gs_analytics_api::TimelyTimeStamp;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait TreeAggregateVertices<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn tree_aggregate_vertices(&self) -> Stream<S, TreeAggregatedVertexOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> TreeAggregateVertices<S>
    for Stream<S, TreeVertexReduceOutput>
{
    fn tree_aggregate_vertices(&self) -> Stream<S, TreeAggregatedVertexOutput> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "TreeAggregateVertices", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for (created_vertex_id, order_index, group, vertex_data) in vector.drain(..) {
                        let mut vertex_data_iter = vertex_data.into_iter();
                        if let Some((_, mut final_properties)) = vertex_data_iter.next() {
                            for (_, next_properties) in vertex_data_iter {
                                merge_properties(&mut final_properties, &next_properties);
                            }
                            session
                                .give((order_index, (created_vertex_id, final_properties, group)));
                        }
                    }
                });
            }
        })
    }
}

pub(super) fn merge_properties(
    final_properties: &mut QueryProperties,
    next_properties: &[QueryProperty],
) {
    final_properties.iter_mut().zip(next_properties.iter()).for_each(
        |((_, lvalue), (_, rvalue))| match lvalue {
            PropertyValue::Isize(num) => {
                if let PropertyValue::Isize(rnum) = rvalue {
                    *num += rnum;
                }
            }
            PropertyValue::Pair(sum, count) => {
                if let PropertyValue::Pair(rsum, rcount) = rvalue {
                    *sum += rsum;
                    *count += rcount;
                }
            }
            _ => unreachable!(),
        },
    );
}
