use crate::computations::views::{AggregatedVertexOutput, GroupsLength, TreeVertexMapOutput};
use differential_dataflow::hashable::Hashable;
use gs_analytics_api::TimelyTimeStamp;
use itertools::Itertools;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait TreeMapVertices<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn tree_map_vertices(&self, level: GroupsLength) -> Stream<S, TreeVertexMapOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> TreeMapVertices<S>
    for Stream<S, AggregatedVertexOutput>
{
    fn tree_map_vertices(&self, level: GroupsLength) -> Stream<S, TreeVertexMapOutput> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "TreeMap", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for (vertex_id, properties, group) in vector.drain(..) {
                        assert!(usize::from(level + 1) == group.len());
                        for (mut new_group, order_index) in
                            group.into_iter().combinations(usize::from(level)).zip(0_u8..)
                        {
                            new_group.push(format!("oi={}", order_index));
                            let new_properties = properties.clone();
                            session.give((vertex_id, order_index, new_properties, new_group));
                        }
                    }
                });
            }
        })
        .exchange(|(_, _, _, group)| group.hashed())
    }
}
