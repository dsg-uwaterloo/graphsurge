use crate::computations::views::{TreeVertexMapOutput, TreeVertexReduceOutput};
use crate::computations::TimelyTimeStamp;
use crate::util::id_generator::IdGenerator;
use gs_analytics_api::VertexId;
use hashbrown::HashMap;
use std::convert::TryFrom;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait TreeReduceVertices<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn tree_reduce_vertices(
        &self,
        peer_count: usize,
        worker_index: usize,
    ) -> Stream<S, TreeVertexReduceOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> TreeReduceVertices<S>
    for Stream<S, TreeVertexMapOutput>
{
    fn tree_reduce_vertices(
        &self,
        peer_count: usize,
        worker_index: usize,
    ) -> Stream<S, TreeVertexReduceOutput> {
        let mut groups = HashMap::new();
        self.unary_notify(
            Pipeline,
            "TreeReduceVertices",
            None,
            move |input, output, notificator| {
                input.for_each(|time, input_data| {
                    let collection = groups.entry(time.time().clone()).or_insert_with(|| {
                        notificator.notify_at(time.retain());
                        HashMap::new()
                    });
                    input_data.replace(Vec::new()).into_iter().for_each(
                        |(vertex_id, order_id, properties, group)| {
                            collection
                                .entry(order_id)
                                .or_insert_with(HashMap::new)
                                .entry(group)
                                .or_insert_with(Vec::new)
                                .push((vertex_id, properties));
                        },
                    );
                });
                notificator.for_each(|time, _cnt, _not| {
                    if let Some(collection) = groups.remove(&time) {
                        let mut session = output.session(&time);
                        let mut group_ids = IdGenerator::new_from(worker_index, peer_count);
                        for (order_id, groups) in collection {
                            for (group, vertex_data) in groups {
                                session.give((
                                    VertexId::try_from(group_ids.next_id()).expect("Overflow"),
                                    order_id,
                                    group,
                                    vertex_data,
                                ));
                            }
                        }
                    }
                })
            },
        )
    }
}
