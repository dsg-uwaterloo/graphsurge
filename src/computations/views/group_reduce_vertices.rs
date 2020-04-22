use crate::computations::views::{VertexGroupMapOutput, VertexGroupReduceOutput};
use crate::computations::TimelyTimeStamp;
use crate::util::id_generator::IdGenerator;
use gs_analytics_api::VertexId;
use hashbrown::HashMap;
use std::convert::TryFrom;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait GroupReduceVertices<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn group_reduce_vertices(
        &self,
        peer_count: usize,
        worker_index: usize,
    ) -> Stream<S, VertexGroupReduceOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> GroupReduceVertices<S>
    for Stream<S, VertexGroupMapOutput>
{
    fn group_reduce_vertices(
        &self,
        peer_count: usize,
        worker_index: usize,
    ) -> Stream<S, VertexGroupReduceOutput> {
        let mut groups = HashMap::new();
        self.unary_notify(Pipeline, "GroupReduce", None, move |input, output, notificator| {
            input.for_each(|time, input_data| {
                let collection = groups.entry(*time.time()).or_insert_with(|| {
                    notificator.notify_at(time.retain());
                    HashMap::new()
                });
                input_data.replace(Vec::new()).into_iter().for_each(
                    |(section_index, group_value, vertex_data)| {
                        collection
                            .entry(section_index)
                            .or_insert_with(HashMap::new)
                            .entry(group_value)
                            .or_insert_with(Vec::new)
                            .push(vertex_data);
                    },
                );
            });
            notificator.for_each(|time, _cnt, _not| {
                if let Some(collection) = groups.remove(&time) {
                    let mut session = output.session(&time);
                    let mut group_ids = IdGenerator::new_from(worker_index, peer_count);
                    for (si, groups) in collection {
                        for (group_value, vertex_id) in groups {
                            session.give((
                                si,
                                VertexId::try_from(group_ids.next_id()).expect("Overflow"),
                                group_value,
                                vertex_id,
                            ));
                        }
                    }
                }
            })
        })
    }
}
