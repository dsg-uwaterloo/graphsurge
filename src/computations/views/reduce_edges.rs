use crate::computations::views::{EdgeMapOutput, EdgeReduceOutput};
use crate::computations::TimelyTimeStamp;
use hashbrown::HashMap;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait ReduceEdges<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn reduce_edges(&self) -> Stream<S, EdgeReduceOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> ReduceEdges<S> for Stream<S, EdgeMapOutput> {
    fn reduce_edges(&self) -> Stream<S, EdgeReduceOutput> {
        let mut stash = HashMap::new();
        self.unary_notify(Pipeline, "ReduceEdges", None, move |input, output, notificator| {
            input.for_each(|time, input_data| {
                let collection = stash.entry(time.time().clone()).or_insert_with(|| {
                    notificator.notify_at(time.retain());
                    HashMap::new()
                });
                input_data.replace(Vec::new()).into_iter().for_each(
                    |(_, edge_map, (section_index, _, edge))| {
                        collection
                            .entry(edge_map)
                            .or_insert_with(HashMap::new)
                            .entry(section_index)
                            .or_insert_with(Vec::new)
                            .push(edge);
                    },
                );
            });
            notificator.for_each(|time, _cnt, _not| {
                if let Some(collection) = stash.remove(&time) {
                    let mut session = output.session(&time);
                    session.give_iterator(
                        collection.into_iter().map(|(em, map)| (em, map.into_iter().collect())),
                    );
                }
            })
        })
    }
}
