use crate::computations::views::tree_aggregate_vertices::merge_properties;
use crate::computations::views::{AggregatedEdgeOutput, TreeEdgeMapOutput};
use crate::computations::TimelyTimeStamp;
use hashbrown::HashMap;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait TreeReduceEdges<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn tree_reduce_edges(&self) -> Stream<S, AggregatedEdgeOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> TreeReduceEdges<S> for Stream<S, TreeEdgeMapOutput> {
    fn tree_reduce_edges(&self) -> Stream<S, AggregatedEdgeOutput> {
        let mut stash = HashMap::new();
        self.unary_notify(Pipeline, "ReduceEdges", None, move |input, output, notificator| {
            input.for_each(|time, input_data| {
                let collection = stash.entry(time.time().clone()).or_insert_with(|| {
                    notificator.notify_at(time.retain());
                    HashMap::new()
                });
                input_data.replace(Vec::new()).into_iter().for_each(
                    |(_, edge_map, (_, edge_list))| {
                        let map = collection.entry(edge_map).or_insert_with(HashMap::new);
                        for (section_index, properties) in edge_list {
                            map.entry(section_index).or_insert_with(Vec::new).push(properties);
                        }
                    },
                );
            });
            notificator.for_each(|time, _cnt, _not| {
                if let Some(collection) = stash.remove(&time) {
                    let mut session = output.session(&time);
                    session.give_iterator(collection.into_iter().map(
                        |(edge_map, properties_map)| {
                            let properties = properties_map
                                .into_iter()
                                .map(|(section_index, properties_list)| {
                                    let mut properties_list_iter = properties_list.into_iter();
                                    let mut final_properties =
                                        properties_list_iter.next().expect("Should not be empty");

                                    for next_properties in properties_list_iter {
                                        merge_properties(&mut final_properties, &next_properties);
                                    }
                                    (section_index, final_properties)
                                })
                                .collect::<Vec<_>>();
                            (edge_map, properties)
                        },
                    ));
                }
            })
        })
    }
}
