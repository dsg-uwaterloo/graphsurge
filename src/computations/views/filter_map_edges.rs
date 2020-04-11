use crate::computations::views::{FilterMapEdgesOutput, FIRST_SECTION_INDEX, NULL_SECTION_INDEX};
use crate::computations::TimelyTimeStamp;
use crate::graph::stream_data::filter::test_where_conditions;
use crate::graph::GraphPointer;
use crate::query_handler::create_view::EdgeSections;
use gs_analytics_api::EdgeId;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait FilterMapEdges<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn filter_map_edges(
        &self,
        edge_sections: EdgeSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, FilterMapEdgesOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> FilterMapEdges<S> for Stream<S, EdgeId> {
    fn filter_map_edges(
        &self,
        edge_sections: EdgeSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, FilterMapEdgesOutput> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "FilterMapEdges", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    if edge_sections.is_empty() {
                        session.give_iterator(vector.drain(..).map(|e| {
                            (FIRST_SECTION_INDEX, (NULL_SECTION_INDEX, NULL_SECTION_INDEX), e)
                        }));
                    } else {
                        edge_sections.iter().for_each(
                            |(section_index, (between_condition, section_details))| {
                                let edges =
                                    if usize::from(section_index.get()) == edge_sections.len() {
                                        vector.drain(..).collect()
                                    } else {
                                        vector.clone()
                                    };
                                edges
                                    .into_iter()
                                    .filter(|&edge| {
                                        test_where_conditions(
                                            edge,
                                            &section_details.where_conditions,
                                            graph_pointer,
                                        )
                                    })
                                    .for_each(|edge| {
                                        let between_indices =
                                            if let Some((src, dst)) = between_condition {
                                                (src.get(), dst.get())
                                            } else {
                                                (NULL_SECTION_INDEX, NULL_SECTION_INDEX)
                                            };
                                        session.give((section_index.get(), between_indices, edge));
                                    });
                            },
                        );
                    }
                });
            }
        })
    }
}
