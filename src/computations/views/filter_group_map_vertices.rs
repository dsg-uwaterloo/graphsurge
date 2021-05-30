use crate::computations::views::{SectionIndex, VertexGroupMapOutput, FIRST_SECTION_INDEX};
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::stream_data::filter::test_where_conditions;
use crate::graph::GraphPointer;
use crate::query_handler::create_view::{
    fmt_condition_clauses, FlattenedGroupCondition, GroupClause, GroupCondition, SectionDetails,
    VertexSections,
};
use gs_analytics_api::{TimelyTimeStamp, VertexId};
use itertools::Itertools;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub(super) trait FilterGroupMapVertices<S: Scope<Timestamp = TimelyTimeStamp>> {
    /// Apply filters and map vertices to their corresponding (potentially multiple) groups.
    fn filter_group_map_vertices(
        &self,
        vertex_sections: VertexSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, VertexGroupMapOutput>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> FilterGroupMapVertices<S> for Stream<S, VertexId> {
    fn filter_group_map_vertices(
        &self,
        vertex_sections: VertexSections,
        graph_pointer: GraphPointer,
    ) -> Stream<S, VertexGroupMapOutput> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "VertexFilterGroupMap", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    if vertex_sections.is_empty() {
                        session.give_iterator(vector.drain(..).map(|vertex_id| {
                            let gv = vec![format!("id={}", vertex_id)];
                            (FIRST_SECTION_INDEX, gv, vertex_id)
                        }));
                    } else {
                        vertex_sections.iter().for_each(|(section_index, section_details)| {
                            let vertices =
                                if usize::from(section_index.get()) == vertex_sections.len() {
                                    vector.drain(..).collect()
                                } else {
                                    vector.clone()
                                };
                            vertices
                                .into_iter()
                                .filter(|&vertex_id| {
                                    test_where_conditions(
                                        vertex_id,
                                        &section_details.where_conditions,
                                        graph_pointer,
                                    )
                                })
                                .for_each(|vertex_data| {
                                    group(
                                        &mut session,
                                        section_index.get(),
                                        vertex_data,
                                        &section_details,
                                        graph_pointer,
                                    );
                                });
                        });
                    }
                });
            }
        })
    }
}

fn group<'a, T: Eq + Clone, P: Push<Bundle<T, VertexGroupMapOutput>> + 'a>(
    session: &mut Session<'a, T, VertexGroupMapOutput, P>,
    section_index: SectionIndex,
    vertex_id: VertexId,
    section_details: &SectionDetails,
    graph_pointer: GraphPointer,
) {
    if section_details.group_clauses.is_empty() {
        let gv = vec![format!("si={}", section_index), format!("id={}", vertex_id)];
        session.give((section_index, gv, vertex_id));
    } else {
        'group_clauses: for (group_clause_index, group_clause) in
            get_flattened_group_clauses(section_details.group_clauses.clone())
                .into_iter()
                .enumerate()
        {
            let mut result =
                vec![format!("si={}", section_index), format!("gci={}", group_clause_index)];
            for group_condition in group_clause {
                match group_condition {
                    FlattenedGroupCondition::Variable(key_id) => {
                        let pv = PropertyValue::get_id(vertex_id);
                        let value = graph_pointer
                            .get_vertex_id_property_value(vertex_id, &pv, key_id)
                            .map_or_else(|| "''".to_owned(), ToString::to_string);
                        result.push(format!("{}={}", key_id, value));
                    }
                    FlattenedGroupCondition::WhereConditions(where_conditions) => {
                        if !test_where_conditions(vertex_id, &where_conditions, graph_pointer) {
                            // One condition failed. Skip the rest and go to next group clause.
                            continue 'group_clauses;
                        }
                        result.push(fmt_condition_clauses(&where_conditions));
                    }
                }
            }
            // If we reach here means all the group conditions hold. Moreover, `result` indicates
            // unique group id, which we will use to get the hash.
            session.give((section_index, result, vertex_id));
        }
    }
}

fn get_flattened_group_clauses(
    group_clauses: Vec<GroupClause>,
) -> Vec<Vec<FlattenedGroupCondition>> {
    let mut flattened_group_clauses = Vec::new();
    for group_clause in group_clauses {
        flattened_group_clauses.extend(
            group_clause
                .into_iter()
                .map(|group_condition| match group_condition {
                    GroupCondition::Variable(key) => {
                        vec![FlattenedGroupCondition::Variable(key)].into_iter()
                    }
                    GroupCondition::WherePredicate(where_predicate) => {
                        vec![FlattenedGroupCondition::WhereConditions(vec![(
                            false,
                            vec![where_predicate],
                        )])]
                        .into_iter()
                    }
                    GroupCondition::List(group_list) => group_list
                        .into_iter()
                        .map(|where_conditions| {
                            FlattenedGroupCondition::WhereConditions(where_conditions)
                        })
                        .collect::<Vec<_>>()
                        .into_iter(),
                })
                .multi_cartesian_product(),
        );
    }
    flattened_group_clauses
}
