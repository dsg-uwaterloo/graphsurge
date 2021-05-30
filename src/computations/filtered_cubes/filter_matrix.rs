use crate::computations::filtered_cubes::{FilteredMatrixRow, FilteredMatrixStream};
use crate::graph::stream_data::filter::test_where_conditions;
use crate::graph::GraphPointer;
use crate::query_handler::create_filtered_cube::Dimension;
use gs_analytics_api::{EdgeId, TimelyTimeStamp};
use itertools::Itertools;
use std::iter;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub trait FilteredMatrix<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn filtered_matrix(
        &self,
        dimensions: Vec<Dimension>,
        graph_pointer: GraphPointer,
    ) -> Stream<S, FilteredMatrixStream>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> FilteredMatrix<S> for Stream<S, EdgeId> {
    fn filtered_matrix(
        &self,
        dimensions: Vec<Dimension>,
        graph_pointer: GraphPointer,
    ) -> Stream<S, FilteredMatrixStream> {
        self.unary(Pipeline, "FilterMatrix", move |_, _| {
            move |input, output| {
                input.for_each(|time, edges| {
                    let mut session = output.session(&time);
                    edges.iter().for_each(|&edge_id| {
                        session.give((
                            edge_id,
                            create_filtered_matrix(edge_id, &dimensions, graph_pointer),
                        ));
                    });
                });
            }
        })
    }
}

fn create_filtered_matrix(
    edge_id: EdgeId,
    dimensions: &[Dimension],
    graph_pointer: GraphPointer,
) -> Vec<FilteredMatrixRow> {
    dimensions
        .iter()
        .map(|dimension| {
            iter::once(0)
                .chain(dimension.iter().map(|conditions| {
                    if test_where_conditions(edge_id, &conditions, graph_pointer) {
                        1
                    } else {
                        0
                    }
                }))
                .collect_vec()
        })
        .collect::<Vec<_>>()
}
