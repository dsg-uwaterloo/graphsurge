use crate::computations::filtered_cubes::Matrices;
use crate::filtered_cubes::DimensionLengths;
use gs_analytics_api::TimelyTimeStamp;
use hashbrown::HashMap;
use itertools::Itertools;
use log::info;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub trait ReduceMatrices<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn reduce_matrices(
        &self,
        dimensions_lengths: DimensionLengths,
        worker_index: usize,
    ) -> Stream<S, Matrices>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> ReduceMatrices<S> for Stream<S, Matrices> {
    fn reduce_matrices(
        &self,
        dimensions_lengths: DimensionLengths,
        worker_index: usize,
    ) -> Stream<S, Matrices> {
        let mut times = HashMap::new();
        self.unary_notify(Pipeline, "ReduceMatrix", None, move |input, output, notificator| {
            input.for_each(|time, input_data| {
                let matrices = times.entry(*time.time()).or_insert_with(|| {
                    if worker_index == 0 {
                        info!("Starting reducing matrices");
                    }
                    notificator.notify_at(time.retain());
                    dimensions_lengths
                        .iter()
                        .map(|&dimension_length| {
                            let row =
                                std::iter::repeat(0).take(dimension_length as usize).collect_vec();
                            (0..dimension_length).map(|_| row.clone()).collect_vec()
                        })
                        .collect_vec()
                });
                for partial_matrices in input_data.iter() {
                    for (dimension_index, filter_matrix_row) in partial_matrices.iter().enumerate()
                    {
                        let dimension_matrix = matrices
                            .get_mut(dimension_index)
                            .expect("Dimension index should be there");
                        for (i, dimension_row) in dimension_matrix.iter_mut().enumerate() {
                            for (j, cell) in dimension_row.iter_mut().enumerate() {
                                *cell +=
                                    unsafe { filter_matrix_row.get_unchecked(i).get_unchecked(j) };
                            }
                        }
                    }
                }
            });
            notificator.for_each(|time, _cnt, _not| {
                if let Some(matrices) = times.remove(&time) {
                    if worker_index == 0 {
                        info!("Done reducing matrices");
                    }
                    output.session(&time).give(matrices);
                }
            });
        })
    }
}
