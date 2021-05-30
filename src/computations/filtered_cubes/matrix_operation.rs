use crate::computations::filtered_cubes::{FilteredMatrixStream, Matrices};
use crate::filtered_cubes::DimensionLengths;
use gs_analytics_api::TimelyTimeStamp;
use hashbrown::HashMap;
use itertools::Itertools;
use log::info;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

pub trait MatrixOperation<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn create_ordering_matrices(
        &self,
        dimensions_length: DimensionLengths,
        worker_index: usize,
    ) -> Stream<S, Matrices>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> MatrixOperation<S> for Stream<S, FilteredMatrixStream> {
    fn create_ordering_matrices(
        &self,
        dimensions_lengths: DimensionLengths,
        worker_index: usize,
    ) -> Stream<S, Matrices> {
        let mut times = HashMap::new();
        self.unary_notify(Pipeline, "MatrixOperation", None, move |input, output, notificator| {
            input.for_each(|time, input_data| {
                let matrices = times.entry(*time.time()).or_insert_with(|| {
                    if worker_index == 0 {
                        info!("Starting ordering matrices processing");
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
                for (_, filter_matrix_all_dimensions) in input_data.iter() {
                    for (dimension_index, filter_matrix_row) in
                        filter_matrix_all_dimensions.iter().enumerate()
                    {
                        let sum: usize =
                            filter_matrix_row.iter().map(|&value| usize::from(value)).sum();
                        if sum == 0 || sum == filter_matrix_row.len() {
                            // Ignore row because it is either all zeros or all ones, so it will not
                            // affect the ordering of the columns.
                            continue;
                        }
                        let dimension_matrix = matrices
                            .get_mut(dimension_index)
                            .expect("Dimension index should be there");
                        for (i, dimension_row) in dimension_matrix.iter_mut().enumerate() {
                            let left_matrix_value =
                                usize::from(unsafe { *filter_matrix_row.get_unchecked(i) });
                            for (j, cell) in dimension_row.iter_mut().enumerate() {
                                let right_matrix_value =
                                    usize::from(unsafe { *filter_matrix_row.get_unchecked(j) });
                                *cell += left_matrix_value * (1 - right_matrix_value)
                                    + (1 - left_matrix_value) * right_matrix_value;
                            }
                        }
                    }
                }
            });
            notificator.for_each(|time, _cnt, _not| {
                if let Some(matrices) = times.remove(&time) {
                    if worker_index == 0 {
                        info!("Done ordering matrices processing");
                    }
                    output.session(&time).give(matrices);
                }
            });
        })
    }
}

#[cfg(test)]
mod tests {
    use super::MatrixOperation;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::Capture;
    use timely::dataflow::InputHandle;
    use timely::Configuration;

    #[test]
    fn test_matrix() {
        timely::execute(Configuration::Process(1), move |worker| {
            let mut filter_matrix_input = InputHandle::new();
            let result = worker.dataflow(|scope| {
                filter_matrix_input.to_stream(scope).create_ordering_matrices(vec![5], 0).capture()
            });
            let mut data = vec![];
            for i in 0..10 {
                data.push((i, vec![vec![0, 1, 0, 0, 0]]));
            }
            for i in 10..50 {
                data.push((i, vec![vec![0, 1, 1, 1, 0]]));
            }
            for i in 50..60 {
                data.push((i, vec![vec![0, 1, 1, 1, 1]]));
            }
            for i in 60..100 {
                data.push((i, vec![vec![0, 0, 1, 0, 1]]));
            }
            for i in 100..200 {
                data.push((i, vec![vec![0, 0, 1, 0, 1]]));
            }
            for d in data {
                filter_matrix_input.send(d);
            }
            filter_matrix_input.close();
            while worker.step() {}
            assert_eq!(
                result.extract()[0].1,
                vec![vec![vec![
                    vec![0, 60, 190, 50, 150],
                    vec![60, 0, 150, 10, 190],
                    vec![190, 150, 0, 140, 40],
                    vec![50, 10, 140, 0, 180],
                    vec![150, 190, 40, 180, 0]
                ]]]
            );
        })
        .expect("Timely error");
    }
}
