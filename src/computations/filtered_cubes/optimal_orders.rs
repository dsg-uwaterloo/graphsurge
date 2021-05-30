use crate::computations::filtered_cubes::tsp::tsp;
use crate::computations::filtered_cubes::{DimensionOrders, Matrices, Matrix};
use gs_analytics_api::TimelyTimeStamp;
use itertools::Itertools;
use timely::dataflow::operators::map::Map;
use timely::dataflow::{Scope, Stream};

pub trait OptimalOrder<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn generate_optimal_orders(&self) -> Stream<S, DimensionOrders>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> OptimalOrder<S> for Stream<S, Matrices> {
    fn generate_optimal_orders(&self) -> Stream<S, DimensionOrders> {
        self.map(move |matrix| get_optimal_order(&matrix))
    }
}

fn get_optimal_order(matrices: &[Matrix]) -> DimensionOrders {
    matrices
        .iter()
        .map(|matrix| {
            let mut tsp_results = tsp(matrix);
            tsp_results.retain(|value| *value != 0);
            tsp_results
        })
        .collect_vec()
}
