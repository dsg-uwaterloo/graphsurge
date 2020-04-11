use crate::computations::filtered_cubes::edge_diff::process_edge_diff;
use crate::computations::filtered_cubes::execute::get_results_stash;
use crate::computations::filtered_cubes::{DimensionOrders, FilteredMatrixStream};
use crate::computations::TimelyTimeStamp;
use crate::filtered_cubes::timestamp::timestamp_mappings::GSTimestampIndex;
use crate::filtered_cubes::{DimensionLength, FilteredCubeEntriesEdgeId};
use hashbrown::HashMap;
use itertools::Itertools;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

pub trait EdgeDiff<S: Scope<Timestamp = TimelyTimeStamp>> {
    fn process_edge(
        &self,
        orders: DimensionOrders,
        dimension_lengths: Vec<DimensionLength>,
        store_total_data: bool,
    ) -> Stream<S, Vec<(GSTimestampIndex, FilteredCubeEntriesEdgeId)>>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>> EdgeDiff<S> for Stream<S, FilteredMatrixStream> {
    fn process_edge(
        &self,
        orders: DimensionOrders,
        dimension_lengths: Vec<DimensionLength>,
        store_total_data: bool,
    ) -> Stream<S, Vec<(GSTimestampIndex, FilteredCubeEntriesEdgeId)>> {
        let mut times = HashMap::new();
        self.unary_notify(Pipeline, "ProcessEdge", None, move |input, output, notificator| {
            input.for_each(|time, input_data| {
                let mut stash = times.entry(time.time().clone()).or_insert_with(|| {
                    notificator.notify_at(time.retain());
                    get_results_stash(&orders, &dimension_lengths)
                });
                for edge in input_data.iter() {
                    process_edge_diff(edge, &mut stash, store_total_data);
                }
            });
            notificator.for_each(|time, _cnt, _not| {
                if let Some(stash) = times.remove(&time) {
                    output.session(&time).give(
                        stash
                            .into_iter()
                            .map(|diff_data| (diff_data.timestamp_index, diff_data.results))
                            .collect_vec(),
                    );
                }
            })
        })
    }
}
