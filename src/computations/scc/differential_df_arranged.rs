use crate::computations::scc::Scc;
use differential_dataflow::algorithms::graphs::propagate::propagate;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Consolidate, JoinCore};
use differential_dataflow::Collection;
use gs_analytics_api::{
    ComputationInput, ComputationTypes, EdgeArrangementEnter, GraphsurgeComputation,
    TimelyTimeStamp,
};
use timely::dataflow::Scope;
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

impl GraphsurgeComputation for Scc {
    /// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/graphs/scc.rs).
    fn graph_analytics<G: Scope>(
        &self,
        input_stream: &ComputationInput<G>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        input_stream.edges.stream.scope().iterative::<TimelyTimeStamp, _, _>(|scope| {
            let edges = input_stream.edges.enter(scope);
            let reverse = input_stream.reverse_edges.enter(scope);

            let inner = Variable::new_from(
                edges.flat_map_ref(|&src, &dst| Some((src, dst))),
                Product::new(Default::default(), 1),
            );

            let result = trim_edges(&trim_edges(&*inner, &edges), &reverse);

            inner.set(&result);
            result.leave()
        })
    }
}

/// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/graphs/scc.rs).
fn trim_edges<G, T>(
    cycle: &Collection<G, <Scc as ComputationTypes>::Result>,
    edges: &EdgeArrangementEnter<G, T>,
) -> Collection<G, <Scc as ComputationTypes>::Result>
where
    G: Scope,
    G::Timestamp: Lattice + Ord + Refines<T>,
    T: Timestamp + Lattice + Ord,
{
    let nodes = edges.flat_map_ref(|_, &dst| Some((dst, dst))).consolidate();

    // NOTE: With a node -> int function, can be improved by:
    // let labels = propagate_at(&cycle, &nodes, |x| *x as u64);
    let labels = propagate(&cycle, &nodes).arrange_by_key();

    edges
        .join_core(&labels, |&e1, &e2, &l1| Some((e2, (e1, l1))))
        .join_core(&labels, |&e2, &(e1, l1), &l2| Some(((e1, e2), (l1, l2))))
        .filter(|(_, (l1, l2))| l1 == l2)
        .map(|((x1, x2), _)| (x2, x1))
}
