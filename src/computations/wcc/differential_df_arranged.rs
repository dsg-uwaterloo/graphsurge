use crate::computations::wcc::WCC;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::Collection;
use gs_analytics_api::{ComputationInput, GraphsurgeComputation, VertexId};
use std::convert::TryFrom;
use timely::dataflow::operators::{Concat, Delay, Map};
use timely::dataflow::Scope;
use timely::order::Product;

impl GraphsurgeComputation for WCC {
    /// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/experiments/src/bin/graphs-static.rs).
    fn graph_analytics<G: Scope>(
        &self,
        input_stream: &ComputationInput<G>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        let vertex_id_bits =
            u32::try_from(std::mem::size_of::<VertexId>() * 8).expect("VertexId Overflow");

        input_stream.edges.stream.scope().iterative(|scope| {
            // import arrangements, nodes.
            let forward = input_stream.edges.enter(scope);
            let reverse = input_stream.reverse_edges.enter(scope);
            let nodes = input_stream.nodes.enter(scope).as_collection(|&src, &()| (src, src));

            let inner = Variable::new(scope, Product::new(Default::default(), 1));

            let labels = inner.arrange_by_key();
            let f_prop = labels.join_core(&forward, |_k, l, d| Some((*d, *l)));
            let r_prop = labels.join_core(&reverse, |_k, l, d| Some((*d, *l)));

            let result = nodes
                .inner
                .map_in_place(move |dtr| {
                    (dtr.1).inner = 256 * (vertex_id_bits - (dtr.0).1.leading_zeros())
                })
                .concat(&inner.filter(|_| false).inner)
                .delay(|dtr, _| dtr.1)
                .as_collection()
                .concat(&f_prop)
                .concat(&r_prop)
                .reduce(|_, s, t| {
                    t.push((*s[0].0, 1));
                });

            inner.set(&result);
            result.leave()
        })
    }
}
