use crate::computations::wcc::Wcc;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::Collection;
use gs_analytics_api::{BasicComputation, SimpleEdge};
use timely::dataflow::Scope;

impl BasicComputation for Wcc {
    fn basic_computation<G: Scope>(
        &self,
        edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        let nodes = edges
            .map_in_place(|pair| {
                let min = std::cmp::min(pair.0, pair.1);
                *pair = (min, min);
            })
            .consolidate();

        // each edge should exist in both directions.
        let edges = edges.map_in_place(|x| std::mem::swap(&mut x.0, &mut x.1)).concat(&edges);

        // don't actually use these labels, just grab the type
        nodes.filter(|_| false).iterate(|inner| {
            let edges = edges.enter(&inner.scope());
            let nodes =
                nodes.enter_at(&inner.scope(), |r| 256 * (64 - u64::from(r.1.leading_zeros())));

            inner.join_map(&edges, |_k, l, d| (*d, *l)).concat(&nodes).reduce(|_, s, t| {
                t.push((*s[0].0, 1));
            })
        })
    }
}
