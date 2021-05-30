use crate::computations::bfs::Bfs;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::Collection;
use gs_analytics_api::{BasicComputation, PropertyInput, SimpleEdge};
use timely::dataflow::Scope;

impl BasicComputation for Bfs {
    /// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/examples/bfs.rs).
    fn basic_computation<G: Scope>(
        &self,
        edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        let (collection_handle, roots_collection) = PropertyInput::new(edges.scope());

        // initialize roots as reaching themselves at distance 0
        let nodes = roots_collection.map(|x| (x, 0));

        // repeatedly update minimal distances each node can be reached from each root
        let result = nodes.iterate(|inner| {
            let edges = edges.enter(&inner.scope());
            let nodes = nodes.enter(&inner.scope());

            inner
                .join_map(&edges, |_k, l, d| (*d, l + 1))
                .concat(&nodes)
                .reduce(|_, s, t| t.push((*s[0].0, 1)))
        });

        collection_handle.send(self.root);

        result
    }
}
