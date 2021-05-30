use crate::computations::bfs::Bfs;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::Collection;
use gs_analytics_api::{ComputationInput, GraphsurgeComputation, PropertyInput};
use timely::dataflow::Scope;

impl GraphsurgeComputation for Bfs {
    /// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/examples/bfs.rs).
    fn graph_analytics<G: Scope>(
        &self,
        input_stream: &ComputationInput<G>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        let (collection_handle, roots_collection) =
            PropertyInput::new(input_stream.nodes.stream.scope());

        // initialize roots as reaching themselves at distance 0
        let nodes = roots_collection.map(|x| (x, 0));

        let result = nodes.iterate(|inner| {
            let edges = input_stream.edges.enter(&inner.scope());
            let nodes = nodes.enter(&inner.scope());

            inner
                .join_core(&edges, |_k, l, d| Some((*d, l + 1)))
                .concat(&nodes)
                .reduce(|_, s, t| t.push((*s[0].0, 1)))
        });

        collection_handle.send(self.root);

        result
    }
}
