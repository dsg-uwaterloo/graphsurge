use crate::computations::pagerank::PageRank;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Consolidate, Count, Join, Threshold};
use differential_dataflow::Collection;
use gs_analytics_api::{ComputationInput, GraphsurgeComputation, TimelyTimeStamp};
use timely::dataflow::operators::Filter;
use timely::dataflow::Scope;
use timely::order::Product;

impl GraphsurgeComputation for PageRank {
    /// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/examples/pagerank.rs).
    fn graph_analytics<G: Scope>(
        &self,
        input_stream: &ComputationInput<G>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        #[allow(clippy::complexity)]
        // snag out-degrees for each node.
        let degrs = input_stream.edges.as_collection(|src, _dst| *src).count();

        input_stream.edges.stream.scope().iterative::<TimelyTimeStamp, _, _>(|inner| {
            // Bring various collections into the scope.
            let edges = input_stream.edges.enter(inner);
            let nodes = input_stream.nodes.enter(inner);
            let degrs = degrs.enter(inner);

            // Initial and reset numbers of surfers at each node.
            let inits = nodes.as_collection(|src, ()| *src).explode(|node| Some((node, 6_000_000)));
            let reset = nodes.as_collection(|src, ()| *src).explode(|node| Some((node, 1_000_000)));

            // Define a recursive variable to track surfers.
            // We start from `inits` and cycle only `iters`.
            let ranks = Variable::new_from(inits, Product::new(Default::default(), 1));

            // Match each surfer with the degree, scale numbers down.
            let to_push = degrs
                .semijoin(&ranks)
                .threshold(|(_node, degr), rank| (5 * rank) / (6 * degr))
                .map(|(node, _degr)| node);

            // Propagate surfers along links, blend in reset surfers.
            let mut pushed =
                edges.semijoin(&to_push).map(|(_node, dest)| dest).concat(&reset).consolidate();

            let iterations = self.iterations;
            if iterations > 0 {
                pushed =
                    pushed.inner.filter(move |(_d, t, _r)| t.inner < iterations).as_collection();
            }

            // Bind the recursive variable, return its limit.
            ranks.set(&pushed);
            pushed.leave()
        })
    }
}
