use crate::computations::pagerank::PageRank;
use crate::computations::TimelyTimeStamp;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Consolidate, Count, Join, Threshold};
use differential_dataflow::Collection;
use gs_analytics_api::{BasicComputation, DiffCount, SimpleEdge, VertexId};
use timely::dataflow::operators::Filter;
use timely::dataflow::Scope;
use timely::order::Product;

impl BasicComputation for PageRank {
    fn basic_computation<G: Scope>(
        &self,
        edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        pagerank(self.iterations, &edges).consolidate()
    }
}

/// Returns a weighted collection in which the weight of each node is proportional
/// to its pagerank in the input graph `edges`.
///
/// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/examples/pagerank.rs).
fn pagerank<G>(
    iterations: TimelyTimeStamp,
    edges: &Collection<G, SimpleEdge, DiffCount>,
) -> Collection<G, VertexId, DiffCount>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // initialize many surfers at each node.
    let nodes = edges.flat_map(|(x, y)| Some(x).into_iter().chain(Some(y))).distinct();

    // snag out-degrees for each node.
    #[allow(clippy::suspicious_map)]
    let degrs = edges.map(|(src, _dst)| src).count();

    edges.scope().iterative::<usize, _, _>(|inner| {
        // Bring various collections into the scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);
        let degrs = degrs.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = nodes.explode(|node| Some((node, 6_000_000)));
        let reset = nodes.explode(|node| Some((node, 1_000_000)));

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

        if iterations > 0 {
            pushed = pushed.inner.filter(move |(_d, t, _r)| t.inner < iterations).as_collection();
        }

        // Bind the recursive variable, return its limit.
        ranks.set(&pushed);
        pushed.leave()
    })
}
