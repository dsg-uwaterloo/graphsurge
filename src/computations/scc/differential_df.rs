use crate::computations::scc::Scc;
use differential_dataflow::algorithms::graphs::scc::strongly_connected;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use gs_analytics_api::{BasicComputation, SimpleEdge};
use timely::dataflow::Scope;

impl BasicComputation for Scc {
    fn basic_computation<G: Scope>(
        &self,
        edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        strongly_connected(edges)
    }
}
