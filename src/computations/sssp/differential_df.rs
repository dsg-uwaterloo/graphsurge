use crate::computations::sssp::Sssp;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use gs_analytics_api::{BasicComputation, SimpleEdge};
use timely::dataflow::Scope;

impl BasicComputation for Sssp {
    /// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/examples/bfs.rs).
    fn basic_computation<G: Scope>(
        &self,
        _edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        unreachable!()
    }
}
