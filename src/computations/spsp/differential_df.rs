use crate::computations::spsp::{bidijkstra_arranged, Spsp};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::Collection;
use gs_analytics_api::{BasicComputation, PropertyInput, SimpleEdge};
use timely::dataflow::Scope;

impl BasicComputation for Spsp {
    fn basic_computation<G: Scope>(
        &self,
        edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        let (collection_handle, goals) = PropertyInput::new(edges.scope());

        let forward = edges.arrange_by_key();
        let reverse = edges.map(|(x, y)| (y, x)).arrange_by_key();
        let result = bidijkstra_arranged(&forward, &reverse, &goals);

        collection_handle.send_iter(self.goals.iter().copied());

        result
    }
}
