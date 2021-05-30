use crate::computations::spsp::{bidijkstra_arranged, Spsp};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use gs_analytics_api::{ComputationInput, GraphsurgeComputation, PropertyInput};
use timely::dataflow::Scope;

impl GraphsurgeComputation for Spsp {
    fn graph_analytics<G: Scope>(
        &self,
        input_stream: &ComputationInput<G>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        let (collection_handle, goals) = PropertyInput::new(input_stream.edges.stream.scope());

        let result = bidijkstra_arranged(&input_stream.edges, &input_stream.reverse_edges, &goals);

        collection_handle.send_iter(self.goals.iter().copied());

        result
    }
}
