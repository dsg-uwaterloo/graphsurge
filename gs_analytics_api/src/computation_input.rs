use crate::{DiffCount, VertexId};
use derive_new::new;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::ord::{OrdKeyBatch, OrdValBatch};
use differential_dataflow::trace::implementations::spine_fueled_neu::Spine;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use std::rc::Rc;
use timely::dataflow::{Scope, ScopeParent};

pub type EdgeTrace<T> =
    Spine<VertexId, VertexId, T, DiffCount, Rc<OrdValBatch<VertexId, VertexId, T, DiffCount>>>;
pub type NodeTrace<T> = Spine<VertexId, (), T, DiffCount, Rc<OrdKeyBatch<VertexId, T, DiffCount>>>;
pub type NodeArrangement<G> = Arranged<G, TraceAgent<NodeTrace<<G as ScopeParent>::Timestamp>>>;
pub type EdgeArrangement<G> = Arranged<G, TraceAgent<EdgeTrace<<G as ScopeParent>::Timestamp>>>;
pub type EdgeArrangementEnter<G, T> =
    Arranged<G, TraceEnter<TraceAgent<EdgeTrace<T>>, <G as ScopeParent>::Timestamp>>;

/// Encapsulates the primary input to computations, containing pre-arranged nodes and edges in both
/// forward and reverse directions.
///
/// Note that the input graph is always assumed to be directed. To operate on an undirected graph,
/// concat `edges` and `reverse_edges` into a single collection.
#[derive(new)]
pub struct ComputationInput<G: Scope>
where
    G::Timestamp: Lattice,
{
    /// Pre-arranged nodes.
    pub nodes: NodeArrangement<G>,
    /// Pre-arranged edges in the forward direction.
    pub edges: EdgeArrangement<G>,
    /// Pre-arranged edges in reverse direction.
    pub reverse_edges: EdgeArrangement<G>,
}
