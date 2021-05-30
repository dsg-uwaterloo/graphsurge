use crate::{DiffCount, VertexId};
use derive_new::new;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::ord::{OrdKeyBatch, OrdValBatch};
use differential_dataflow::trace::implementations::spine_fueled_neu::Spine;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use hashbrown::HashSet;
use log::info;
use std::rc::Rc;
use timely::dataflow::{Scope, ScopeParent};
use timely::Configuration;

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ComputationType {
    TwoStageDifferential,
    OneStageDifferential,
    Adaptive,
    Basic,
    Timely,
    IndividualBasic,
    Individual,
    CompareDifferential,
}

impl ComputationType {
    pub fn description(self) -> &'static str {
        match self {
            ComputationType::TwoStageDifferential => "differential-2-stage",
            ComputationType::OneStageDifferential => "differential-1-stage",
            ComputationType::Adaptive => "adaptive",
            ComputationType::Basic => "differential-basic",
            ComputationType::IndividualBasic => "differential-individual-basic",
            ComputationType::Individual => "differential-individual",
            ComputationType::CompareDifferential => "compare-differential",
            ComputationType::Timely => "timely",
        }
    }
}

impl std::fmt::Display for ComputationType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            if let ComputationType::CompareDifferential = self {
                String::new()
            } else {
                format!(" {:?}", self).to_lowercase()
            }
        )
    }
}

impl Default for ComputationType {
    fn default() -> Self {
        ComputationType::Basic
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum MaterializeResults {
    None,
    Diff,
    Full,
}

impl Default for MaterializeResults {
    fn default() -> Self {
        MaterializeResults::None
    }
}

#[allow(clippy::too_many_arguments)]
#[derive(new, Clone, Default)]
pub struct ComputationRuntimeData {
    pub c_type: ComputationType,
    pub total_vertices: usize,
    pub materialize_results: MaterializeResults,
    pub save_to: Option<String>,
    pub threads: usize,
    pub process_id: usize,
    pub hosts: Vec<String>,
    pub splits: Option<HashSet<usize>>,
    pub batch_size: Option<usize>,
    pub comp_multipler: Option<f64>,
    pub diff_multipler: Option<f64>,
    pub limit: usize,
    pub use_lr: bool,
}

impl ComputationRuntimeData {
    pub fn timely_config(&self) -> Configuration {
        if self.hosts.len() > 1 {
            let c_hosts = self.hosts.to_vec();
            info!("Process {} w/ {} threads, hosts = {:?}", self.process_id, self.threads, c_hosts);
            Configuration::Cluster {
                threads: self.threads,
                process: self.process_id,
                addresses: c_hosts,
                report: false,
                log_fn: Box::new(|_| None),
            }
        } else {
            info!("Process {} w/ {} threads", self.process_id, self.threads);
            Configuration::Process(self.threads)
        }
    }

    pub fn should_materialize_results(&self) -> bool {
        self.materialize_results != MaterializeResults::None
    }
}
