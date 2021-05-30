mod computation_input;
mod property_input;

// Public exports from root of the crate.
pub use computation_input::*;
pub use property_input::PropertyInput;

use derive_new::new;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use hashbrown::HashMap;
use serde_derive::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::ExchangeData;

// Universally used types.
pub type VertexId = u32;
pub type EdgeId = u32;
pub type DiffCount = isize;
pub type SimpleEdge = (VertexId, VertexId);
pub type TimelyTimeStamp = u32;
pub type GsTimestampIndex = usize;

#[derive(Serialize, Deserialize, new, Clone)]
pub struct FilteredCubeData<T> {
    pub entries: Vec<CubeDataEntries<T>>,
}
pub type CubeDataEntries<T> = (GsTimestampIndex, T, FilteredCubeEntries, (usize, usize));
pub type FilteredCubeEntries = (Vec<SimpleEdge>, Vec<(SimpleEdge, DiffCount)>);

/// This trait is used to define the output type of a computation.
///
/// Different computations will have different output types, depending on the individual computation
/// results, and this trait helps capture those types in a generic way for in function parameters
/// and returns.
///
/// # Examples
/// ```notest
/// type VertexId = u32;
/// type BFSLength = u16;
///
/// impl ComputationTypes for BFS {
///     type Result = (VertexId, BFSLength);
/// }
/// ```
pub trait ComputationTypes {
    /// The data type of the output of the implementing computation.
    type Result: Eq + Default + Hash + Ord + Debug + Copy + Send + Sync + ExchangeData;
}

/// The primary trait of the Graphsurge computation API, used to define a computation dataflow.
///
/// `ComputationInput` internally encapsulates `Arrangements` of the vertices and edges of the
/// input graph, to serve as inputs to the computation.
///
/// Computations that need extra inputs (e.g., BFS needs a starting `root`) can use `PropertyInput`
/// to manage defining and adding such inputs to the dataflow.
pub trait GraphsurgeComputation: ComputationTypes {
    fn graph_analytics<G: Scope>(
        &self,
        input_stream: &ComputationInput<G>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy;
}

/// A secondary trait that is used to define computations that want to the use the higher level
/// differential operators such as `join` and `reduce` instead of their arranged counterparts.
pub trait BasicComputation: ComputationTypes + Send + Sync + Clone + 'static {
    fn basic_computation<G: Scope>(
        &self,
        _edges: &Collection<G, SimpleEdge>,
    ) -> Collection<G, Self::Result>
    where
        G::Timestamp: Lattice + Ord + Copy,
    {
        unimplemented!()
    }
}

/// Timely implementation. Mostly unused for now.
pub trait TimelyComputation: ComputationTypes {
    type TimelyResult: Debug;
    fn timely_computation<T: GsTs>(
        &self,
        _cube: &FilteredCubeData<T>,
        _runtime_data: &ComputationRuntimeData,
    ) -> HashMap<T, Vec<Self::TimelyResult>> {
        unimplemented!()
    }
}

pub trait GsTs: Copy + Timestamp {
    fn next(&self) -> Self;
}

macro_rules! implement_nextts {
    ($($index_type:ty,)*) => (
        $(
            impl GsTs for $index_type {
                fn next(&self) -> Self {
                    self.checked_add(1).expect("Error: TS.next() overflow")
                }
            }
        )*
    )
}
implement_nextts!(usize, u128, u64, u32, u16, u8, i32, i64,);
