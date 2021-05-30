use crate::graph::properties::property_value::PropertyValue;
use abomonation_derive::Abomonation;
use gs_analytics_api::{EdgeId, TimelyTimeStamp, VertexId};
use timely::order::Product;

mod aggregate_edges;
mod aggregate_vertices;
pub mod execute;
mod filter_group_map_vertices;
mod filter_map_edges;
mod group_reduce_vertices;
mod map_edges_to_groups;
pub mod monitor;
mod reduce_edges;
mod tree_aggregate_vertices;
mod tree_map_edges_to_groups;
mod tree_map_vertices;
mod tree_reduce_edges;
mod tree_reduce_vertices;

const FIRST_SECTION_INDEX: u8 = 1_u8;
const NULL_SECTION_INDEX: u8 = 0_u8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionType {
    SingleView,
    AggregatedCube(GroupsLength),
}
type GroupsLength = u8;
type GroupValue = Vec<String>;
type QueryProperties = Vec<QueryProperty>;
type QueryProperty = (String, PropertyValue);
type CreatedVertexId = VertexId;
type SectionIndex = u8;
type VertexGroupMapOutput = (SectionIndex, GroupValue, VertexId);
type VertexGroupReduceOutput = (SectionIndex, CreatedVertexId, GroupValue, Vec<VertexId>);
type AggregatedVertexOutput = (CreatedVertexId, QueryProperties, GroupValue);
type FilterMapEdgesOutput = (SectionIndex, (SectionIndex, SectionIndex), EdgeId);
type VertexReverseGroupOutput = (VertexId, SectionIndex, CreatedVertexId);
type EdgeMapOutput = (EdgeMapState, (CreatedVertexId, CreatedVertexId), FilterMapEdgesOutput);
type ReducedEdge = Vec<(SectionIndex, Vec<EdgeId>)>;
type EdgeReduceOutput = ((CreatedVertexId, CreatedVertexId), ReducedEdge);
#[derive(Debug, Clone, Abomonation, PartialEq, Eq)]
enum EdgeMapState {
    None,
    SrcOnly(u8),
    Done,
}
type TimelyInnerTimeStamp = Product<TimelyTimeStamp, TimelyTimeStamp>;
type AggregatedOutput = Vec<(SectionIndex, QueryProperties)>;
type AggregatedEdgeOutput = ((CreatedVertexId, CreatedVertexId), AggregatedOutput);
type OrderIndex = SectionIndex;
type TreeVertexMapOutput = (CreatedVertexId, OrderIndex, QueryProperties, GroupValue);
type TreeVertexReduceOutput =
    (CreatedVertexId, OrderIndex, GroupValue, Vec<(CreatedVertexId, QueryProperties)>);
type TreeAggregatedVertexOutput = (OrderIndex, (CreatedVertexId, QueryProperties, GroupValue));
type TreeVertexReverseGroupOutput = (CreatedVertexId, CreatedVertexId);
type TreeEdgeMapOutput = (EdgeMapState, (CreatedVertexId, CreatedVertexId), AggregatedEdgeOutput);
pub type VertexViewOutput = AggregatedVertexOutput;
pub type EdgeViewOutput = AggregatedEdgeOutput;
