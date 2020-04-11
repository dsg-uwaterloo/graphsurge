#![allow(clippy::unused_self)]

use crate::create_pointer;
use crate::error::GraphSurgeError;
use crate::graph::key_store::KeyId;
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::properties::Properties;
use abomonation_derive::Abomonation;
use gs_analytics_api::{EdgeId, VertexId};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::convert::TryFrom;
use std::iter::Enumerate;
use std::slice::Iter;

pub mod key_store;
pub mod properties;
pub mod serde;
pub mod stream_data;

#[derive(Default, Debug, Clone)]
pub struct Graph {
    edges: Vec<Edge>,
    vertices: Vec<Vertex>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum VertexOrEdge {
    Vertex,
    Edge,
}
pub type VertexOrEdgeId = VertexId;
pub type SrcVertexId = VertexOrEdgeId;
pub type DstVertexId = VertexOrEdgeId;
pub type TypeId = KeyId;
pub type VertexTypeId = TypeId;
pub type SrcVertexTypeId = TypeId;
pub type DstVertexTypeId = TypeId;
pub type EdgeTypeId = TypeId;
create_pointer!(GraphPointer, Graph);

#[derive(Debug, Clone, Abomonation, Serialize, Deserialize, new)]
pub struct Vertex {
    pub properties: Properties,
}

#[derive(Debug, Clone, Abomonation, Serialize, Deserialize, new)]
pub struct Edge {
    pub properties: Properties,
    pub src_vertex_id: VertexId,
    pub dst_vertex_id: VertexId,
}

impl Graph {
    pub fn reset(&mut self) {
        self.edges.clear();
        self.vertices.clear();
    }

    pub fn vertex_count(&self) -> VertexId {
        VertexId::try_from(self.vertices.len()).expect("Overflow")
    }

    pub fn edges_count(&self) -> EdgeId {
        EdgeId::try_from(self.edges.len()).expect("Overflow")
    }

    pub fn append_vertex(&mut self, vertex: Vertex) -> VertexId {
        self.vertices.push(vertex);
        VertexId::try_from(self.vertices.len() - 1).expect("Overflow")
    }

    pub fn append_edge(&mut self, edge: Edge) -> EdgeId {
        self.edges.push(edge);
        EdgeId::try_from(self.edges.len() - 1).expect("Overflow")
    }

    pub fn edge_iterator(&self) -> Enumerate<Iter<Edge>> {
        self.edges.iter().enumerate()
    }

    pub fn randomize_edges(&mut self) {
        let mut rng = thread_rng();
        self.edges.shuffle(&mut rng);
    }

    #[inline(always)]
    fn get_vertex(&self, vertex_id: VertexId) -> &Vertex {
        &self.vertices[vertex_id as usize]
    }

    #[inline(always)]
    fn get_edge(&self, edge_id: EdgeId) -> &Edge {
        &self.edges[edge_id as usize]
    }

    #[inline(always)]
    pub fn get_vertex_id_property_value<'a>(
        &'a self,
        vertex_id: VertexId,
        vertex_id_pv: &'a PropertyValue,
        key_id: KeyId,
    ) -> Option<&'a PropertyValue> {
        let vertex = self.get_vertex(vertex_id);
        self.get_vertex_property_value(vertex, vertex_id_pv, key_id)
    }

    #[inline(always)]
    #[allow(clippy::unused_self)]
    fn get_vertex_property_value<'a>(
        &'a self,
        vertex: &'a Vertex,
        vertex_id: &'a PropertyValue,
        key_id: KeyId,
    ) -> Option<&'a PropertyValue> {
        vertex.properties.get_property(vertex_id, key_id)
    }

    #[inline(always)]
    fn get_edge_id_property_value<'a>(
        &'a self,
        edge_id: EdgeId,
        edge_id_pv: &'a PropertyValue,
        key_id: KeyId,
    ) -> Option<&'a PropertyValue> {
        let edge = self.get_edge(edge_id);
        self.get_edge_property_value(edge, edge_id_pv, key_id)
    }

    #[inline(always)]
    fn get_edge_property_value<'a>(
        &'a self,
        edge: &'a Edge,
        edge_id: &'a PropertyValue,
        key_id: KeyId,
    ) -> Option<&'a PropertyValue> {
        edge.properties.get_property(edge_id, key_id)
    }

    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    pub fn vertices(&self) -> &[Vertex] {
        &self.vertices
    }

    pub fn serialize(
        &self,
        bin_dir: &str,
        thread_count: usize,
        block_size: Option<usize>,
    ) -> Result<(), GraphSurgeError> {
        self::serde::serialize(self, bin_dir, thread_count, block_size)
    }

    pub fn deserialize(
        &mut self,
        bin_dir: &str,
        thread_count: usize,
    ) -> Result<(), GraphSurgeError> {
        self::serde::deserialize(self, bin_dir, thread_count)
    }
}
