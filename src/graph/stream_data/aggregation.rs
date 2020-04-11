use crate::graph::key_store::{KeyId, KeyStore};
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::GraphPointer;
use crate::graph::VertexOrEdgeId;
use abomonation_derive::Abomonation;
use gs_analytics_api::EdgeId;
use std::convert::TryFrom;

#[derive(Debug, Clone, Copy, Abomonation, Eq, PartialEq)]
pub enum AggregationOperation {
    Count,
    Avg,
}

pub fn get_aggregates(
    operation: AggregationOperation,
    property_key_id: KeyId,
    data: &[VertexOrEdgeId],
    graph_pointer: GraphPointer,
) -> PropertyValue {
    match operation {
        AggregationOperation::Count => {
            let count = if KeyStore::is_defined_constant(property_key_id) {
                data.len()
            } else {
                data.iter()
                    .filter(|&&vertex_id| {
                        let pv = PropertyValue::get_id(vertex_id);
                        graph_pointer
                            .get_vertex_id_property_value(vertex_id, &pv, property_key_id)
                            .is_some()
                    })
                    .count()
            };
            PropertyValue::Isize(isize::try_from(count).expect("Count value overflow"))
        }
        AggregationOperation::Avg => {
            let mut count = 0;
            let mut sum = 0;
            for &vertex_id in data.iter() {
                let pv = PropertyValue::get_id(vertex_id);
                if let Some(PropertyValue::Isize(value)) =
                    graph_pointer.get_vertex_id_property_value(vertex_id, &pv, property_key_id)
                {
                    sum += value;
                    count += 1;
                }
            }
            PropertyValue::Pair(sum, count)
        }
    }
}

pub fn get_edge_aggregates(
    operation: AggregationOperation,
    property_key_id: KeyId,
    edges: &[EdgeId],
    graph_pointer: GraphPointer,
) -> PropertyValue {
    match operation {
        AggregationOperation::Count => {
            let count = if KeyStore::is_defined_constant(property_key_id) {
                edges.len()
            } else {
                edges
                    .iter()
                    .filter(|&&edge_id| {
                        let pv = PropertyValue::get_id(edge_id);
                        graph_pointer
                            .get_edge_id_property_value(edge_id, &pv, property_key_id)
                            .is_some()
                    })
                    .count()
            };
            PropertyValue::Isize(isize::try_from(count).expect("Count value overflow"))
        }
        AggregationOperation::Avg => {
            let mut count = 0;
            let mut sum = 0;
            for &edge_id in edges.iter() {
                let pv = PropertyValue::get_id(edge_id);
                if let Some(PropertyValue::Isize(value)) =
                    graph_pointer.get_edge_id_property_value(edge_id, &pv, property_key_id)
                {
                    sum += value;
                    count += 1;
                }
            }
            PropertyValue::Pair(sum, count)
        }
    }
}
