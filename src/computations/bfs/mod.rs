use crate::computations::ComputationProperties;
use crate::error::computation_error;
use crate::error::GraphSurgeError;
use crate::graph::properties::property_value::PropertyValue;
use gs_analytics_api::{ComputationTypes, VertexId};
use hashbrown::HashMap;
use std::convert::TryFrom;

mod differential_df;
mod differential_df_arranged;

type BFSLength = usize;

#[derive(Clone)]
pub struct BFS {
    root: VertexId,
}

impl BFS {
    pub fn new(root: VertexId) -> Self {
        Self { root }
    }

    pub fn instance(
        properties: &HashMap<String, ComputationProperties>,
    ) -> Result<Self, GraphSurgeError> {
        let property = "root";
        if properties.len() != 1 {
            return Err(computation_error(format!(
                "BFS needs one property '{}', but found {} properties",
                property,
                properties.len()
            )));
        }
        let value = properties.get(property).ok_or_else(|| {
            computation_error(format!(
                "BFS needs property '{}' but found '{:?}'",
                property,
                properties.keys().collect::<Vec<_>>()
            ))
        })?;
        if let ComputationProperties::Value(PropertyValue::Isize(root)) = value {
            Ok(Self { root: VertexId::try_from(*root).expect("Root value cannot be negative") })
        } else {
            Err(computation_error(format!(
                "BFS {} should be a Usize(vertex id) but found '{}'",
                property,
                value.get_type()
            )))
        }
    }
}

impl ComputationTypes for BFS {
    type Result = (VertexId, BFSLength);
}
