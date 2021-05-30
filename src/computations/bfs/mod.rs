use crate::computations::ComputationProperties;
use crate::error::GSError;
use crate::graph::properties::property_value::PropertyValue;
use gs_analytics_api::{ComputationTypes, TimelyComputation, VertexId};
use hashbrown::HashMap;
use std::convert::TryFrom;

mod differential_df;
mod differential_df_arranged;

const NAME: &str = "BFS";
const PROPERTY: &str = "root";
type BfsLength = usize;

#[derive(Clone)]
pub struct Bfs {
    root: VertexId,
}

impl Bfs {
    pub fn new(root: VertexId) -> Self {
        Self { root }
    }

    pub fn instance(properties: &HashMap<String, ComputationProperties>) -> Result<Self, GSError> {
        if properties.len() != 1 {
            return Err(GSError::PropertyCount(NAME, 1, vec![PROPERTY], properties.len()));
        }
        let value = properties.get(PROPERTY).ok_or_else(|| {
            GSError::Property(NAME, PROPERTY, properties.keys().cloned().collect::<Vec<_>>())
        })?;
        if let ComputationProperties::Value(PropertyValue::Isize(root)) = value {
            Ok(Self { root: VertexId::try_from(*root).expect("Root value cannot be negative") })
        } else {
            Err(GSError::PropertyType(NAME, PROPERTY, "usize(vertex id)", value.get_type()))
        }
    }
}

impl ComputationTypes for Bfs {
    type Result = (VertexId, BfsLength);
}

impl TimelyComputation for Bfs {
    type TimelyResult = ();
}
