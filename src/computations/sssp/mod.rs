use crate::computations::ComputationProperties;
use crate::error::GSError;
use crate::graph::properties::property_value::PropertyValue;
use gs_analytics_api::{ComputationTypes, TimelyComputation, VertexId};
use hashbrown::HashMap;
use std::convert::TryFrom;

mod differential_df;
mod differential_df_arranged;

const NAME: &str = "SSSP";
const PROPERTY: &str = "root";
const PROPERTY2: &str = "weight_cap";
type BFSLength = usize;

#[derive(Clone)]
pub struct Sssp {
    root: VertexId,
    weight_cap: usize,
}

impl Sssp {
    pub fn new(root: VertexId, weight_cap: usize) -> Self {
        Self { root, weight_cap }
    }

    pub fn instance(properties: &HashMap<String, ComputationProperties>) -> Result<Self, GSError> {
        if properties.len() != 2 {
            return Err(GSError::PropertyCount(
                NAME,
                2,
                vec![PROPERTY, PROPERTY2],
                properties.len(),
            ));
        }
        let keys = properties.keys().cloned().collect::<Vec<_>>();
        let value = properties
            .get(PROPERTY)
            .ok_or_else(|| GSError::Property(NAME, PROPERTY, keys.clone()))?;
        let value2 = properties.get(PROPERTY2).ok_or(GSError::Property(NAME, PROPERTY2, keys))?;
        let root;
        if let ComputationProperties::Value(PropertyValue::Isize(r)) = value {
            root = VertexId::try_from(*r).expect("Root value cannot be negative");
        } else {
            return Err(GSError::PropertyType(
                NAME,
                PROPERTY,
                "usize(vertex id)",
                value.get_type(),
            ));
        }
        let weight_cap;
        if let ComputationProperties::Value(PropertyValue::Isize(w)) = value2 {
            weight_cap = usize::try_from(*w).expect("Root value cannot be negative");
        } else {
            return Err(GSError::PropertyType(
                NAME,
                PROPERTY2,
                "usize(vertex id)",
                value.get_type(),
            ));
        }

        Ok(Self { root, weight_cap })
    }
}

impl ComputationTypes for Sssp {
    type Result = (VertexId, BFSLength);
}

impl TimelyComputation for Sssp {
    type TimelyResult = ();
}
