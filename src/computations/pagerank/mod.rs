use crate::computations::ComputationProperties;
use crate::error::GSError;
use crate::graph::properties::property_value::PropertyValue;
use gs_analytics_api::{ComputationTypes, TimelyComputation, TimelyTimeStamp, VertexId};
use hashbrown::HashMap;
use std::convert::TryFrom;

mod differential_df;
mod differential_df_arranged;

const NAME: &str = "PageRank";
const PROPERTY: &str = "iterations";

#[derive(Clone)]
pub struct PageRank {
    iterations: TimelyTimeStamp,
}

impl PageRank {
    pub fn instance(properties: &HashMap<String, ComputationProperties>) -> Result<Self, GSError> {
        if properties.len() != 1 {
            return Err(GSError::PropertyCount(NAME, 1, vec![PROPERTY], properties.len()));
        }
        let value = properties.get(PROPERTY).ok_or_else(|| {
            GSError::Property(NAME, PROPERTY, properties.keys().cloned().collect::<Vec<_>>())
        })?;
        if let ComputationProperties::Value(PropertyValue::Isize(iterations)) = value {
            Ok(Self {
                iterations: TimelyTimeStamp::try_from(*iterations)
                    .expect("Iterations value overflow"),
            })
        } else {
            Err(GSError::PropertyType(NAME, PROPERTY, "isize(iterations)", value.get_type()))
        }
    }
}

impl ComputationTypes for PageRank {
    type Result = VertexId;
}

impl TimelyComputation for PageRank {
    type TimelyResult = ();
}
