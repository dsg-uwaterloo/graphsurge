use crate::computations::{ComputationProperties, TimelyTimeStamp};
use crate::error::computation_error;
use crate::error::GraphSurgeError;
use crate::graph::properties::property_value::PropertyValue;
use gs_analytics_api::{ComputationTypes, VertexId};
use hashbrown::HashMap;
use std::convert::TryFrom;

mod differential_df;
mod differential_df_arranged;

#[derive(Clone)]
pub struct PageRank {
    iterations: TimelyTimeStamp,
}

impl PageRank {
    pub fn instance(
        properties: &HashMap<String, ComputationProperties>,
    ) -> Result<Self, GraphSurgeError> {
        let property = "iterations";
        if properties.len() != 1 {
            return Err(computation_error(format!(
                "PageRank needs one property '{}', but found {} properties",
                property,
                properties.len()
            )));
        }
        let value = properties.get(property).ok_or_else(|| {
            computation_error(format!(
                "PageRank needs property '{}' but found '{:?}'",
                property,
                properties.keys().collect::<Vec<_>>()
            ))
        })?;
        if let ComputationProperties::Value(PropertyValue::Isize(iterations)) = value {
            Ok(Self {
                iterations: TimelyTimeStamp::try_from(*iterations)
                    .expect("Iterations value overflow"),
            })
        } else {
            Err(computation_error(format!(
                "PageRank {} should be a Isize(iterations) but found '{}'",
                property,
                value.get_type()
            )))
        }
    }
}

impl ComputationTypes for PageRank {
    type Result = VertexId;
}
