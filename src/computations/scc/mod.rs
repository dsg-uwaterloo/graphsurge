use crate::computations::ComputationProperties;
use crate::error::computation_error;
use crate::error::GraphSurgeError;
use gs_analytics_api::{ComputationTypes, VertexId};
use hashbrown::HashMap;

mod differential_df;
mod differential_df_arranged;

#[derive(Clone)]
pub struct SCC;

impl SCC {
    pub fn instance(
        properties: &HashMap<String, ComputationProperties>,
    ) -> Result<Self, GraphSurgeError> {
        if properties.is_empty() {
            Ok(Self {})
        } else {
            Err(computation_error(format!(
                "SCC does not need any property, but found {} properties",
                properties.len()
            )))
        }
    }
}

impl ComputationTypes for SCC {
    type Result = (VertexId, VertexId);
}
