use crate::computations::ComputationProperties;
use crate::error::GSError;
use gs_analytics_api::{ComputationTypes, TimelyComputation, VertexId};
use hashbrown::HashMap;

mod differential_df;
mod differential_df_arranged;

const NAME: &str = "SCC";

#[derive(Clone)]
pub struct Scc;

impl Scc {
    pub fn instance(properties: &HashMap<String, ComputationProperties>) -> Result<Self, GSError> {
        if properties.is_empty() {
            Ok(Self {})
        } else {
            Err(GSError::PropertyCount(NAME, 0, vec![], properties.len()))
        }
    }
}

impl ComputationTypes for Scc {
    type Result = (VertexId, VertexId);
}

impl TimelyComputation for Scc {
    type TimelyResult = ();
}
