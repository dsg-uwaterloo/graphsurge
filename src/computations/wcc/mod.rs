use crate::computations::ComputationProperties;
use crate::error::GSError;
use gs_analytics_api::{ComputationTypes, TimelyComputation, VertexId};
use hashbrown::HashMap;

mod differential_df;
mod differential_df_arranged;

const NAME: &str = "WCC";

#[derive(Clone)]
pub struct Wcc;

impl Wcc {
    pub fn instance(properties: &HashMap<String, ComputationProperties>) -> Result<Self, GSError> {
        if properties.is_empty() {
            Ok(Self {})
        } else {
            Err(GSError::PropertyCount(NAME, 0, vec![], properties.len()))
        }
    }
}

impl ComputationTypes for Wcc {
    type Result = (VertexId, VertexId);
}

impl TimelyComputation for Wcc {
    type TimelyResult = ();
}
