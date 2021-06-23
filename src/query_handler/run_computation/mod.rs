use crate::computations::{ComputationProperties, SplitIndices};
use gs_analytics_api::{ComputationType, MaterializeResults};
use hashbrown::HashMap;

pub mod executor;

#[derive(new)]
pub struct RunComputationAst {
    computation: String,
    file: Option<String>,
    properties: HashMap<String, ComputationProperties>,
    cube: String,
    c_type: ComputationType,
    materialize_results: MaterializeResults,
    save_to: Option<String>,
    hosts: Vec<String>,
    splits: Option<SplitIndices>,
    batch_size: Option<usize>,
    comp_multipler: Option<f64>,
    diff_multipler: Option<f64>,
    limit: usize,
    use_lr: bool,
}

impl std::fmt::Display for RunComputationAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "run{} computation {}{} on cube {}",
            self.c_type.to_string(),
            self.computation,
            if self.properties.is_empty() {
                String::new()
            } else {
                format!(
                    "({})",
                    self.properties
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect::<Vec<String>>()
                        .join(",")
                )
            },
            self.cube,
        )
    }
}
