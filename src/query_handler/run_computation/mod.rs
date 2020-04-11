use crate::computations::{ComputationProperties, ComputationType, SplitIndices};
use hashbrown::HashMap;

pub mod executor;

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum MaterializeResults {
    None,
    Diff,
    Full,
}

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
