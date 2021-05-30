use crate::error::GSError;
use crate::global_store::GlobalStore;
use crate::query_handler::show_computations::ShowComputationsAst;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;

impl GraphSurgeQuery for ShowComputationsAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        let result = global_store
            .computations
            .iter()
            .enumerate()
            .map(|(index, (computation, _))| format!("({}) {}", (index + 1), computation))
            .collect::<Vec<String>>()
            .join("\n");
        Ok(GraphSurgeResult::new(format!("Computations:\n{}", result)))
    }
}
