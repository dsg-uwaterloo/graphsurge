use crate::computations::views::execute::execute;
use crate::computations::views::ExecutionType;
use crate::error::GraphSurgeError;
use crate::global_store::GlobalStore;
use crate::query_handler::create_aggregated_cube::CreateAggregatedCubeAst;
use crate::query_handler::create_view::executor::fmt_graph;
use crate::query_handler::{GraphSurgeQuery, GraphSurgeResult};
use std::fmt::Write;

impl GraphSurgeQuery for CreateAggregatedCubeAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        let (vertices, edges, aggregations) = execute(
            self.ast.clone(),
            global_store,
            ExecutionType::AggregatedCube(self.group_length),
        )?;
        let mut result = String::new();
        result.push_str(&fmt_graph(&vertices, &edges));
        for (level, outputs) in aggregations.iter().enumerate() {
            write!(result, "\nLevel {}", level).expect("String write failed");
            for (order, (vertices, edges)) in outputs.iter().enumerate() {
                write!(result, "\nOrder {}\n", order).expect("String write failed");
                result.push_str(&fmt_graph(&vertices, &edges));
            }
        }
        Ok(GraphSurgeResult::new(result))
    }
}
