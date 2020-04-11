use crate::query_handler::create_view::{fmt_edge_sections, fmt_vertex_sections, CreateViewAst};
use std::ops::Deref;

pub mod executor;

#[derive(Default, Debug, Clone, new)]
pub struct CreateAggregatedCubeAst {
    ast: CreateViewAst,
    group_length: u8,
}

impl Deref for CreateAggregatedCubeAst {
    type Target = CreateViewAst;

    fn deref(&self) -> &Self::Target {
        &self.ast
    }
}

impl std::fmt::Display for CreateAggregatedCubeAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "create aggregated cube {} with{}{}",
            self.name,
            fmt_vertex_sections(&self.vertex_sections, &self.label_map),
            fmt_edge_sections(&self.edge_sections, &self.label_map),
        )
    }
}
