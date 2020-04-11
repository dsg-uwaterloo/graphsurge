use crate::query_handler::create_view::WhereConditions;

pub mod executor;

#[derive(Default, Debug, new)]
pub struct CreateViewCollectionAst {
    name: String,
    dimensions: Vec<Dimension>,
    manual_order: bool,
    materialized: bool,
    store_total_data: bool,
    hosts: Vec<String>,
}
pub type Dimension = Vec<WhereConditions>;

impl std::fmt::Display for CreateViewCollectionAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        // TODO
        write!(f, "create view collection {}", self.name,)
    }
}
