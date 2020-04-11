pub mod executor;

#[derive(new)]
pub struct GenerateCubeAst {
    name: String,
    first_view: usize,
    adds: usize,
    dels: usize,
    batch_count: usize,
    graph_filename: String,
}

impl std::fmt::Display for GenerateCubeAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "generate cube '{}'", self.name)
    }
}
