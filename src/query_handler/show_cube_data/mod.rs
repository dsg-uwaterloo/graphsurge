pub mod executor;

#[derive(new)]
pub struct ShowCollectionDataAst {
    name: String,
}

impl std::fmt::Display for ShowCollectionDataAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "show data for cube '{}'", self.name)
    }
}
