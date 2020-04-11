pub mod executor;

#[derive(new)]
pub struct ShowCollectionsAst;

impl std::fmt::Display for ShowCollectionsAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "show cubes")
    }
}
