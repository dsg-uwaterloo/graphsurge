pub mod executor;

pub struct DeleteCollectionsAst;

impl std::fmt::Display for DeleteCollectionsAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "delete cubes")
    }
}
