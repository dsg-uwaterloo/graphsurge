pub mod executor;

pub struct ShowComputationsAst;

impl std::fmt::Display for ShowComputationsAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "show computations")
    }
}
