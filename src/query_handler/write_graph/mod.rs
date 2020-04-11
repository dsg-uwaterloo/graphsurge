pub mod executor;

#[derive(new)]
pub struct WriteGraphAst {
    dir: String,
}

impl std::fmt::Display for WriteGraphAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "write graph to '{}'", self.dir)
    }
}
