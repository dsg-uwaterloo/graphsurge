pub mod executor;

#[derive(new)]
pub struct WriteCubeAst {
    name: String,
    dir: String,
    threads: usize,
    graphbolt_format: bool,
}

impl std::fmt::Display for WriteCubeAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "write cube to '{}'", self.dir)
    }
}
