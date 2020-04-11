pub mod executor;

#[derive(new)]
pub struct LoadCubeAst {
    name: String,
    m: usize,
    n: usize,
    dir: String,
    prefix: String,
    separator: Option<char>,
    comment_char: Option<char>,
    threads: Option<usize>,
    with_full: bool,
}

impl std::fmt::Display for LoadCubeAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "load cube to '{}'", self.dir)
    }
}
