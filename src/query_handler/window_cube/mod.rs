pub mod executor;

#[derive(new)]
pub struct WindowCubeAst {
    name: String,
    first_view: usize,
    small_diffs: usize,
    large_diff: usize,
    diff_batch_count: usize,
    total_batch_count: usize,
    graph_filename: String,
}

impl std::fmt::Display for WindowCubeAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "window cube '{}'", self.name)
    }
}
