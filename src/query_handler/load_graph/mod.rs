pub mod executor;

#[derive(new)]
pub struct LoadGraphAst {
    only_edge_files: bool,
    vertex_file: Option<String>,
    edge_file: String,
    separator: Option<u8>,
    comment_char: Option<u8>,
    has_headers: bool,
    save_mappings_dir: Option<String>,
    randomize: bool,
}

impl std::fmt::Display for LoadGraphAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "load graph with {}edges from '{}'",
            if let Some(vf) = &self.vertex_file {
                format!("vertices from '{}' and ", vf)
            } else {
                "".to_string()
            },
            self.edge_file,
        )
    }
}
