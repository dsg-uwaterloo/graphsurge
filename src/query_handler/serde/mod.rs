pub mod executor;

#[derive(new)]
pub struct Serde {
    operation: Operation,
    bin_dir: String,
    threads: usize,
    block_size: Option<usize>,
}

#[derive(Eq, PartialEq)]
pub enum Operation {
    SerializeGraph,
    DeserializeGraph,
    SerializeCollection(String),
    DeserializeCollection,
}

impl std::fmt::Display for Serde {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{} '{}'",
            if self.operation == Operation::SerializeGraph {
                "serialize to"
            } else {
                "deserialize from"
            },
            self.bin_dir
        )
    }
}
