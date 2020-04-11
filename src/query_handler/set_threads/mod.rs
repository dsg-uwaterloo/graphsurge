pub mod executor;

pub struct SetThreads(pub usize, pub usize);

impl std::fmt::Display for SetThreads {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "SET {}", self.0)
    }
}
