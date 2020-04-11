#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct IdGenerator {
    next: usize,
    increment: usize,
}

impl IdGenerator {
    pub fn new_from(start: usize, increment: usize) -> Self {
        IdGenerator { next: start, increment }
    }

    pub fn next_id(&mut self) -> usize {
        let next = self.next;
        self.next = self.next.checked_add(self.increment).expect("Ran out of ids");
        next
    }
}
