use crate::DiffCount;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::operators::unordered_input::{ActivateCapability, UnorderedHandle};
use timely::dataflow::operators::UnorderedInput;
use timely::dataflow::Scope;

/// Used to define and send inputs to computations that require additional properties (such as
/// starting `root`s for BFS).
pub struct PropertyInput<G: Scope, D: Clone + 'static>
where
    G::Timestamp: Lattice + Copy,
{
    handle: UnorderedHandle<G::Timestamp, (D, G::Timestamp, DiffCount)>,
    capability: ActivateCapability<G::Timestamp>,
}

impl<G: Scope, D: Clone + 'static> PropertyInput<G, D>
where
    G::Timestamp: Lattice + Copy,
{
    /// Creates a new input collection.
    pub fn new(mut scope: G) -> (Self, Collection<G, D, DiffCount>) {
        let ((handle, capability), stream) =
            scope.new_unordered_input::<(D, G::Timestamp, DiffCount)>();
        (PropertyInput { handle, capability }, stream.as_collection())
    }

    /// Sends a single input to the collection.
    pub fn send(mut self, input: D) {
        self.handle.session(self.capability).give((input, G::Timestamp::default(), 1));
    }

    /// Sends a set of inputs specified by the `iter` iterator.
    pub fn send_iter<I: Iterator<Item = D>>(mut self, iter: I) {
        self.handle
            .session(self.capability)
            .give_iterator(iter.map(move |v| (v, G::Timestamp::default(), 1)));
    }
}
