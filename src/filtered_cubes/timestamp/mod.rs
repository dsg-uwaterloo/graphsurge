use crate::filtered_cubes::DimensionLength;
use abomonation_derive::Abomonation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use std::fmt::Debug;
use timely::progress::timestamp::Refines;
use timely::progress::PathSummary;

pub mod timestamp_mappings;

#[derive(
    PartialEq, Eq, PartialOrd, Ord, Default, Clone, Copy, Abomonation, Hash, Serialize, Deserialize,
)]
pub struct GSTimestamp {
    id: TimestampId,
}
pub type TimestampId = [DimensionId; MAX_DIMENSIONS];
pub type DimensionId = u16;
const MAX_DIMENSIONS: usize = 3;

pub trait NextTS {
    fn next(&self) -> Self;
    fn next_with(&self, count: DimensionId) -> Self;
}

impl NextTS for GSTimestamp {
    fn next(&self) -> Self {
        let mut next = *self;
        next.set_value_at(0, 1, next.get_value_at(0, 1) + 1);
        next
    }
    fn next_with(&self, count: DimensionId) -> Self {
        let mut next = *self;
        next.set_value_at(0, 1, next.get_value_at(0, 1) + count);
        next
    }
}

macro_rules! implement_nextts {
    ($($index_type:ty,)*) => (
        $(
            impl NextTS for $index_type {
                fn next(&self) -> Self {
                    self.checked_add(1).expect("Error: TS.next() overflow")
                }
                fn next_with(&self, count: DimensionId) -> Self {
                    self.checked_add(count as Self).expect("Error: TS.next() overflow")
                }
            }
        )*
    )
}
implement_nextts!(usize, u128, u64, u32, u16, u8,);

impl GSTimestamp {
    pub fn new(dimension_ids: &[DimensionId]) -> Self {
        let len = dimension_ids.len();
        assert!(
            len > 0 && len <= MAX_DIMENSIONS,
            "Total dimensions should be between 1 and {}",
            MAX_DIMENSIONS
        );
        let mut ts = Self { id: TimestampId::default() };
        for (index, dimension_id) in dimension_ids.iter().rev().enumerate() {
            ts.id[index] = *dimension_id;
        }
        ts
    }

    pub fn get_zeroth_timestamp() -> Self {
        Self { id: TimestampId::default() }
    }

    pub fn get_value_at(self, index: usize, len: usize) -> DimensionId {
        assert!(
            len > 0 && len <= MAX_DIMENSIONS,
            "Total dimensions should be between 1 and {}",
            MAX_DIMENSIONS
        );
        assert!(index < len, "Dimension id ({}) should be less than length ({})", index, len);
        self.id[len - 1 - index]
    }

    pub fn set_value_at(&mut self, index: usize, len: usize, timestamp_id: DimensionId) {
        assert!(
            len > 0 && len <= MAX_DIMENSIONS,
            "Total dimensions should be between 1 and {}",
            MAX_DIMENSIONS
        );
        assert!(index < len, "Dimension id ({}) should be less than length ({})", index, len);
        self.id[len - 1 - index] = timestamp_id;
    }

    /// Get the neighborhood timestamps based on the inclusion-exclusion principle.
    pub fn get_diff_neighborhood(self) -> (Vec<Self>, Vec<Self>) {
        let mut diff_neighborhood = (Vec::new(), Vec::new());
        for level in 1..=MAX_DIMENSIONS {
            // Get indices that need to be decremented `level` at-a-time.
            'outer: for indices_to_decrement in (0..MAX_DIMENSIONS).combinations(level) {
                let mut previous_timestamp = self;
                for index_to_decrement in indices_to_decrement {
                    let timestamp_value = previous_timestamp.id[index_to_decrement];
                    if timestamp_value == 0 {
                        // Previous timestamp does not exist.
                        continue 'outer;
                    }
                    previous_timestamp.id[index_to_decrement] = timestamp_value - 1;
                }
                if level % 2 == 1 {
                    // Add the previous sum.
                    diff_neighborhood.0.push(previous_timestamp);
                } else {
                    // Subtract the previous sum.
                    diff_neighborhood.1.push(previous_timestamp);
                }
            }
        }
        diff_neighborhood
    }

    pub fn all_timestamps(dimension_lengths: &[DimensionLength]) -> Vec<GSTimestamp> {
        dimension_lengths
            .iter()
            .map(|&dimension_length| 0..dimension_length)
            .multi_cartesian_product()
            .map(|values| GSTimestamp::new(&values))
            .collect_vec()
    }

    pub fn get_str(self, sep: char) -> String {
        format!("{}{sep}{}{sep}{}", self.id[2], self.id[1], self.id[0], sep = sep)
    }
}

impl std::fmt::Display for GSTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "[{}]", self.get_str(','))
    }
}

impl Debug for GSTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "[{}]", self.get_str(','))
    }
}

impl timely::PartialOrder for GSTimestamp {
    fn less_equal(&self, other: &Self) -> bool {
        unsafe {
            self.id.get_unchecked(0) <= other.id.get_unchecked(0)
                && self.id.get_unchecked(1) <= other.id.get_unchecked(1)
                && self.id.get_unchecked(2) <= other.id.get_unchecked(2)
        }
    }
}

impl timely::progress::Timestamp for GSTimestamp {
    type Summary = ();
}

impl PathSummary<GSTimestamp> for () {
    fn results_in(&self, src: &GSTimestamp) -> Option<GSTimestamp> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl Refines<()> for GSTimestamp {
    fn to_inner(_outer: ()) -> Self {
        GSTimestamp::default()
    }
    fn to_outer(self) {}
    fn summarize(_summary: <Self>::Summary) {}
}

impl Lattice for GSTimestamp {
    fn minimum() -> Self {
        GSTimestamp::default()
    }

    fn join(&self, other: &Self) -> Self {
        unsafe {
            Self {
                id: [
                    std::cmp::max(*self.id.get_unchecked(0), *other.id.get_unchecked(0)),
                    std::cmp::max(*self.id.get_unchecked(1), *other.id.get_unchecked(1)),
                    std::cmp::max(*self.id.get_unchecked(2), *other.id.get_unchecked(2)),
                ],
            }
        }
    }

    fn meet(&self, other: &Self) -> Self {
        unsafe {
            Self {
                id: [
                    std::cmp::min(*self.id.get_unchecked(0), *other.id.get_unchecked(0)),
                    std::cmp::min(*self.id.get_unchecked(1), *other.id.get_unchecked(1)),
                    std::cmp::min(*self.id.get_unchecked(2), *other.id.get_unchecked(2)),
                ],
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::decimal_literal_representation)]
mod tests {
    use crate::filtered_cubes::timestamp::GSTimestamp;
    use differential_dataflow::lattice::Lattice;
    use hashbrown::HashMap;
    use itertools::Itertools;
    use timely::PartialOrder;

    #[test]
    fn test_basic_timestamp_operations() {
        let timestamp = vec![10, 2000, 65535];
        let ts = GSTimestamp::new(&timestamp);
        let expected_ts_id = [65535, 2000, 10];
        assert_eq!(ts.id, expected_ts_id);
    }

    #[test]
    fn test_diff_neighborhood() {
        let ts = GSTimestamp::new(&[10, 2000, 65535]);
        let diff_neighborhood = ts.get_diff_neighborhood();
        let expected_positive_neighbors =
            [[10, 2000, 65534], [10, 1999, 65535], [9, 2000, 65535], [9, 1999, 65534]]
                .iter()
                .map(|values| GSTimestamp::new(values))
                .collect_vec();
        assert_eq!(diff_neighborhood.0, expected_positive_neighbors);
        let expected_negative_neighbors = [[10, 1999, 65534], [9, 2000, 65534], [9, 1999, 65535]]
            .iter()
            .map(|values| GSTimestamp::new(values))
            .collect_vec();
        assert_eq!(diff_neighborhood.1, expected_negative_neighbors);
    }

    /// Tests that creating timestamp with illegal dimensions panics.
    #[test]
    fn test_illegal_timestamps() {
        let result = std::panic::catch_unwind(|| GSTimestamp::new(&[3, 2, 367, 99, 223]));
        assert!(result.is_err(), "Dimensions larger than `MAX_DIMENSIONS` should panic.");

        let result = std::panic::catch_unwind(|| GSTimestamp::new(&[]));
        assert!(result.is_err(), "Empty dimensions should panic.");
    }

    #[test]
    fn test_generating_all_timestamps() {
        let dimension_lengths = [3, 2, 4];
        let all_timestamps = GSTimestamp::all_timestamps(&dimension_lengths);
        let expected_timestamps = [
            [0, 0, 0],
            [0, 0, 1],
            [0, 0, 2],
            [0, 0, 3],
            [0, 1, 0],
            [0, 1, 1],
            [0, 1, 2],
            [0, 1, 3],
            [1, 0, 0],
            [1, 0, 1],
            [1, 0, 2],
            [1, 0, 3],
            [1, 1, 0],
            [1, 1, 1],
            [1, 1, 2],
            [1, 1, 3],
            [2, 0, 0],
            [2, 0, 1],
            [2, 0, 2],
            [2, 0, 3],
            [2, 1, 0],
            [2, 1, 1],
            [2, 1, 2],
            [2, 1, 3],
        ]
        .iter()
        .map(|values| GSTimestamp::new(values))
        .collect_vec();
        assert_eq!(all_timestamps, expected_timestamps)
    }

    #[test]
    fn test_partial_order() {
        let dimension_lengths = [3, 2, 4];
        let all_timestamps = GSTimestamp::all_timestamps(&dimension_lengths);
        let previous_timestamps = all_timestamps
            .iter()
            .map(|timestamp| {
                (
                    timestamp.get_str('_'),
                    GSTimestamp::all_timestamps(&[10, 10, 10])
                        .iter()
                        .copied()
                        .filter_map(|previous_timestamp| {
                            if previous_timestamp.less_than(timestamp) {
                                Some(previous_timestamp.get_str('_'))
                            } else {
                                None
                            }
                        })
                        .collect_vec(),
                )
            })
            .collect::<HashMap<_, _>>();

        let expected_previous_timestamps = [
            ([0, 0, 0], vec![]),
            ([0, 0, 1], vec![[0, 0, 0]]),
            ([0, 0, 2], vec![[0, 0, 0], [0, 0, 1]]),
            ([0, 0, 3], vec![[0, 0, 0], [0, 0, 1], [0, 0, 2]]),
            ([0, 1, 0], vec![[0, 0, 0]]),
            ([0, 1, 1], vec![[0, 0, 0], [0, 0, 1], [0, 1, 0]]),
            ([0, 1, 2], vec![[0, 0, 0], [0, 0, 1], [0, 0, 2], [0, 1, 0], [0, 1, 1]]),
            (
                [0, 1, 3],
                vec![[0, 0, 0], [0, 0, 1], [0, 0, 2], [0, 0, 3], [0, 1, 0], [0, 1, 1], [0, 1, 2]],
            ),
            ([1, 0, 0], vec![[0, 0, 0]]),
            ([1, 0, 1], vec![[0, 0, 0], [0, 0, 1], [1, 0, 0]]),
            ([1, 0, 2], vec![[0, 0, 0], [0, 0, 1], [0, 0, 2], [1, 0, 0], [1, 0, 1]]),
            (
                [1, 0, 3],
                vec![[0, 0, 0], [0, 0, 1], [0, 0, 2], [0, 0, 3], [1, 0, 0], [1, 0, 1], [1, 0, 2]],
            ),
            ([1, 1, 0], vec![[0, 0, 0], [0, 1, 0], [1, 0, 0]]),
            (
                [1, 1, 1],
                vec![[0, 0, 0], [0, 0, 1], [0, 1, 0], [0, 1, 1], [1, 0, 0], [1, 0, 1], [1, 1, 0]],
            ),
            (
                [1, 1, 2],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 0, 2],
                    [0, 1, 0],
                    [0, 1, 1],
                    [0, 1, 2],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 0, 2],
                    [1, 1, 0],
                    [1, 1, 1],
                ],
            ),
            (
                [1, 1, 3],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 0, 2],
                    [0, 0, 3],
                    [0, 1, 0],
                    [0, 1, 1],
                    [0, 1, 2],
                    [0, 1, 3],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 0, 2],
                    [1, 0, 3],
                    [1, 1, 0],
                    [1, 1, 1],
                    [1, 1, 2],
                ],
            ),
            ([2, 0, 0], vec![[0, 0, 0], [1, 0, 0]]),
            ([2, 0, 1], vec![[0, 0, 0], [0, 0, 1], [1, 0, 0], [1, 0, 1], [2, 0, 0]]),
            (
                [2, 0, 2],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 0, 2],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 0, 2],
                    [2, 0, 0],
                    [2, 0, 1],
                ],
            ),
            (
                [2, 0, 3],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 0, 2],
                    [0, 0, 3],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 0, 2],
                    [1, 0, 3],
                    [2, 0, 0],
                    [2, 0, 1],
                    [2, 0, 2],
                ],
            ),
            ([2, 1, 0], vec![[0, 0, 0], [0, 1, 0], [1, 0, 0], [1, 1, 0], [2, 0, 0]]),
            (
                [2, 1, 1],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 1, 0],
                    [0, 1, 1],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 1, 0],
                    [1, 1, 1],
                    [2, 0, 0],
                    [2, 0, 1],
                    [2, 1, 0],
                ],
            ),
            (
                [2, 1, 2],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 0, 2],
                    [0, 1, 0],
                    [0, 1, 1],
                    [0, 1, 2],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 0, 2],
                    [1, 1, 0],
                    [1, 1, 1],
                    [1, 1, 2],
                    [2, 0, 0],
                    [2, 0, 1],
                    [2, 0, 2],
                    [2, 1, 0],
                    [2, 1, 1],
                ],
            ),
            (
                [2, 1, 3],
                vec![
                    [0, 0, 0],
                    [0, 0, 1],
                    [0, 0, 2],
                    [0, 0, 3],
                    [0, 1, 0],
                    [0, 1, 1],
                    [0, 1, 2],
                    [0, 1, 3],
                    [1, 0, 0],
                    [1, 0, 1],
                    [1, 0, 2],
                    [1, 0, 3],
                    [1, 1, 0],
                    [1, 1, 1],
                    [1, 1, 2],
                    [1, 1, 3],
                    [2, 0, 0],
                    [2, 0, 1],
                    [2, 0, 2],
                    [2, 0, 3],
                    [2, 1, 0],
                    [2, 1, 1],
                    [2, 1, 2],
                ],
            ),
        ]
        .iter()
        .map(|(ts, pts)| {
            (ts.iter().join("_"), pts.iter().map(|ts| ts.iter().join("_")).collect_vec())
        })
        .collect::<HashMap<_, _>>();
        assert_eq!(previous_timestamps, expected_previous_timestamps);
    }

    #[test]
    fn test_lattice() {
        for (index, (ts1, ts2, expected_join, expected_meet)) in [
            (vec![10, 2000, 65535], vec![2, 2, 2], vec![10, 2000, 65535], vec![2, 2, 2]),
            (
                vec![10, 2000, 65534],
                vec![655, 9999, 65535],
                vec![655, 9999, 65535],
                vec![10, 2000, 65534],
            ),
            (
                vec![10, 9999, 65534],
                vec![655, 2000, 65535],
                vec![655, 9999, 65535],
                vec![10, 2000, 65534],
            ),
            (vec![10, 9999], vec![655, 2000, 65535], vec![655, 2000, 65535], vec![10, 9999]),
            (vec![10, 9999, 65534], vec![655, 2000], vec![10, 9999, 65534], vec![655, 2000]),
        ]
        .iter()
        .map(|(t1, t2, t3, t4)| {
            (GSTimestamp::new(t1), GSTimestamp::new(t2), GSTimestamp::new(t3), GSTimestamp::new(t4))
        })
        .enumerate()
        {
            assert_eq!(ts1.join(&ts2), expected_join, "Join test failed for index {}", index);
            assert_eq!(ts1.meet(&ts2), expected_meet, "Meet test failed for index {}", index);
        }
    }
}
