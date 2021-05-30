use crate::filtered_cubes::DimensionLength;
use abomonation_derive::Abomonation;
use differential_dataflow::lattice::Lattice;
use gs_analytics_api::GsTs;
use itertools::Itertools;
use std::fmt::Debug;
use timely::progress::timestamp::Refines;
use timely::progress::PathSummary;

#[derive(
    PartialEq, Eq, PartialOrd, Ord, Default, Clone, Copy, Abomonation, Hash, Serialize, Deserialize,
)]
pub struct GSTimestamp1D {
    id: TimestampId,
}
pub type TimestampId = [DimensionId; MAX_DIMENSIONS];
pub type DimensionId = u16;
const MAX_DIMENSIONS: usize = 1;

impl GsTs for GSTimestamp1D {
    fn next(&self) -> Self {
        let mut next = *self;
        next.set_value_at(0, 1, next.get_value_at(0, 1) + 1);
        next
    }
}

impl GSTimestamp1D {
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

    pub fn all_timestamps(dimension_lengths: &[DimensionLength]) -> Vec<GSTimestamp1D> {
        dimension_lengths
            .iter()
            .map(|&dimension_length| 0..dimension_length)
            .multi_cartesian_product()
            .map(|values| GSTimestamp1D::new(&values))
            .collect_vec()
    }

    pub fn get_str(self, _sep: char) -> String {
        format!("{}", self.id[0])
    }
}

impl std::fmt::Display for GSTimestamp1D {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "[{}]", self.get_str(','))
    }
}

impl Debug for GSTimestamp1D {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "[{}]", self.get_str(','))
    }
}

impl timely::PartialOrder for GSTimestamp1D {
    fn less_equal(&self, other: &Self) -> bool {
        unsafe { self.id.get_unchecked(0) <= other.id.get_unchecked(0) }
    }
}

impl timely::progress::Timestamp for GSTimestamp1D {
    type Summary = ();
}

impl PathSummary<GSTimestamp1D> for () {
    fn results_in(&self, src: &GSTimestamp1D) -> Option<GSTimestamp1D> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl Refines<()> for GSTimestamp1D {
    fn to_inner(_outer: ()) -> Self {
        GSTimestamp1D::default()
    }
    fn to_outer(self) {}
    fn summarize(_summary: <Self>::Summary) {}
}

impl Lattice for GSTimestamp1D {
    fn minimum() -> Self {
        GSTimestamp1D::default()
    }

    fn join(&self, other: &Self) -> Self {
        unsafe {
            Self { id: [std::cmp::max(*self.id.get_unchecked(0), *other.id.get_unchecked(0))] }
        }
    }

    fn meet(&self, other: &Self) -> Self {
        unsafe {
            Self { id: [std::cmp::min(*self.id.get_unchecked(0), *other.id.get_unchecked(0))] }
        }
    }
}

#[cfg(test)]
#[allow(clippy::decimal_literal_representation)]
mod tests {
    use super::GSTimestamp1D;
    use differential_dataflow::lattice::Lattice;
    use hashbrown::HashMap;
    use itertools::Itertools;
    use timely::PartialOrder;

    #[test]
    fn test_basic_timestamp_operations() {
        let timestamp = vec![10];
        let ts = GSTimestamp1D::new(&timestamp);
        let expected_ts_id = [10];
        assert_eq!(ts.id, expected_ts_id);
    }

    #[test]
    fn test_diff_neighborhood() {
        let ts = GSTimestamp1D::new(&[10]);
        let diff_neighborhood = ts.get_diff_neighborhood();
        let expected_positive_neighbors =
            [[9]].iter().map(|values| GSTimestamp1D::new(values)).collect_vec();
        assert_eq!(diff_neighborhood.0, expected_positive_neighbors);
        let expected_negative_neighbors = Vec::new();
        assert_eq!(diff_neighborhood.1, expected_negative_neighbors);
    }

    /// Tests that creating timestamp with illegal dimensions panics.
    #[test]
    #[should_panic(expected = "Total dimensions should be between 1 and 1")]
    fn test_illegal_timestamp() {
        let _ = GSTimestamp1D::new(&[3, 2, 367, 99, 223]);
    }
    #[test]
    #[should_panic(expected = "Total dimensions should be between 1 and 1")]
    fn test_empty_timestamp() {
        let _ = GSTimestamp1D::new(&[]);
    }

    #[test]
    fn test_generating_all_timestamps() {
        let dimension_lengths = [3];
        let all_timestamps = GSTimestamp1D::all_timestamps(&dimension_lengths);
        let expected_timestamps =
            [[0], [1], [2]].iter().map(|values| GSTimestamp1D::new(values)).collect_vec();
        assert_eq!(all_timestamps, expected_timestamps);
    }

    #[test]
    fn test_partial_order() {
        let dimension_lengths = [3];
        let all_timestamps = GSTimestamp1D::all_timestamps(&dimension_lengths);
        let previous_timestamps = all_timestamps
            .iter()
            .map(|timestamp| {
                (
                    timestamp.get_str('_'),
                    GSTimestamp1D::all_timestamps(&[10])
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

        let expected_previous_timestamps = [([0], vec![]), ([1], vec![[0]]), ([2], vec![[0], [1]])]
            .iter()
            .map(|(ts, pts)| {
                (ts.iter().join("_"), pts.iter().map(|ts| ts.iter().join("_")).collect_vec())
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(previous_timestamps, expected_previous_timestamps);
    }

    #[test]
    fn test_lattice() {
        for (index, (ts1, ts2, expected_join, expected_meet)) in
            [(vec![10], vec![2], vec![10], vec![2]), (vec![10], vec![655], vec![655], vec![10])]
                .iter()
                .map(|(t1, t2, t3, t4)| {
                    (
                        GSTimestamp1D::new(t1),
                        GSTimestamp1D::new(t2),
                        GSTimestamp1D::new(t3),
                        GSTimestamp1D::new(t4),
                    )
                })
                .enumerate()
        {
            assert_eq!(ts1.join(&ts2), expected_join, "Join test failed for index {}", index);
            assert_eq!(ts1.meet(&ts2), expected_meet, "Meet test failed for index {}", index);
        }
    }
}
