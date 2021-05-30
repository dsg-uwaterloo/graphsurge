use crate::graph::key_store::KeyId;
use crate::graph::properties::operations::Operator;
use crate::graph::properties::property_value_type::PropertyValueType;
use crate::graph::VertexOrEdgeId;
use abomonation_derive::Abomonation;
use std::convert::TryFrom;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Abomonation, Serialize, Deserialize)]
pub enum PropertyValue {
    KeyId(KeyId),
    Isize(isize),
    String(String),
    Strings(Vec<String>),
    Bool(bool),
    Pair(isize, isize),
}

impl PropertyValue {
    pub fn get_id(id: VertexOrEdgeId) -> Self {
        PropertyValue::Isize(isize::try_from(id).expect("Overflow"))
    }

    pub fn compare(&self, other: &Self, operator: Operator) -> bool {
        match operator {
            Operator::Less => self < other,
            Operator::LessEqual => self <= other,
            Operator::Greater => self > other,
            Operator::GreaterEqual => self >= other,
            Operator::Equal => self == other,
            Operator::NotEqual => self != other,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            PropertyValue::KeyId(key_id) => {
                bincode::serialize(key_id).expect("Serialization error")
            }
            PropertyValue::Isize(value) => bincode::serialize(value).expect("Serialization error"),
            PropertyValue::String(value) => bincode::serialize(value).expect("Serialization error"),
            PropertyValue::Strings(value) => {
                bincode::serialize(value).expect("Serialization error")
            }
            PropertyValue::Bool(value) => bincode::serialize(value).expect("Serialization error"),
            PropertyValue::Pair(value1, value2) => {
                bincode::serialize(&(value1, value2)).expect("Serialization error")
            }
        }
    }

    pub fn value_type(&self) -> PropertyValueType {
        match self {
            PropertyValue::KeyId(_) => PropertyValueType::KeyId,
            PropertyValue::Isize(_) => PropertyValueType::Isize,
            PropertyValue::String(_) => PropertyValueType::String,
            PropertyValue::Strings(_) => PropertyValueType::Strings,
            PropertyValue::Bool(_) => PropertyValueType::Bool,
            PropertyValue::Pair(_, _) => PropertyValueType::Pair,
        }
    }
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                PropertyValue::KeyId(key_id) => format!("{}", key_id),
                PropertyValue::Isize(value) => format!("{}", value),
                PropertyValue::String(value) => format!("'{}'", value),
                PropertyValue::Strings(value) => format!("{:?}", value),
                PropertyValue::Bool(value) => format!("{:?}", value),
                PropertyValue::Pair(value1, value2) => format!("({},{})", value1, value2),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::properties::operations::Operator;
    use crate::graph::properties::property_value::PropertyValue;

    #[test]
    fn equal_isize_isize_comparisons() {
        let left = PropertyValue::Isize(123_isize);
        let right = PropertyValue::Isize(123_isize);

        assert_equal(&left, &right);
    }

    #[test]
    fn equal_negative_isize_isize_comparisons() {
        let left = PropertyValue::Isize(-199_999);
        let right = PropertyValue::Isize(-199_999);

        assert_equal(&left, &right);
    }

    #[test]
    fn less_than_isize_isize_comparisons() {
        let left = PropertyValue::Isize(123);
        let right = PropertyValue::Isize(999);

        assert_less_than(&left, &right);
    }

    #[test]
    fn greater_than_usize_isize_comparisons() {
        let left = PropertyValue::Isize(186_744_073_709_551_600);
        let right = PropertyValue::Isize(78);

        assert_greater_than(&left, &right);
    }

    #[test]
    fn negative_isize_isize_comparisons() {
        let left = PropertyValue::Isize(0x7FFF_FFFF_FFFF_FFFF);
        let right = PropertyValue::Isize(-1999);

        assert_greater_than(&left, &right);
    }

    #[test]
    fn equal_string_comparisons() {
        let left = PropertyValue::String("testing".to_owned());
        let right = PropertyValue::String("testing".to_owned());

        assert_equal(&left, &right);
    }

    #[test]
    fn less_string_comparisons() {
        let left = PropertyValue::String("abc testing".to_owned());
        let right = PropertyValue::String("xyz testing".to_owned());

        assert_less_than(&left, &right);
    }

    #[test]
    fn greater_string_comparisons() {
        let left = PropertyValue::String("986".to_owned());
        let right = PropertyValue::String("11465753".to_owned());

        assert_greater_than(&left, &right);
    }

    #[test]
    fn equal_bool_comparisons() {
        let left = PropertyValue::Bool(true);
        let right = PropertyValue::Bool(true);

        assert_equal(&left, &right);
    }

    #[test]
    fn less_bool_comparisons() {
        let left = PropertyValue::Bool(false);
        let right = PropertyValue::Bool(true);

        assert_less_than(&left, &right);
    }

    #[test]
    fn equal_pair_comparisons() {
        let left = PropertyValue::Pair(146_837_348_556_565, 1);
        let right = PropertyValue::Pair(146_837_348_556_565, 1);

        assert_equal(&left, &right);
    }

    #[test]
    fn less_pair_comparisons() {
        let left = PropertyValue::Pair(146_837_348_556_564, 123);
        let right = PropertyValue::Pair(146_837_348_556_565, 1);

        assert_less_than(&left, &right);
    }

    #[test]
    fn greater_pair_comparisons() {
        let left = PropertyValue::Pair(146_837_348_556_564, 123);
        let right = PropertyValue::Pair(199, 146_837_348_556_565);

        assert_greater_than(&left, &right);
    }

    fn assert_equal(left: &PropertyValue, right: &PropertyValue) {
        assert_eq!(left.compare(&right, Operator::Equal), true);
        assert_eq!(left.compare(&right, Operator::NotEqual), false);
        assert_eq!(left.compare(&right, Operator::Greater), false);
        assert_eq!(left.compare(&right, Operator::GreaterEqual), true);
        assert_eq!(left.compare(&right, Operator::Less), false);
        assert_eq!(left.compare(&right, Operator::LessEqual), true);

        assert_eq!(right.compare(&left, Operator::Equal), true);
        assert_eq!(right.compare(&left, Operator::NotEqual), false);
        assert_eq!(right.compare(&left, Operator::Greater), false);
        assert_eq!(right.compare(&left, Operator::GreaterEqual), true);
        assert_eq!(right.compare(&left, Operator::Less), false);
        assert_eq!(right.compare(&left, Operator::LessEqual), true);
    }

    fn assert_greater_than(left: &PropertyValue, right: &PropertyValue) {
        assert_eq!(left.compare(&right, Operator::Equal), false);
        assert_eq!(left.compare(&right, Operator::NotEqual), true);
        assert_eq!(left.compare(&right, Operator::Greater), true);
        assert_eq!(left.compare(&right, Operator::GreaterEqual), true);
        assert_eq!(left.compare(&right, Operator::Less), false);
        assert_eq!(left.compare(&right, Operator::LessEqual), false);

        assert_eq!(right.compare(&left, Operator::Equal), false);
        assert_eq!(right.compare(&left, Operator::NotEqual), true);
        assert_eq!(right.compare(&left, Operator::Greater), false);
        assert_eq!(right.compare(&left, Operator::GreaterEqual), false);
        assert_eq!(right.compare(&left, Operator::Less), true);
        assert_eq!(right.compare(&left, Operator::LessEqual), true);
    }

    fn assert_less_than(left: &PropertyValue, right: &PropertyValue) {
        assert_eq!(left.compare(&right, Operator::Equal), false);
        assert_eq!(left.compare(&right, Operator::NotEqual), true);
        assert_eq!(left.compare(&right, Operator::Greater), false);
        assert_eq!(left.compare(&right, Operator::GreaterEqual), false);
        assert_eq!(left.compare(&right, Operator::Less), true);
        assert_eq!(left.compare(&right, Operator::LessEqual), true);

        assert_eq!(right.compare(&left, Operator::Equal), false);
        assert_eq!(right.compare(&left, Operator::NotEqual), true);
        assert_eq!(right.compare(&left, Operator::Greater), true);
        assert_eq!(right.compare(&left, Operator::GreaterEqual), true);
        assert_eq!(right.compare(&left, Operator::Less), false);
        assert_eq!(right.compare(&left, Operator::LessEqual), false);
    }
}
