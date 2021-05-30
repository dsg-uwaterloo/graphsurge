use crate::graph::properties::property_value::PropertyValue;
use crate::graph::properties::PropertyKeyId;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Operator {
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Equal,
    NotEqual,
}

impl Operator {
    pub fn as_string(self) -> String {
        match self {
            Operator::Less => "<",
            Operator::LessEqual => "<=",
            Operator::Greater => ">",
            Operator::GreaterEqual => ">=",
            Operator::Equal => "=",
            Operator::NotEqual => "<>",
        }
        .to_owned()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Operand {
    Property(PropertyKeyId),
    Edge,
    SourceVertex(PropertyKeyId),
    DestinationVertex(PropertyKeyId),
}
pub type LeftOperand = Operand;

impl std::fmt::Display for Operand {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                Operand::Edge => "e".to_owned(),
                Operand::Property(k) => k.to_string(),
                Operand::SourceVertex(k) => format!("u.{}", k),
                Operand::DestinationVertex(k) => format!("v.{}", k),
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RightOperand {
    Value(PropertyValue),
    Variable(Operand),
}

impl std::fmt::Display for RightOperand {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                RightOperand::Value(v) => v.to_string(),
                RightOperand::Variable(v) => v.to_string(),
            }
        )
    }
}
