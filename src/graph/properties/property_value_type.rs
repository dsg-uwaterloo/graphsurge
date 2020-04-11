#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PropertyValueType {
    KeyId,
    Isize,
    String,
    Strings,
    Bool,
    Pair,
}

impl std::fmt::Display for PropertyValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                PropertyValueType::KeyId => "KeyId",
                PropertyValueType::Isize => "ISize",
                PropertyValueType::String => "String",
                PropertyValueType::Strings => "Strings",
                PropertyValueType::Bool => "Bool",
                PropertyValueType::Pair => "Pair",
            }
        )
    }
}
