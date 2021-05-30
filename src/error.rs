#[derive(Debug)]
pub enum GSError {
    Generic(String),
    ReadFile(String, String),
    CreateFile(String, String),
    WriteFile(String, String),
    Parsing(String),
    LoadGraph(String),
    GraphParse(String, &'static str, String, String),
    CollectionAlreadyExists(String),
    CollectionMissing(String),
    Collection(String),
    Computation(String),
    PropertyCount(&'static str, usize, Vec<&'static str>, usize),
    Property(&'static str, &'static str, Vec<String>),
    PropertyType(&'static str, &'static str, &'static str, String),
    Serialize(String, String),
    Deserialize(String, String),
    NotDirectory(String),
    Timely(String),
    TimelyResults(String),
    ResultsMismatch,
    TypeMismatch(String, String),
    UnknownComputation(String),
}

impl std::fmt::Display for GSError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            GSError::Generic(msg) => write!(f, "[GSError] {}", msg)?,
            GSError::ReadFile(file_path, e) => {
                write!(f, "[IOError] Could not open file '{}' for reading: {}", file_path, e)?;
            }
            GSError::CreateFile(file_path, e) => {
                write!(f, "[IOError] Could not create file '{}' for writing: {}", file_path, e)?;
            }
            GSError::WriteFile(file_path, e) => {
                write!(f, "[IOError] Could not write to '{}': {}", file_path, e)?;
            }
            GSError::Parsing(message) => write!(f, "[ParsingError] {}", message)?,
            GSError::LoadGraph(message) => write!(f, "[LoadGraphError] {}", message,)?,
            GSError::GraphParse(property, ptype, vertex_id, file) => write!(
                f,
                "[LoadGraphError] Could not parse property value '{}' as {} \
                            for vertex id '{}' in file '{}'",
                property, ptype, vertex_id, file
            )?,
            GSError::Collection(message) => write!(f, "[CollectionError] {}", message,)?,
            GSError::CollectionAlreadyExists(name) => {
                write!(f, "[CollectionError] Collection '{}' already exists in store", name)?;
            }
            GSError::CollectionMissing(name) => {
                write!(f, "[CollectionError] Collection '{}' has not been created yet", name)?;
            }
            GSError::Timely(message) => write!(f, "[TimelyError] {}", message,)?,
            GSError::TimelyResults(message) => {
                write!(f, "[TimelyError] Results from Timely has errors: {}", message,)?;
            }
            GSError::Computation(message) => write!(f, "[ComputationError] {}", message,)?,
            GSError::ResultsMismatch => write!(f, "[ComputationError] Results do not match")?,
            GSError::TypeMismatch(expected, found) => {
                write!(f, "[ComputationError] Expected type '{}', got '{}'", expected, found)?;
            }
            GSError::UnknownComputation(comp) => {
                write!(f, "[ComputationError] Unknown computation type '{}'", comp)?;
            }
            GSError::NotDirectory(path) => {
                write!(f, "[SerdeError] Invalid binary directory: '{}'", path,)?;
            }
            GSError::Serialize(name, e) => {
                write!(f, "[SerdeError] Could not serialize '{}': {}", name, e)?;
            }
            GSError::Deserialize(name, e) => {
                write!(f, "[SerdeError] Could not deserialize '{}': {}", name, e)?;
            }
            GSError::PropertyCount(computation, required, properties, found) => write!(
                f,
                "[InitError] {} needs {} {} {:?}, but found {} properties",
                computation,
                required,
                if *required == 1 { "property" } else { "properties" },
                properties,
                found
            )?,
            GSError::Property(computation, property, properties) => write!(
                f,
                "[InitError] {} needs property '{}' but found '{:?}'",
                computation, property, properties
            )?,
            GSError::PropertyType(computation, property, expected, found) => write!(
                f,
                "[InitError] {} property '{}; should be a '{}' but found '{}'",
                computation, property, expected, found
            )?,
        }
        Ok(())
    }
}
