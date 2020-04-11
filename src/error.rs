#[derive(Debug)]
pub enum ErrorType {
    Error,
    IOError,
    ParsingError,
    LoadGraphError,
    GraphError,
    CreateCubeError,
    CreateViewError,
    KeyDoesNotExistError,
    ComputationError,
    SerdeError,
}

impl std::fmt::Display for ErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, new)]
pub struct GraphSurgeError {
    error_type: ErrorType,
    message: String,
}

pub fn gs_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::Error, message)
}

pub fn load_graph_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::LoadGraphError, message)
}

pub fn graph_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::GraphError, message)
}

pub fn io_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::IOError, message)
}

pub fn parsing_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::ParsingError, message)
}

pub fn create_cube_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::CreateCubeError, message)
}

pub fn create_view_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::CreateViewError, message)
}

pub fn key_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::KeyDoesNotExistError, message)
}

pub fn computation_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::ComputationError, message)
}

pub fn serde_error(message: String) -> GraphSurgeError {
    GraphSurgeError::new(ErrorType::SerdeError, message)
}

impl std::fmt::Display for GraphSurgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}] {}", self.error_type.to_string(), self.message,)
    }
}
