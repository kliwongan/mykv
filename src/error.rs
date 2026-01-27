use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;
