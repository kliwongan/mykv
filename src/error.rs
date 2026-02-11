use core::error::Error;
use thiserror::Error;

// Temporary simplified error type
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub type RaftResult<T> = std::result::Result<T, RaftError>;

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
}
