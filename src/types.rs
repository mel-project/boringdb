use crate::DbError;

/// Result type used throughout the codebase.
pub type BoringResult<T> = std::result::Result<T, DbError>;