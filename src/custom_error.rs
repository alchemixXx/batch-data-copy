pub type CustomResult<T> = core::result::Result<T, CustomError>;

#[derive(Debug)]
pub enum CustomError {
    DbQueryExecution(String),
    DbTableStructure,
    DbConnection,
    DbTechnology,
    FileCreationError,
    FileDataInsertionError,
    FolderCreationError,
    NotImplemented,
}

impl From<sqlx::error::Error> for CustomError {
    fn from(e: sqlx::error::Error) -> Self {
        Self::DbQueryExecution(e.to_string())
    }
}

impl std::error::Error for CustomError {}
impl core::fmt::Display for CustomError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(f, "{self:?}")
    }
}
