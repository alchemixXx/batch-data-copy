use crate::custom_error::CustomResult;

pub trait DataInsertQueryGeneratorTrait {
    fn generate(&self) -> CustomResult<String>;
}
