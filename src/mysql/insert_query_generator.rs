use crate::{
    custom_error::CustomResult,
    mysql::double_staged_tables_query_generator::DoubleStagedTablesQueryGenerator,
    traits::DataInsertQueryGeneratorTrait,
};

use super::batch_tables_query_generator::BatchTablesQueryGenerator;

pub struct InsertQueryGenerator<'config> {
    pub config: &'config crate::config::Config,
}

impl<'config> DataInsertQueryGeneratorTrait for InsertQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<String> {
        let mut result = String::new();
        println!("Generating insert statement for mysql");

        // let batch_tables_generator = BatchTablesQueryGenerator { config: &self.config };
        // let batch_tables_sql = batch_tables_generator.generate()?;
        // result.push_str(batch_tables_sql.as_str());

        let double_staged_tables_generator = DoubleStagedTablesQueryGenerator {
            config: &self.config,
        };
        let double_staged_tables_sql = double_staged_tables_generator.generate()?;
        result.push_str(double_staged_tables_sql.as_str());

        println!("Generated insert statement for mysql");
        Ok(result)
    }
}
