use crate::{
    custom_error::CustomResult,
    logger::LoggerTrait,
    mysql::double_staged_tables_query_generator::DoubleStagedTablesQueryGenerator,
    traits::{ InsertQueries, TablesInsertQueryGeneratorTrait, TechnologyInsertGeneratorTrait },
};

use super::batch_tables_query_generator::BatchTablesQueryGenerator;

pub struct InsertQueryGenerator<'config> {
    pub config: &'config crate::config::Config,
}

impl<'config> LoggerTrait for InsertQueryGenerator<'config> {}
impl<'config> TechnologyInsertGeneratorTrait for InsertQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<InsertQueries> {
        let logger = self.get_logger();
        logger.info("Generating insert statement for mysql");

        let batch_tables_generator = BatchTablesQueryGenerator { config: &self.config };
        let batch_tables_sql = batch_tables_generator.generate()?;

        let double_staged_tables_generator = DoubleStagedTablesQueryGenerator {
            config: &self.config,
        };
        let double_staged_tables_sql = double_staged_tables_generator.generate()?;

        logger.info("Generated insert statement for mysql");
        let result = InsertQueries {
            batch_tables: batch_tables_sql,
            double_staged_tables: double_staged_tables_sql,
            triple_staged_tables: None,
            redshift_tables: None,
        };
        Ok(result)
    }
}
