use crate::{ custom_error::CustomResult, logger::LoggerTrait, traits::InsertQueries };

use super::redshift_tables_query_generator::RedshiftTablesQueryGenerator;

pub struct InsertQueryGenerator<'config> {
    pub config: &'config crate::config::Config,
}

impl<'config> LoggerTrait for InsertQueryGenerator<'config> {}

impl<'config> InsertQueryGenerator<'config> {
    pub async fn generate(&self) -> CustomResult<InsertQueries> {
        let logger = self.get_logger();
        logger.info("Generating insert statement for redshift");

        let redshift_tables_generator = RedshiftTablesQueryGenerator { config: &self.config };
        let redshift_tables_sql = redshift_tables_generator.generate().await?;

        logger.info("Generated insert statement for redsfhit");
        let result = InsertQueries {
            batch_tables: None,
            double_staged_tables: None,
            triple_staged_tables: None,
            redshift_tables: redshift_tables_sql,
        };
        Ok(result)
    }
}
