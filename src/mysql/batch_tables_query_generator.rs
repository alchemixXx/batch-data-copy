use crate::{ config::Config, custom_error::CustomResult, traits::DataInsertQueryGeneratorTrait };

use super::{
    batch_table_select_query_provider::BatchTableSelectQueryProvider,
    db::get_connection,
    traits::TableQueryGenerator,
};

pub struct BatchTablesQueryGenerator<'config> {
    pub config: &'config Config,
}

impl<'config> DataInsertQueryGeneratorTrait for BatchTablesQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<String> {
        let mut result = String::new();
        let mut connection = get_connection(&self.config.source)?;
        let select_query_provider = BatchTableSelectQueryProvider { config: &self.config };
        for table in &self.config.tables.batch_tables {
            let mut select_query = select_query_provider.get_select_query(
                &mut connection,
                table,
                None
            )?;
            select_query.push_str(";");
            let data = self.get_data(&mut connection, table, &select_query)?;
            let insert_query = self.generate_insert_query(&data, &table)?;
            result.push_str(insert_query.as_str());
        }
        Ok(result)
    }
}

impl<'config> TableQueryGenerator for BatchTablesQueryGenerator<'config> {}
