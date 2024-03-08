use crate::{ config::Config, custom_error::CustomResult, traits::DataInsertQueryGeneratorTrait };

use super::{
    db::get_connection,
    double_staged_table_select_query_provider::DoubleStagedTableSelectQueryProvider,
    traits::TableQueryGenerator,
};

pub struct DoubleStagedTablesQueryGenerator<'config> {
    pub config: &'config Config,
}

impl<'config> DataInsertQueryGeneratorTrait for DoubleStagedTablesQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<String> {
        let mut result = String::new();
        let mut connection = get_connection(&self.config.source)?;
        let select_query_provider = DoubleStagedTableSelectQueryProvider { config: &self.config };
        for table_prefix in &self.config.tables.double_partitioned_tables {
            let table = select_query_provider.get_table_name(table_prefix);
            let mut select_query = select_query_provider.get_select_query(
                &mut connection,
                table_prefix,
                None
            )?;
            select_query.push_str(";");
            println!("Select query: {}", select_query);
            let data = self.get_data(&mut connection, &table, &select_query)?;
            let insert_query = self.generate_insert_query(&data, &table)?;
            result.push_str(insert_query.as_str());
        }
        Ok(result)
    }
}

impl<'config> TableQueryGenerator for DoubleStagedTablesQueryGenerator<'config> {}
