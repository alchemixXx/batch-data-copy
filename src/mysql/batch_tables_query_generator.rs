use crate::{ config::Config, custom_error::CustomResult, traits::TablesInsertQueryGeneratorTrait };

use super::{
    batch_table_query_provider::BatchTableQueryProvider,
    db::get_connection,
    traits::TableQueryGenerator,
};

pub struct BatchTablesQueryGenerator<'config> {
    pub config: &'config Config,
}

impl<'config> TablesInsertQueryGeneratorTrait for BatchTablesQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<Option<String>> {
        let mut result = String::new();
        let mut connection = get_connection(&self.config.source)?;
        let provider = BatchTableQueryProvider { config: &self.config };
        for table in &self.config.tables.batch_tables {
            let mut select_query = provider.get_select_query(&mut connection, table, None)?;
            select_query.push_str(";");
            print!("\nselect_query:\n");
            print!("{}", select_query);
            println!("\n");
            let data = provider.get_data(&mut connection, table, &select_query)?;
            let insert_query = provider.generate_insert_query(&data, &table)?;
            result.push_str(insert_query.as_str());
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}
