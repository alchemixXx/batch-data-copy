use crate::{ config::Config, custom_error::CustomResult };

use super::{ db::get_connections_pool, redshift_table_query_provider::RedshiftTableQueryProvider };
use super::traits::TableQueryGenerator;

pub struct RedshiftTablesQueryGenerator<'config> {
    pub config: &'config Config,
}

impl<'config> RedshiftTablesQueryGenerator<'config> {
    pub async fn generate(&self) -> CustomResult<Option<String>> {
        let mut result = String::new();
        let mut pool = get_connections_pool(&self.config.redshift_db).await?;
        let provider = RedshiftTableQueryProvider { config: &self.config };
        for table in &self.config.tables.redshift_tables {
            let mut select_query = provider.get_select_query(&mut pool, table, None)?;
            select_query.push_str(";");
            print!("\nselect_query:\n");
            print!("{}", select_query);
            println!("\n");
            let data = provider.get_data(&mut pool, table, &select_query).await?;
            let insert_query = provider.generate_insert_query(&data, &table)?;
            print!("\n insert query :\n");
            print!("{}", insert_query);
            println!("\n");
            result.push_str(insert_query.as_str());
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}
