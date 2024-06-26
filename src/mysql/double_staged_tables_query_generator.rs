use crate::{
    config::Config,
    custom_error::CustomResult,
    logger::LoggerTrait,
    traits::TablesInsertQueryGeneratorTrait,
};

use super::{
    db::get_connection,
    double_staged_table_query_provider::DoubleStagedTableQueryProvider,
    traits::TableQueryGenerator,
};

pub struct DoubleStagedTablesQueryGenerator<'config> {
    pub config: &'config Config,
}

impl<'config> LoggerTrait for DoubleStagedTablesQueryGenerator<'config> {}
impl<'config> TablesInsertQueryGeneratorTrait for DoubleStagedTablesQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<Option<String>> {
        let logger = self.get_logger();
        let mut result = String::new();
        let mut connection = get_connection(&self.config.source)?;
        let provider = DoubleStagedTableQueryProvider { config: &self.config };
        for table_prefix in &self.config.tables.double_partitioned_tables {
            let table = provider.get_table_name(table_prefix);
            let mut select_query = provider.get_select_query(&mut connection, table_prefix, None)?;
            select_query.push_str(";");
            logger.info(format!("\nselect query:\n\n {}\n\n", select_query).as_str());
            let data = provider.get_data(&mut connection, &table, &select_query)?;
            let insert_query = provider.generate_insert_query(&data, &table)?;
            logger.info(format!("\ninsert query:\n\n {}\n\n", insert_query).as_str());
            result.push_str(insert_query.as_str());
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}
