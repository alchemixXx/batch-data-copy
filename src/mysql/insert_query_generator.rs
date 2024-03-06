use crate::{ custom_error::CustomResult, traits::DataInsertQueryGeneratorTrait };

use super::simple_tables_query_generator::SimpleTablesQueryGenerator;

pub struct InsertQueryGenerator<'config> {
    pub config: &'config crate::config::Config,
}

impl<'config> DataInsertQueryGeneratorTrait for InsertQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<String> {
        let mut result = String::new();
        println!("Generating insert statement for mysql");

        let simple_tables_generator = SimpleTablesQueryGenerator { config: &self.config };
        let simple_tables_sql = simple_tables_generator.generate()?;

        result.push_str(simple_tables_sql.as_str());

        println!("Generated insert statement for mysql");
        Ok(result)
    }
}
