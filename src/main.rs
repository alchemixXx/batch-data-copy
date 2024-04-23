use clap::Parser;
mod config;
mod cli;
mod logger;
use cli::CLi;
use custom_error::CustomResult;
mod custom_error;
mod mysql;
use mysql::insert_query_generator::InsertQueryGenerator as MySqlInsertQueryGenerator;
use mysql::data_saver::DataSaver as MySqlDataSaver;

mod redshift;
use redshift::insert_query_generator::InsertQueryGenerator as RedshiftInsertQueryGenerator;
use mysql::data_saver::DataSaver as RedshiftDataSaver;
use crate::{
    custom_error::CustomError,
    traits::{ TechnologyInsertGeneratorTrait, DataSaverTrait },
};
mod traits;

#[tokio::main]
async fn main() -> CustomResult<()> {
    let cli_args = CLi::parse();
    let config = config::read_config(&cli_args.path);

    logger::Logger::init(config.log.log_level);

    if config.tables.redshift_tables.len() > 0 {
        let generator = RedshiftInsertQueryGenerator { config: &config };
        let sql_statements = generator.generate().await?;
        let saver = RedshiftDataSaver { config: &config };
        saver.save(&sql_statements)?;
    }

    if config.technology.category == "mysql" {
        let generator = MySqlInsertQueryGenerator { config: &config };
        let sql_statements = generator.generate()?;
        let saver = MySqlDataSaver { config: &config };
        saver.save(&sql_statements)?;
        return Ok(());
    }

    Err(CustomError::DbTechnology)
}
