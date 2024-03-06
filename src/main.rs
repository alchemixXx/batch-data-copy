use clap::Parser;
mod config;
mod cli;
use cli::CLi;
use custom_error::CustomResult;
mod custom_error;
mod mysql;
use mysql::insert_query_generator::InsertQueryGenerator;

use crate::{ custom_error::CustomError, traits::DataInsertQueryGeneratorTrait };
mod traits;

fn main() -> CustomResult<()> {
    println!("Reading cli args...");
    let cli_args = CLi::parse();
    println!("CLI args: {:#?}", cli_args);

    println!("Reading config file...");
    let config = config::read_config(&cli_args.path);
    println!("Read config file");

    if config.technology.category == "mysql" {
        let generator = InsertQueryGenerator { config: &config };
        let sql_statements = generator.generate()?;
        println!("SQL Statements: {}", sql_statements);
        return Ok(());
    }

    Err(CustomError::DbTechnology)
}