use serde_derive::Deserialize;

use std::fs;
use toml;

use crate::logger::LogLevel;

#[derive(Debug, Deserialize, Clone)]
pub struct TablesConfig {
    pub batch_tables: Vec<String>,
    pub partitioned_tables: Vec<String>,
    pub double_partitioned_tables: Vec<String>,
    pub triple_partitioned_tables: Vec<String>,
    pub redshift_tables: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbConfig {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BatchConfig {
    pub study_id: i64,
    pub area_id: u8,
    pub lifecycle_id: u8,
    pub subject_id: Option<i64>,
    pub job_id: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbTechnology {
    pub category: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TargetPath {
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogsConfig {
    pub log_level: LogLevel,
}

// Top level struct to hold the TOML data.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub source: DbConfig,
    pub redshift_db: DbConfig,
    pub target_db: Option<DbConfig>,
    pub target_path: TargetPath,
    pub tables: TablesConfig,
    pub technology: DbTechnology,
    pub business: BatchConfig,
    pub log: LogsConfig,
}

pub fn read_config(path: &str) -> Config {
    println!("Reading config file: {}", path);
    let content_result = fs::read_to_string(path);

    let contents = match content_result {
        Ok(contents) => contents,
        Err(error) => {
            println!("Error reading file: {}", error);
            std::process::exit(1);
        }
    };

    let data_result = toml::from_str(&contents);
    let data: Config = match data_result {
        Ok(data) => data,
        Err(error) => {
            println!("Error parsing file: {}", error);
            std::process::exit(1);
        }
    };
    println!("Read config file: {}", path);
    println!("{:#?}", data);

    data
}
