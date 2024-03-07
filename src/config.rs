use serde_derive::Deserialize;

use std::fs;
use toml;

#[derive(Debug, Deserialize, Clone)]
pub struct TablesConfig {
    pub batch_tables: Vec<String>,
    pub partitioned_tables: Vec<String>,
    pub double_partitioned_tables: Vec<String>,
    pub triple_partitioned_tables: Vec<String>,
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
    pub subject_id: Option<u8>,
    pub job_id: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbTechnology {
    pub category: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TargetFile {
    pub path: String,
}

// Top level struct to hold the TOML data.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub source: DbConfig,
    pub target_db: DbConfig,
    pub target_file: Option<TargetFile>,
    pub tables: TablesConfig,
    pub technology: DbTechnology,
    pub business: BatchConfig,
}

pub fn read_config(path: &str) -> Config {
    println!("Reading config file: {}", path);
    let contents = fs
        ::read_to_string(path)
        .expect(format!("Could not read file `{}`", path).as_str());

    let data: Config = toml
        ::from_str(&contents)
        .expect(format!("Unable to load data from `{}`", path).as_str());
    println!("Read config file: {}", path);
    println!("{:#?}", data);

    data
}
