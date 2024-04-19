use sqlx::Pool;
use sqlx::postgres::Postgres;

use crate::config::DbConfig;
use crate::custom_error::{ CustomResult, CustomError };

pub async fn get_connections_pool(db_config: &DbConfig) -> CustomResult<Pool<Postgres>> {
    let url = get_url(db_config);
    let pool = Pool::<Postgres>::connect(&url).await;

    match pool {
        Ok(pool) => {
            println!("Created connection Pool for DB");
            Ok(pool)
        }
        Err(err) => {
            println!("Can't create connection Pool: {:#?}", err);
            Err(CustomError::DbConnection)
        }
    }
}

fn get_url(db_config: &DbConfig) -> String {
    let url = format!(
        "redshift://{}:{}@{}:{}/{}",
        db_config.username,
        db_config.password,
        db_config.host,
        db_config.port,
        db_config.database
    );

    url
}
