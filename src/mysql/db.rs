use mysql::*;

use crate::config::DbConfig;
use crate::custom_error::{ CustomResult, CustomError };

pub fn get_connection(db_config: &DbConfig) -> CustomResult<PooledConn> {
    let pool = get_connections_pool(db_config)?;
    let logger = crate::logger::Logger::new();

    let connection = match pool.get_conn() {
        Ok(conn) => {
            logger.warn("Got connection from Pool");
            conn
        }
        Err(err) => {
            logger.error(format!("Can't get connection from Pool: {:#?}", err).as_str());
            return Err(CustomError::DbConnection);
        }
    };

    Ok(connection)
}

pub fn get_connections_pool(db_config: &DbConfig) -> CustomResult<Pool> {
    let logger = crate::logger::Logger::new();
    let url = get_url(db_config);
    let pool = Pool::new(url.as_str());

    match pool {
        Ok(pool) => {
            logger.info("Created connection Pool for DB");
            Ok(pool)
        }
        Err(err) => {
            logger.error(format!("Can't create connection Pool: {:#?}", err).as_str());
            Err(CustomError::DbConnection)
        }
    }
}

fn get_url(db_config: &DbConfig) -> String {
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        db_config.username,
        db_config.password,
        db_config.host,
        db_config.port,
        db_config.database
    );

    url
}
