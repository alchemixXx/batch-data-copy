use std::collections::HashMap;

use sqlx::postgres::{ types::Oid, PgRow };
use sqlx::types::chrono::{ DateTime, NaiveDateTime, Utc };
use sqlx::{ Pool, Postgres, Row };
use sqlx::Column;

use crate::custom_error::{ CustomError, CustomResult };

pub trait TableQueryGenerator {
    async fn get_data(
        &self,
        pool: &Pool<Postgres>,
        _table: &String,
        query: &String
    ) -> CustomResult<Vec<HashMap<String, String>>> {
        let data = sqlx
            ::query(query.as_str())
            .fetch_all(pool).await
            .map_err(|e| CustomError::DbQueryExecution(e.to_string()))?;

        let records = data
            .into_iter()
            .map(|row| {
                let record = self.pg_row_to_hashmap(&row);
                record
            })
            .collect::<Vec<HashMap<String, String>>>();

        Ok(records)
    }

    fn pg_row_to_hashmap(&self, row: &PgRow) -> HashMap<String, String> {
        let mut hashmap = HashMap::new();
        for (i, column) in row.columns().iter().enumerate() {
            let value = match column.type_info().oid().unwrap() {
                Oid(16) => {
                    let value = row.try_get::<Option<bool>, _>(i).unwrap();
                    match value {
                        Some(val) => val.to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(20) => {
                    let value = row.try_get::<Option<i64>, _>(i).unwrap();
                    match value {
                        Some(val) => val.to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(21) => {
                    let value = row.try_get::<Option<i16>, _>(i).unwrap();
                    match value {
                        Some(val) => val.to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(23) => {
                    let value = row.try_get::<Option<i32>, _>(i).unwrap();
                    match value {
                        Some(val) => val.to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(700) => {
                    let value = row.try_get::<Option<f32>, _>(i).unwrap();
                    match value {
                        Some(val) => val.to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(701) => {
                    let value = row.try_get::<Option<f64>, _>(i).unwrap();
                    match value {
                        Some(val) => val.to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(1114) => {
                    let timestamp = row.try_get::<Option<NaiveDateTime>, _>(i).unwrap();
                    match timestamp {
                        Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                        None => "NULL".to_string(),
                    }
                }
                Oid(1184) => {
                    let timestamp = row.try_get::<Option<DateTime<Utc>>, _>(i).unwrap();

                    match timestamp {
                        Some(timestamp) => timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                        None => "NULL".to_string(),
                    }
                }
                _ => row.try_get::<String, _>(i).unwrap().to_string(),
            };
            hashmap.insert(column.name().to_string(), value);
        }
        hashmap
    }
}
