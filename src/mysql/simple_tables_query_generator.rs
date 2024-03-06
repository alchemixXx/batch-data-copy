use std::collections::HashMap;
use mysql::{ from_value, prelude::Queryable, PooledConn, Row, Value };
use crate::{
    config::Config,
    custom_error::{ CustomError, CustomResult },
    traits::DataInsertQueryGeneratorTrait,
};

use super::db::{ get_connection };

pub struct SimpleTablesQueryGenerator<'config> {
    pub config: &'config Config,
}

impl<'config> DataInsertQueryGeneratorTrait for SimpleTablesQueryGenerator<'config> {
    fn generate(&self) -> CustomResult<String> {
        let mut result = String::new();
        let mut connection = get_connection(&self.config.source)?;
        for table in &self.config.tables.simple_tables {
            let data = self.get_data(&mut connection, table)?;
            let insert_query = self.generate_insert_query(&data, &table)?;
            // result.push_str(insert_query.as_str());
            println!("{:#?}", insert_query);
            result.push_str(insert_query.as_str());
        }
        Ok(result)
    }
}

// #[derive(Debug, Deserialize)]
// pub struct TaskForCreate {
//     pub Field: String,
// }

impl<'config> SimpleTablesQueryGenerator<'config> {
    fn get_columns(&self, connection: &mut PooledConn, table: &str) -> CustomResult<Vec<String>> {
        let column_query = format!("SHOW COLUMNS FROM {};", table);
        let columns_result = connection.query_map(column_query, |row: Row| {
            let columns = row.columns_ref();
            if columns[0].name_str() != "Field" {
                panic!("Got wrong table definition structure");
            }
            let column: String = row.get(0).unwrap();

            column
        });

        match columns_result {
            Ok(values) => Ok(values),
            Err(_) => Err(CustomError::DbTableStructure),
        }
    }

    fn get_data(
        &self,
        connection: &mut PooledConn,
        table: &str
    ) -> CustomResult<Vec<HashMap<String, mysql::Value>>> {
        let columns = self.get_columns(connection, table)?;
        let data: Vec<HashMap<String, mysql::Value>> = connection
            .query_map(format!("SELECT * FROM {} LIMIT 5;", table), |row: Row| {
                let mut map: HashMap<String, mysql::Value> = HashMap::new();
                for (index, column_name) in columns.iter().enumerate() {
                    map.insert(column_name.clone(), row.get(index).unwrap());
                }
                map
            })
            .unwrap();

        Ok(data)
    }

    fn generate_insert_query(
        &self,
        data: &Vec<HashMap<String, Value>>,
        table: &str
    ) -> CustomResult<String> {
        println!("Generating insert statements for table: {}", table);
        let mut result = String::new();

        let mut columns_populated = false;
        let mut columns: Vec<String> = vec![];
        let mut values_as_strings: Vec<String> = vec![];

        for row in data {
            if !columns_populated {
                columns = row
                    .keys()
                    .map(|key| key.to_string())
                    .collect();
                columns_populated = true;
            }
            println!("{:#?}", columns);
            println!("{:#?}", row);
            let mut values_as_str = String::new();
            for (index, column) in columns.iter().enumerate() {
                let value = row.get(column.as_str()).unwrap();
                let value_str = self.parse_mysql_value_to_string(value);
                values_as_str.push_str(value_str.as_str());

                if index < columns.len() - 1 {
                    values_as_str.push_str(", ");
                }
            }

            values_as_strings.push(values_as_str);
        }
        println!("Generated insert statements for table: {}", table);

        columns = columns
            .iter()
            .map(|column| format!("`{}`", column))
            .collect();

        let insert_query = format!(
            "INSERT INTO\n{} ({})\nVALUES\n({});",
            table,
            columns.join(", "),
            values_as_strings.join("), \n(")
        );

        result.push_str(insert_query.as_str());
        result.push_str("\n");

        Ok(result)
    }

    fn parse_mysql_value_to_string(&self, value: &Value) -> String {
        match value {
            mysql::Value::NULL => "NULL".to_string(),
            _ => {
                let mut value = from_value::<String>(value.clone());
                if value.contains('\'') {
                    value = value.replace('\'', "\\'");
                }
                format!("'{}'", value)
            }
        }
    }
}
