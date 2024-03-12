use std::collections::HashMap;

use mysql::{ from_value, prelude::Queryable, Error, PooledConn, Row, Value };

use crate::custom_error::{ CustomError, CustomResult };

#[derive(Debug, Clone)]
pub struct ColumnProps {
    pub name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub key: String,
    pub default_value: Option<String>,
    pub extra: String,
}

#[derive(Debug, mysql::prelude::FromRow)]
pub struct FkColumnUsage {
    pub column_name: String,
    pub referenced_table_name: String,
    pub referenced_column_name: String,
}

type ColumnData = HashMap<String, (ColumnProps, Value)>;

pub trait TableQueryGenerator {
    fn get_columns(
        &self,
        connection: &mut PooledConn,
        table: &str
    ) -> CustomResult<Vec<ColumnProps>> {
        let column_query = format!("SHOW COLUMNS FROM {};", table);

        let raw_results = connection
            .query_map(
                column_query,
                |(field_value, type_value, null_value, key_value, default_value, extra)| {
                    let props = ColumnProps {
                        name: field_value,
                        data_type: type_value,
                        is_nullable: null_value,
                        key: key_value,
                        default_value,
                        extra,
                    };

                    props
                }
            )
            .map_err(|err| CustomError::DbQueryExecution(err.to_string()));

        match raw_results {
            Ok(results) => Ok(results),
            Err(_) => Err(CustomError::DbTableStructure),
        }
    }

    fn get_data(
        &self,
        connection: &mut PooledConn,
        table: &str,
        query: &str
    ) -> CustomResult<Vec<ColumnData>> {
        let columns = self.get_columns(connection, table)?;
        let data: Vec<HashMap<String, (ColumnProps, mysql::Value)>> = connection
            .query_map(query, |row: Row| {
                let mut map: HashMap<String, (ColumnProps, mysql::Value)> = HashMap::new();
                for (index, props) in columns.iter().enumerate() {
                    map.insert(props.name.clone(), (props.clone(), row.get(index).unwrap()));
                }
                map
            })
            .unwrap();

        Ok(data)
    }

    fn generate_insert_query(&self, data: &Vec<ColumnData>, table: &str) -> CustomResult<String> {
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
            let mut values_as_str = String::new();
            for (index, column) in columns.iter().enumerate() {
                let params = row.get(column.as_str()).unwrap();
                let value_str = self.parse_mysql_value_to_string(&params.0, &params.1);
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

        if columns.len() > 0 {
            result.push_str(insert_query.as_str());
            result.push_str("\n");
        }

        Ok(result)
    }

    fn parse_mysql_value_to_string(&self, column_pros: &ColumnProps, value: &Value) -> String {
        match value {
            mysql::Value::NULL => "NULL".to_string(),
            mysql::Value::Bytes(bytes) => {
                if column_pros.data_type.starts_with("binary") {
                    let hex_string: String = bytes
                        .iter()
                        .map(|b| format!("{:02X}", b))
                        .collect();
                    format!("X'{}'", hex_string)
                } else {
                    let mut value = from_value::<String>(value.clone());
                    if value.contains('\'') {
                        value = value.replace('\'', "\\'");
                    }
                    format!("'{}'", value)
                }
            }
            _ => {
                let mut value = from_value::<String>(value.clone());
                if value.contains('\'') {
                    value = value.replace('\'', "\\'");
                }
                format!("'{}'", value)
            }
        }
    }

    fn get_table_references(
        &self,
        connection: &mut PooledConn,
        table: &String,
        database: &String
    ) -> CustomResult<Vec<FkColumnUsage>> {
        let query = format!(
            r#"
            SELECT
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME  
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                TABLE_NAME = '{}' AND TABLE_SCHEMA = '{}'
                AND REFERENCED_COLUMN_NAME IS NOT NULL
                AND REFERENCED_TABLE_NAME IS NOT NULL
            "#,
            table,
            database
        );

        let raw_results: Result<Vec<FkColumnUsage>, Error> = connection.query_map(
            query,
            |(column_name, referenced_table_name, referenced_column_name)| {
                FkColumnUsage {
                    column_name,
                    referenced_table_name,
                    referenced_column_name,
                }
            }
        );

        match raw_results {
            Ok(results) => Ok(results),
            Err(_) => Err(CustomError::DbTableStructure),
        }
    }
}
