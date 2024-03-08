use std::collections::HashMap;

use mysql::{ from_value, prelude::Queryable, PooledConn, Row, Value };

use crate::custom_error::{ CustomError, CustomResult };

#[derive(Debug, Clone)]
pub struct ColumnProps {
    pub name: String,
    pub data_type: String,
}

type ColumnData = HashMap<String, (ColumnProps, Value)>;

pub trait TableQueryGenerator {
    fn get_columns(
        &self,
        connection: &mut PooledConn,
        table: &str
    ) -> CustomResult<Vec<ColumnProps>> {
        let column_query = format!("SHOW COLUMNS FROM {};", table);
        let columns_result = connection.query_map(column_query, |row: Row| {
            let columns = row.columns_ref();
            if columns[0].name_str() != "Field" {
                panic!("Got wrong table definition structure");
            }
            let column_name: String = row.get(0).unwrap();

            if columns[1].name_str() != "Type" {
                panic!("Got wrong table definition structure");
            }
            let data_type: String = row.get(1).unwrap();

            ColumnProps {
                name: column_name,
                data_type: data_type,
            }
        });

        match columns_result {
            Ok(values) => Ok(values),
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
                println!("{:#?}", column_pros);
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
                let value = from_value::<String>(value.clone());
                // if value.contains('\'') {
                //     value = value.replace('\'', "\\'");
                // }
                format!("'{}'", value)
            }
        }
    }
}
