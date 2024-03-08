use mysql::{ prelude::Queryable, Error, PooledConn, Row };

use crate::{ config::Config, custom_error::{ CustomError, CustomResult } };
#[derive(Debug, mysql::prelude::FromRow)]
struct FkColumnUsage {
    pub column_name: String,
    pub referenced_table_name: String,
    pub referenced_column_name: String,
}

pub struct DoubleStagedTableSelectQueryProvider<'config> {
    pub config: &'config Config,
}

impl<'config> DoubleStagedTableSelectQueryProvider<'config> {
    pub fn get_table_name(&self, table: &String) -> String {
        format!("{}_{}_{}", table, self.config.business.lifecycle_id, self.config.business.area_id)
    }

    pub fn get_select_query(
        &self,
        connection: &mut PooledConn,
        table_prefix: &String,
        select_column: Option<String>
    ) -> CustomResult<String> {
        let table = self.get_table_name(table_prefix);
        let columns = self.get_columns(connection, &table)?;
        let selected = self.get_select_column(select_column);
        let references = self.get_table_references(connection, &table)?;

        let mut query = format!("SELECT {} FROM {}", selected, table);

        if columns.contains(&String::from("study_id")) {
            query.push_str(format!(" WHERE study_id = {}", self.config.business.study_id).as_str());
        }

        if columns.contains(&String::from("subject_id")) {
            if let Some(subject_id) = self.config.business.subject_id {
                query.push_str(format!(" AND subject_id = {}", subject_id).as_str());
            }
        }
        if columns.contains(&String::from("job_id")) {
            if let Some(job_id) = self.config.business.job_id {
                query.push_str(format!(" AND job_id = {}", job_id).as_str());
            }
        }

        if columns.contains(&String::from("issue_id")) {
            let issue_table = self.get_table_name(&String::from("issues"));
            let mut subquery = format!(
                "SELECT id FROM {} WHERE study_id = {}",
                issue_table,
                self.config.business.study_id
            );

            if let Some(subject_id) = self.config.business.subject_id {
                subquery.push_str(format!(" AND subject_id = {}", subject_id).as_str());
            }

            query.push_str(format!(" AND issue_id IN ({})", subquery).as_str());
        }

        for reference in references.iter() {
            let subquery = self.get_select_query(
                connection,
                &reference.referenced_table_name,
                Some(reference.referenced_column_name.clone())
            )?;

            query.push_str(
                format!(" AND {} IN (\n{}\n)", reference.column_name, subquery).as_str()
            );
        }

        Ok(query)
    }

    fn get_table_references(
        &self,
        connection: &mut PooledConn,
        table: &String
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
            self.config.source.database
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

    fn get_select_column(&self, select_column: Option<String>) -> String {
        match select_column {
            Some(column) => column,
            None => String::from("*"),
        }
    }

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
}
