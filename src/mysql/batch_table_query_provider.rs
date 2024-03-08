use mysql::PooledConn;

use crate::{ config::Config, custom_error::CustomResult };

use super::traits::TableQueryGenerator;

pub struct BatchTableQueryProvider<'config> {
    pub config: &'config Config,
}

impl<'config> BatchTableQueryProvider<'config> {
    pub fn get_select_query(
        &self,
        connection: &mut PooledConn,
        table: &String,
        select_column: Option<String>
    ) -> CustomResult<String> {
        if table == "cb_batch_runs" {
            self.get_cb_batch_runs_select_query(table, select_column)
        } else if table.starts_with("cb_") {
            self.get_cb_select_query(connection, table, select_column)
        } else {
            self.get_general_select_query(table)
        }
    }

    fn get_cb_batch_runs_select_query(
        &self,
        table: &String,
        select_column: Option<String>
    ) -> CustomResult<String> {
        let selected = self.get_select_column(select_column);

        let mut query = format!(
            "SELECT {} FROM {} WHERE study_id = {} AND area_id = {} AND lifecycle_id = {}",
            selected,
            table,
            self.config.business.study_id,
            self.config.business.area_id,
            self.config.business.lifecycle_id
        );

        if let Some(job_id) = self.config.business.job_id {
            query.push_str(format!(" AND job_id = {}", job_id).as_str());
        }

        Ok(query)
    }

    fn get_cb_select_query(
        &self,
        connection: &mut PooledConn,
        table: &String,
        select_column: Option<String>
    ) -> CustomResult<String> {
        let references = self.get_table_references(
            connection,
            table,
            &self.config.source.database
        )?;
        let selected = self.get_select_column(select_column);
        let mut query = format!("SELECT {} FROM {}", selected, table);
        for (index, reference) in references.iter().enumerate() {
            let subquery = self.get_select_query(
                connection,
                &reference.referenced_table_name,
                Some(reference.referenced_column_name.clone())
            )?;
            if index == 0 {
                query.push_str(
                    format!("\nWHERE {} IN (\n{}\n)", reference.column_name, subquery).as_str()
                );
            } else {
                query.push_str(
                    format!(" AND {} IN (\n{}\n)", reference.column_name, subquery).as_str()
                );
            }
        }

        Ok(query)
    }

    fn get_general_select_query(&self, table: &String) -> CustomResult<String> {
        Ok(format!("SELECT * FROM {}", table))
    }

    fn get_select_column(&self, select_column: Option<String>) -> String {
        match select_column {
            Some(column) => column,
            None => String::from("*"),
        }
    }
}

impl<'config> TableQueryGenerator for BatchTableQueryProvider<'config> {}
