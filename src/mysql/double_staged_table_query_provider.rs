use mysql::PooledConn;

use crate::{ config::Config, custom_error::CustomResult };

use super::traits::TableQueryGenerator;

pub struct DoubleStagedTableQueryProvider<'config> {
    pub config: &'config Config,
}

impl<'config> DoubleStagedTableQueryProvider<'config> {
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
        let column_names: Vec<String> = columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let selected = self.get_select_column(select_column);
        let references = self.get_table_references(
            connection,
            &table,
            &self.config.source.database
        )?;

        let mut query = format!("SELECT {} FROM {}", selected, table);

        if column_names.contains(&String::from("study_id")) {
            query.push_str(format!(" WHERE study_id = {}", self.config.business.study_id).as_str());
        }

        if column_names.contains(&String::from("subject_id")) {
            if let Some(subject_id) = self.config.business.subject_id {
                query.push_str(format!(" AND subject_id = {}", subject_id).as_str());
            }
        }
        if column_names.contains(&String::from("job_id")) {
            if let Some(job_id) = self.config.business.job_id {
                query.push_str(format!(" AND job_id = {}", job_id).as_str());
            }
        }

        if column_names.contains(&String::from("issue_id")) {
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

    fn get_select_column(&self, select_column: Option<String>) -> String {
        match select_column {
            Some(column) => column,
            None => String::from("*"),
        }
    }
}

impl<'config> TableQueryGenerator for DoubleStagedTableQueryProvider<'config> {}
