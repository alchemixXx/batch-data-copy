use sqlx::{ Pool, Postgres };
use crate::{ config::Config, custom_error::CustomResult };

use std::collections::HashMap;

use super::traits::TableQueryGenerator;

pub struct RedshiftTableQueryProvider<'config> {
    pub config: &'config Config,
}

impl<'config> TableQueryGenerator for RedshiftTableQueryProvider<'config> {}
impl<'config> RedshiftTableQueryProvider<'config> {
    pub fn get_select_query(
        &self,
        pool: &mut Pool<Postgres>,
        table: &String,
        _select_column: Option<String>
    ) -> CustomResult<String> {
        if table == "records_trail" {
            return self.get_records_trail_select(pool);
        }

        if table == "audit" {
            return self.get_audit_select(pool);
        }
        Ok(format!("SELECT * FROM {};", table))
    }

    fn get_records_trail_select(&self, _pool: &mut Pool<Postgres>) -> CustomResult<String> {
        let mut query = format!(
            "SELECT * FROM records_trail_{} WHERE study_id={} AND (parent_area_id={} OR child_area_id={})",
            self.config.business.lifecycle_id,
            self.config.business.study_id,
            self.config.business.area_id,
            self.config.business.area_id
        );

        if let Some(subject_id) = self.config.business.subject_id {
            query.push_str(format!(" AND subject_id={}", subject_id).as_str());
        }

        if let Some(job_id) = self.config.business.job_id {
            query.push_str(format!(" AND job_id={}", job_id).as_str());
        }

        if let Some(limit) = self.config.business.limit {
            query.push_str(format!(" LIMIT {}", limit).as_str());
        }

        Ok(query)
    }
    fn get_audit_select(&self, _pool: &mut Pool<Postgres>) -> CustomResult<String> {
        let mut query = format!(
            "SELECT * FROM audit WHERE study_id={} AND lifecycle_id={} AND area_id={}",
            self.config.business.study_id,
            self.config.business.lifecycle_id,
            self.config.business.area_id
        );

        if let Some(subject_id) = self.config.business.subject_id {
            query.push_str(format!(" AND subject_id={}", subject_id).as_str());
        }

        if let Some(job_id) = self.config.business.job_id {
            query.push_str(format!(" AND job_id={}", job_id).as_str());
        }

        if let Some(limit) = self.config.business.limit {
            query.push_str(format!(" LIMIT {}", limit).as_str());
        }

        Ok(query)
    }

    pub fn generate_insert_query(
        &self,
        data: &Vec<HashMap<String, String>>,
        table: &String
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
            let mut values_as_str = String::new();
            for (index, column) in columns.iter().enumerate() {
                let value = row.get(column.as_str()).unwrap();
                values_as_str.push_str(value);

                if index < columns.len() - 1 {
                    values_as_str.push_str(", ");
                }
            }

            values_as_strings.push(values_as_str);
        }

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
        println!("Generated insert statements for table: {}", table);

        Ok(result)
    }
}
