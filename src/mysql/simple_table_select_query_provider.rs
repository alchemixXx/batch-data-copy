use crate::config::Config;

pub struct SimpleTableSelectQueryProvider<'config> {
    config: &'config Config,
}

impl<'config> SimpleTableSelectQueryProvider<'config> {
    pub fn get_select_query(&self, table: &String) -> String {
        if table == "cb_batch_runs" {
            let mut query = self.get_cb_batch_runs_select_query();
            query.push_str(";\n\n");

            return query;
        } else if table.starts_with("cb_") {
            return self.get_cb_select_query(table);
        }
    }

    fn get_cb_batch_runs_select_query(&self, table: &String, is_id_required: bool) -> String {
        let mut selected_columns = String::from("*");
        if is_id_required {
            selected_columns = String::from("id");
        }
        let mut query = format!(
            "SELECT {} FROM {} WHERE study_id = {} AND area_id = {} AND lifecycle_id = {}",
            selected_columns,
            table,
            self.config.business.study_id,
            self.config.business.area_id,
            self.config.business.lifecycle_id
        );

        if let Some(subject_id) = self.config.business.subject_id {
            query.push_str(format!(" AND subject_id = {}", subject_id).as_str());
        }

        if let Some(job_id) = self.config.business.job_id {
            query.push_str(format!(" AND job_id = {}", job_id).as_str());
        }

        // if let Some(limit) = self.config.business.limit {
        //     query.push_str(format!(" LIMIT {}", limit).as_str());
        // }

        query
    }

    fn get_cb_select_query(&self, table: &String) -> String {
        format!("SELECT * FROM {}", table)
    }
}
