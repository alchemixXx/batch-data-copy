use mysql::{ prelude::Queryable, PooledConn };

use crate::{
    config::{ Config, DbConfig },
    custom_error::{ CustomError, CustomResult },
    traits::{ DataSaverTrait, InsertQueries },
};

use super::db::get_connection;

pub struct DataSaver<'config> {
    pub config: &'config Config,
}

impl<'config> DataSaverTrait for DataSaver<'config> {
    fn save(&self, data: &InsertQueries) -> CustomResult<()> {
        self.save_to_files(data, &self.config.target_path.path)?;

        if let Some(target_db) = &self.config.target_db {
            self.save_to_db(data, &target_db)?;
        }

        Ok(())
    }

    fn save_to_db(&self, data: &InsertQueries, config: &DbConfig) -> CustomResult<()> {
        let mut connection = get_connection(config)?;

        self.exec_no_output_statement(&mut connection, &"SET FOREIGN_KEY_CHECKS = 0".to_string())?;
        if let Some(batch_sql) = &data.batch_tables {
            println!("Executing batch tables");
            self.exec_no_output_statement(&mut connection, batch_sql)?;
            println!("Batch tables executed");
        }

        if let Some(double_staged_sql) = &data.double_staged_tables {
            println!("Executing double staged tables");
            self.exec_no_output_statement(&mut connection, double_staged_sql)?;
            println!("Double staged tables executed");
        }

        if let Some(triple_staged_sql) = &data.triple_staged_tables {
            println!("Executing triple staged tables");
            self.exec_no_output_statement(&mut connection, triple_staged_sql)?;
            println!("Triple staged tables executed");
        }

        self.exec_no_output_statement(&mut connection, &"SET FOREIGN_KEY_CHECKS = 1".to_string())?;

        Ok(())
    }
}

impl DataSaver<'_> {
    fn exec_no_output_statement(
        &self,
        connection: &mut PooledConn,
        query: &String
    ) -> CustomResult<()> {
        let result = connection.query_drop(query);

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(CustomError::DbQueryExecution(err.to_string())),
        }
    }
}
