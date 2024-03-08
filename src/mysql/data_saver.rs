use crate::{
    config::Config,
    custom_error::{ CustomError, CustomResult },
    traits::{ DataSaverTrait, InsertQueries },
};

pub struct DataSaver<'config> {
    pub config: &'config Config,
}

impl<'config> DataSaverTrait for DataSaver<'config> {
    fn save(&self, data: &InsertQueries) -> CustomResult<()> {
        if let Some(target) = &self.config.target_file {
            self.save_to_files(data, &target.path)?;
        } else if let Some(_target_db) = &self.config.target_db {
            self.save_to_db(data)?;
        } else {
            println!("No target specified. Output will be printed to the console");
            if let Some(batch_tables) = &data.batch_tables {
                println!("Batch tables:");
                println!("{}", batch_tables);
            }
            if let Some(double_staged_tables) = &data.double_staged_tables {
                println!("Double staged tables:");
                println!("{}", double_staged_tables);
            }

            if let Some(triple_staged_tables) = &data.triple_staged_tables {
                println!("Triple staged tables:");
                println!("{}", triple_staged_tables);
            }
            println!("Output has been printed to the console");
        }

        Ok(())
    }

    fn save_to_db(&self, _data: &InsertQueries) -> CustomResult<()> {
        Err(CustomError::NotImplemented)
    }
}
