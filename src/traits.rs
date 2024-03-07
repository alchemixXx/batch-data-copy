use std::{ fs::File, io::Write };

use crate::custom_error::{ CustomResult, CustomError };

pub trait DataInsertQueryGeneratorTrait {
    fn generate(&self) -> CustomResult<String>;
}

pub trait DataSaverTrait {
    fn save(&self, data: &String) -> CustomResult<()>;
    fn save_to_db(&self, data: &String) -> CustomResult<()>;
    fn save_to_file(&self, data: &String, file_path: &String) -> CustomResult<()> {
        let file_result = File::create(file_path);

        let mut file = match file_result {
            Ok(file) => file,
            Err(_) => {
                return Err(CustomError::FileCreationError);
            }
        };

        // Write the data to the file
        let write_result = file.write_all(data.as_bytes());

        match write_result {
            Ok(_) => Ok(()),
            Err(_) => Err(CustomError::FileDataInsertionError),
        }
    }
}
