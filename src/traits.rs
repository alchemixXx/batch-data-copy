use std::{ fs::{ self, File }, io::Write };

use crate::{ config::DbConfig, custom_error::{ CustomError, CustomResult } };

pub struct InsertQueries {
    pub batch_tables: Option<String>,
    pub double_staged_tables: Option<String>,
    pub triple_staged_tables: Option<String>,
}

pub trait TablesInsertQueryGeneratorTrait {
    fn generate(&self) -> CustomResult<Option<String>>;
}

pub trait TechnologyInsertGeneratorTrait {
    fn generate(&self) -> CustomResult<InsertQueries>;
}

pub trait DataSaverTrait {
    fn save(&self, data: &InsertQueries) -> CustomResult<()>;
    fn save_to_db(&self, data: &InsertQueries, config: &DbConfig) -> CustomResult<()>;
    fn save_to_files(&self, data: &InsertQueries, folder_path: &String) -> CustomResult<()> {
        self.create_folder(folder_path)?;
        if let Some(batch_tables) = &data.batch_tables {
            let file_path = format!("{}/batch_tables.sql", folder_path);
            self.save_to_file(batch_tables, &file_path)?;
        }

        if let Some(double_staged_tables) = &data.double_staged_tables {
            let file_path = format!("{}/double_staged_tables.sql", folder_path);
            self.save_to_file(double_staged_tables, &file_path)?;
        }

        if let Some(triple_staged_tables) = &data.triple_staged_tables {
            let file_path = format!("{}/triple_staged_tables.sql", folder_path);
            self.save_to_file(triple_staged_tables, &file_path)?;
        }

        Ok(())
    }

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

    fn create_folder(&self, folder_path: &String) -> CustomResult<()> {
        let folder_creation_res = fs::create_dir_all(folder_path);

        match folder_creation_res {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    println!("Folder already exists");
                    Ok(())
                } else {
                    Err(CustomError::FolderCreationError)
                }
            }
        }
    }
}
