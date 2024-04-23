use crate::{
    config::{ Config, DbConfig },
    custom_error::{ CustomError, CustomResult },
    logger::LoggerTrait,
    traits::{ DataSaverTrait, InsertQueries },
};

pub struct DataSaver<'config> {
    pub config: &'config Config,
}

impl<'config> LoggerTrait for DataSaver<'config> {}

impl<'config> DataSaverTrait for DataSaver<'config> {
    fn save(&self, data: &InsertQueries) -> CustomResult<()> {
        self.save_to_files(data, &self.config.target_path.path)?;

        Ok(())
    }

    fn save_to_db(&self, _data: &InsertQueries, _config: &DbConfig) -> CustomResult<()> {
        let logger = self.get_logger();
        logger.error("Not implemented yet");

        Err(CustomError::NotImplemented)
    }
}
