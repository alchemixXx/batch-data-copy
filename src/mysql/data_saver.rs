use crate::{ config::Config, custom_error::CustomResult, traits::DataSaverTrait };

pub struct DataSaver<'config> {
    pub config: &'config Config,
}

impl<'config> DataSaverTrait for DataSaver<'config> {
    fn save(&self, data: &String) -> CustomResult<()> {
        if let Some(target) = &self.config.target_file {
            self.save_to_file(data, &target.path)?;
            Ok(())
        } else {
            self.save_to_db(data)?;
            Ok(())
        }
    }

    fn save_to_db(&self, _data: &String) -> CustomResult<()> {
        Ok(())
    }
}
