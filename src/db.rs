use crate::types::BoringResult;
use crate::{low_level::LowLevel, Dict};
use std::{path::PathBuf, sync::Arc};

/// A database full of maps.
pub struct Database {
    low_level: Arc<LowLevel>,
}

impl Database {
    /// Opens a database given a filename.
    pub fn open(path: impl Into<PathBuf>) -> BoringResult<Self> {
        let low_level: LowLevel = LowLevel::open(path)?;
        Ok(Self {
            low_level: Arc::new(low_level),
        })
    }

    /// Opens a dictionary given its name. Creates a new empty map if the map doesn't exist
    pub fn open_dict(&self, name: &str) -> BoringResult<Dict> {
        self.low_level.transaction(|transaction| {
            transaction.execute(
                &format!(
                    "create table if not exists {} (key BLOB PRIMARY KEY, value BLOB NOT NULL)",
                    name
                ),
                [],
            )?;
            transaction.commit()?;
            Ok(())
        })?;
        Ok(Dict::new(self.low_level.clone(), name))
    }
}