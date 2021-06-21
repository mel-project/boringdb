use crate::Result;
use crate::{low_level::LowLevel, Dict};
use std::{path::PathBuf, sync::Arc};

/// A database full of maps.
pub struct Database {
    low_level: Arc<LowLevel>,
}

impl Database {
    /// Opens a database given a filename.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let ll = LowLevel::open(path)?;
        Ok(Self {
            low_level: Arc::new(ll),
        })
    }

    /// Opens a dictionary given its name. Creates a new empty map if the map doesn't exist
    pub fn open_dict(&self, name: &str) -> Result<Dict> {
        self.low_level.transaction(|txn| {
            txn.execute(
                &format!(
                    "create table if not exists {} (key BLOB PRIMARY KEY, value BLOB NOT NULL) without rowid",
                    name
                ),
                [],
            )?;
            txn.commit()?;
            Ok(())
        })?;
        Ok(Dict::new(self.low_level.clone(), name))
    }
}
