use std::{path::PathBuf, sync::Mutex};

use rusqlite::OpenFlags;

/// Low-level, interface to the SQLite database, encapsulating pooling etc.
pub(crate) struct LowLevel {
    connection: Mutex<rusqlite::Connection>,
}

impl LowLevel {
    /// Opens a new LowLevel, given a path.
    pub fn open(path: impl Into<PathBuf>) -> rusqlite::Result<Self> {
        let flags = OpenFlags::default() | OpenFlags::SQLITE_OPEN_CREATE;
        let connection = rusqlite::Connection::open_with_flags(path.into(), flags)?;
        // exclusive access to the DB
        connection.query_row("PRAGMA locking_mode = EXCLUSIVE;", [], |f| {
            f.get::<_, String>(0)
        })?;
        // connection.query_row("PRAGMA journal_mode = WAL;", [], |f| f.get::<_, String>(0))?;
        // connection.execute("PRAGMA synchronous = NORMAL;", [])?;
        // memory-mapped I/O
        connection.query_row("PRAGMA mmap_size=1073741824;", [], |f| f.get::<_, i32>(0))?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Runs a transaction in a closure.
    pub fn transaction<T>(
        &self,
        action: impl FnOnce(rusqlite::Transaction) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut conn = self.connection.lock().unwrap();
        let txn = conn.transaction()?;
        action(txn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simple() {
        let low_level = LowLevel::open("/tmp/lltest").unwrap();
        low_level
            .transaction(|conn| {
                conn.execute(
                    "create table if not exists test (key blob primary key, value blob)",
                    [],
                )?;
                conn.commit()?;
                Ok(())
            })
            .unwrap()
    }
}