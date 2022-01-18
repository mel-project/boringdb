use std::path::PathBuf;
use std::sync::Mutex;

/// Low-level, interface to the SQLite database, encapsulating pooling etc.
pub(crate) struct LowLevel {
    connection: Mutex<rusqlite::Connection>,
}

impl Drop for LowLevel {
    fn drop(&mut self) {
        eprintln!("ll is drop");
    }
}

impl LowLevel {
    /// Opens a new LowLevel, given a path.
    pub fn open(path: impl Into<PathBuf>) -> rusqlite::Result<Self> {
        let flags: rusqlite::OpenFlags = rusqlite::OpenFlags::default();
        let connection: rusqlite::Connection =
            rusqlite::Connection::open_with_flags(path.into(), flags)?;
        // exclusive access to the DB
        connection.query_row(
            "PRAGMA locking_mode = EXCLUSIVE;",
            [],
            |row: &rusqlite::Row| row.get::<usize, String>(0),
        )?;
        connection.query_row("PRAGMA journal_mode = DELETE;", [], |f| {
            f.get::<_, String>(0)
        })?;
        // connection.execute("PRAGMA synchronous = NORMAL;", [])?;

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
        let transaction: rusqlite::Transaction = conn.transaction()?;
        action(transaction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simple() {
        let low_level =
            LowLevel::open("/tmp/low_level_test").expect("Could not open low_level_test.");

        low_level
            .transaction(|conn: rusqlite::Transaction| {
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
