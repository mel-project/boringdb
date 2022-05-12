use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::Connection;

/// Low-level, interface to the SQLite database, encapsulating pooling etc.
pub(crate) struct LowLevel {
    conn_send: flume::Sender<Connection>,
    conn_recv: flume::Receiver<Connection>,
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
        let (conn_send, conn_recv) = flume::bounded(16);
        let path: PathBuf = path.into();
        for _ in 0..16 {
            let connection: rusqlite::Connection =
                rusqlite::Connection::open_with_flags(path.clone(), flags)?;
            connection.query_row("PRAGMA journal_mode = WAL;", [], |f| f.get::<_, String>(0))?;
            connection.execute("PRAGMA synchronous = NORMAL;", [])?;
            conn_send.send(connection).unwrap();
        }
        Ok(Self {
            conn_send,
            conn_recv,
        })
    }

    /// Runs a transaction in a closure.
    pub fn transaction<T>(
        &self,
        mut action: impl FnMut(rusqlite::Transaction) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut conn = self.conn_recv.recv().unwrap();
        let ee;
        loop {
            let transaction = conn.transaction();
            match transaction {
                Ok(transaction) => match action(transaction) {
                    Ok(t) => {
                        ee = Ok(t);
                        break;
                    }
                    Err(err) => {
                        if err.to_string().contains("busy") {
                            continue;
                        } else {
                            ee = Err(err);
                            break;
                        }
                    }
                },
                Err(err) => {
                    ee = Err(err);
                    break;
                }
            }
        }
        self.conn_send.send(conn).unwrap();
        ee
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
