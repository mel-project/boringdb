//! # BoringDB
//! A SQLite-based, single-process, key-value database.
//!
//! You want boringdb if:
//! - You want high performance somewhat approaching that of databases like sled and RocksDB
//! - You don't need SQL, multiprocess support, or the other cool features of SQLite
//! - You want SQLite's proven reliability
//! ## A note on durability
//! By default, boringdb has *eventual durability*: database state is guaranteed to be consistent even in the face of arbitrary crashes, and transactions are guaranteed to have "serializable" semantics. However, after a crash the database may be slightly *out of date*, usually by a fraction of a second.
//!
//! To avoid this behavior, use the [Dict::flush] method to manually force synchronization with disk. Note that this comes with a fairly severe performance cost.
//!
//!
//! ## Method Of Operation
//! BoringDB stores key/value pairs into byte vectors (`Vec<u8>`).
//!
//! ## Examples
//! Examples can be found in the [examples directory](https://github.com/themeliolabs/boringdb/tree/master/examples)

mod db;
mod dict;
mod globals;
mod low_level;
mod types;
pub use db::*;
pub use dict::*;

pub use types::BoringResult;

use thiserror::Error;

/// A database error. Generally, these errors are quite fatal, and in application code `unwrap`ping them is usually fine.
#[derive(Error, Debug)]
pub enum DbError {
    #[error("internal SQLite error: {0}")]
    SqliteFailed(#[from] rusqlite::Error),
    #[error("writer thread died")]
    WriteThreadFailed,
}

#[cfg(test)]
mod tests {
    use crate::db::Database;
    use crate::dict::Dict;

    use std::ops::Range;

    use bytes::Bytes;
    use env_logger::Env;
    use log::{debug, SetLoggerError};
    fn init_logs() {
        let initialise_logs: Result<(), SetLoggerError> =
            env_logger::Builder::from_env(Env::default().default_filter_or("boringdb=debug"))
                .try_init();

        match initialise_logs {
            Ok(_) => debug!("Logging initialised successfully."),
            Err(error) => eprintln!("Error with logs: {}", error),
        }
    }
    #[test]
    fn simple_writes() {
        init_logs();

        const UNHAPPY_STRING_BYTES: &[u8; 5] = b"oh no";

        let database: Database =
            Database::open("/tmp/labooyah.db").expect(" Could not open test database.");
        let dict: Dict = database
            .open_dict("labooyah")
            .expect("Could not open dictionary.");

        let range: Range<i32> = 0..1000;

        range.into_iter().for_each(|index| {
            let key: String = format!("hello world {}", fastrand::u64(0..=u64::MAX));

            dict.insert(key.as_bytes().to_vec(), UNHAPPY_STRING_BYTES.as_ref())
                .expect("Could not insert into dictionary.");
            // log::info!("inserted {}", i);
            // log::info!("got {:?}", dict.get(key.as_bytes()).unwrap().unwrap());

            let output_bytes: Bytes = dict
                .get(key.as_bytes())
                .expect("BoringResult was None.")
                .expect("Bytes was None");

            assert_eq!(output_bytes.as_ref(), UNHAPPY_STRING_BYTES);
        });
    }
}
