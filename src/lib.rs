// ! A SQLite-based, single-process, key-value database. You want boringdb if:
// ! - You want high performance somewhat approaching that of databases like sled and RocksDB
// ! - You don't need SQL, multiprocess support, or the other cool features of SQLite
// ! - You want SQLite's proven reliability
// ! ## A note on durability
// ! By default, boringdb has *eventual durability*: database state is guaranteed to be consistent even in the face of arbitrary crashes, and transactions are guaranteed to have "serializable" semantics. However, after a crash the database may be slightly *out of date*, usually by a fraction of a second.
// !
// ! To avoid this behavior, use the [Dict::flush] method to manually force synchronization with disk. Note that this comes with a fairly severe performance cost.

mod dict;
mod db;
mod low_level;
mod types;

pub use dict::*;
pub use db::*;

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
    use std::convert::TryInto;
    use std::ops::Range;

    use easy_parallel::Parallel;
    use env_logger::Env;
    use log::{debug, SetLoggerError};
    use nanorand::Rng;


    use super::*;
    fn init_logs() {
        let initialise_logs: Result<(), SetLoggerError> = env_logger::Builder::from_env(Env::default().default_filter_or("boringdb=debug"))
            .try_init();

        match initialise_logs {
            Ok(_) => debug!("Logging initialised successfully."),
            Err(error) => eprintln!("Error with logs: {}", error),
        }
    }
    #[test]
    fn simple_writes() {
        init_logs();

        let database: Database = Database::open("/tmp/labooyah.db").expect(" Could not open test database.");
        let dict: Dict = database.open_dict("labooyah").expect("Could not open dictionary.");

        let range: Range<i32> = 0..1000;

        range.into_iter().for_each(|_unused_count| {
            let key: String = format!(
                "hello world {}",
                nanorand::tls_rng().generate_range(0..=u64::MAX)
            );

            dict.insert(key.as_bytes().to_vec(), b"oh no".as_ref())
                .expect("Could not insert into dictionary.");
            // log::info!("inserted {}", i);
            // log::info!("got {:?}", dict.get(key.as_bytes()).unwrap().unwrap());
            assert_eq!(
                dict.get(key.as_bytes()).unwrap().unwrap().as_ref(),
                b"oh no"
            );
        });

    }
    #[test]
    fn transactional_increment() {
        init_logs();

        const THREADS: u64 = 100;
        const INCREMENTS: u64 = 100;
        {
            let database: Database = Database::open("/tmp/transactions.db").expect(" Could not open test database.");
            let dict: Dict = database.open_dict("labooyah").expect("Could not open dictionary.");
            dict.insert(b"counter".to_vec(), 0u64.to_be_bytes().to_vec())
                .expect("Could not insert into dictionary.");

            let mut parallel: Parallel<()> = Parallel::new();

            // let range: Range<u64> = 0..THREADS;

            for _ in 0..THREADS {
                parallel = parallel.add(|| {
                    for _ in 0..INCREMENTS {
                        let mut txn = dict.transaction().unwrap();
                        let counter = u64::from_be_bytes(
                            txn.get(b"counter")
                                .unwrap()
                                .unwrap()
                                .as_ref()
                                .try_into()
                                .unwrap(),
                        );
                        txn.insert(b"counter".to_vec(), (counter + 1).to_be_bytes().to_vec())
                            .unwrap();
                    }
                });
            }
            parallel.run();
        }
        let database = Database::open("/tmp/transactions.db").unwrap();
        let dict = database.open_dict("labooyah").unwrap();
        let final_count = u64::from_be_bytes(
            dict.get(b"counter")
                .unwrap()
                .unwrap()
                .as_ref()
                .try_into()
                .unwrap(),
        );
        assert_eq!(final_count, THREADS * INCREMENTS);
    }
}