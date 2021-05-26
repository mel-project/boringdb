mod dict;
mod low_level;
pub use dict::*;
mod db;
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

/// Result type used throughout the codebase.
pub type Result<T> = std::result::Result<T, DbError>;

#[cfg(test)]
mod tests {
    use nanorand::RNG;

    use env_logger::Env;

    use super::*;
    fn init_logs() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("boringdb=debug"))
            .try_init();
    }
    #[test]
    fn simple_writes() {
        init_logs();
        let database = Database::open("/home/miyuruasuka/labooyah.db").unwrap();
        let dict = database.open_dict("labooyah").unwrap();
        for i in 0..100000 {
            let key = format!(
                "hello world {}",
                nanorand::tls_rng().generate_range(0, u64::MAX)
            );
            dict.insert(key.as_bytes().to_vec(), b"oh no".as_ref())
                .unwrap();
            // log::info!("inserted {}", i);
            // log::info!("got {:?}", dict.get(key.as_bytes()).unwrap().unwrap());
            assert_eq!(
                dict.get(key.as_bytes()).unwrap().unwrap().as_ref(),
                b"oh no"
            );
        }
    }
}
