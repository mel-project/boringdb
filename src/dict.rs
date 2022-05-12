#![allow(clippy::mutable_key_type)]
use crate::types::BoringResult;
use crate::{low_level::LowLevel, DbError};

use std::sync::Arc;

use bytes::Bytes;
use rusqlite::{params, OptionalExtension};

/// A non-cloneable on-disk mapping
struct DictInner {
    low_level: Arc<LowLevel>,
    read_statement: String,
    write_statement: String,
    delete_statement: String,
    table_name: String,
}

impl DictInner {
    /// Create a new map inner.
    fn new(low_level: Arc<LowLevel>, table_name: &str) -> Self {
        let write_statement = format!("insert into {}(key, value) values ($1, $2) on conflict(key) do update set value = excluded.value", table_name);
        let delete_statement = format!("delete from {} where key = $1", table_name);
        Self {
            low_level,
            read_statement: format!("select value from {} where key = $1", table_name),
            table_name: table_name.to_string(),
            write_statement,
            delete_statement,
        }
    }

    /// Writes a key-value pair, returning the previous value
    fn write(&self, key: Bytes, value: Bytes) -> BoringResult<Option<Bytes>> {
        self.low_level
            .transaction(|txn| {
                let existing: Option<Vec<u8>> = txn
                    .prepare_cached(&self.read_statement)?
                    .query_row(params![key.as_ref()], |r| r.get(0))
                    .optional()?;
                txn.prepare_cached(&self.write_statement)?
                    .execute(params![key.as_ref(), value.as_ref()])?;
                txn.commit()?;
                if let Some(existing) = existing {
                    Ok(Some(existing.into()))
                } else {
                    Ok(None)
                }
            })
            .map_err(DbError::SqliteFailed)
    }

    /// Writes a key-value pair, returning the previous value
    fn remove(&self, key: &[u8]) -> BoringResult<Option<Bytes>> {
        self.low_level
            .transaction(|txn| {
                let existing: Option<Vec<u8>> = txn
                    .prepare_cached(&self.read_statement)?
                    .query_row(params![key], |r| r.get(0))
                    .optional()?;
                txn.prepare_cached(&self.delete_statement)?
                    .execute(params![key])?;
                txn.commit()?;
                if let Some(existing) = existing {
                    Ok(Some(existing.into()))
                } else {
                    Ok(None)
                }
            })
            .map_err(DbError::SqliteFailed)
    }

    /// Reads a value
    fn read(&self, key: &[u8]) -> BoringResult<Option<Bytes>> {
        let read_statement = self.read_statement.clone();

        let result: Option<Vec<u8>> = self.low_level.transaction(move |transaction| {
            let mut statement: rusqlite::CachedStatement =
                transaction.prepare_cached(&read_statement)?;
            statement.query_row(&[key], |r| r.get(0)).optional()
        })?;

        Ok(result.map(|v| v.into()))
    }
}

/// A cloneable on-disk mapping, corresponding to a table in SQLite
#[derive(Clone)]
pub struct Dict {
    inner: Arc<DictInner>,
}

impl Dict {
    /// Gets a key/value pair.
    pub fn get(&self, key: &[u8]) -> BoringResult<Option<Bytes>> {
        self.inner.read(key)
    }

    /// Inserts a key/value pair.
    pub fn insert(
        &self,
        key: impl Into<Bytes>,
        val: impl Into<Bytes>,
    ) -> BoringResult<Option<Bytes>> {
        self.inner.write(key.into(), val.into())
    }

    /// Delete a key.
    pub fn remove(&self, key: &[u8]) -> BoringResult<Option<Bytes>> {
        self.inner.remove(key)
    }

    /// Create a new mapping based on a table name.
    pub(crate) fn new(low_level: Arc<LowLevel>, table_name: &str) -> Self {
        Self {
            inner: Arc::new(DictInner::new(low_level, table_name)),
        }
    }

    /// Flushes to disk. Guarantees that all operations that happens before the call reaches disk before this call terminates.
    ///
    /// **Note**: This can be very slow, especially after a large number of writes. Calling this function is not needed for atomicity or crash-safety, but only when you want to absolutely prevent the database from traveling "back in time" a few seconds in case of a crash. Usually this is only needed if you e.g. want to store a reference to an object in this boringdb database in some other database, and you cannot tolerate "dangling pointers".
    pub fn flush(&self) -> BoringResult<()> {
        Ok(())
    }
}
