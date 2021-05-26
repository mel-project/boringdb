use std::{
    collections::HashMap,
    mem::ManuallyDrop,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::Result;
use crate::{low_level::LowLevel, DbError};
use bytes::Bytes;
use flume::{Receiver, Sender};
use nanorand::RNG;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use rusqlite::OptionalExtension;

/// A clonable on-disk mapping, corresponding to a table in SQLite
#[derive(Clone)]
pub struct Dict {
    inner: Arc<DictInner>,
}

impl Dict {
    /// Gets a key/value pair.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.read(key)
    }

    /// Inserts a key/value pair.
    pub fn insert(&self, key: impl Into<Bytes>, val: impl Into<Bytes>) -> Result<()> {
        self.inner.write(key.into(), val.into())
    }

    /// Create a new mapping based on a table name.
    pub(crate) fn new(low_level: Arc<LowLevel>, table_name: &str) -> Self {
        Self {
            inner: Arc::new(DictInner::new(low_level, table_name)),
        }
    }
}

/// A cache entry
#[derive(Debug)]
struct CacheEntry {
    value: Bytes,
    pseudotime: AtomicU64,
}

// Global pseudotime counter
static GLOBAL_PSEUDO_TIME: AtomicU64 = AtomicU64::new(0);

/// A non-clonable on-disk mapping
struct DictInner {
    cache: RwLock<HashMap<Bytes, CacheEntry>>,
    send_change: ManuallyDrop<Sender<SyncInstruction>>,
    thread_handle: Option<JoinHandle<Option<()>>>,
    low_level: Arc<LowLevel>,

    gc_threshold: usize,

    read_statement: String,
}

impl Drop for DictInner {
    fn drop(&mut self) {
        let _ = unsafe { ManuallyDrop::take(&mut self.send_change) };
        self.thread_handle.take().unwrap().join().unwrap();
    }
}

impl DictInner {
    /// Create a new map iner.
    fn new(low_level: Arc<LowLevel>, table_name: &str) -> Self {
        let (send_change, recv_change) = flume::bounded(5000);
        let thread_handle = {
            let table_name = table_name.to_string();
            let write_statement = format!("insert into {}(key, value) values ($1, $2) on conflict(key) do update set value = excluded.value", table_name);
            let low_level = low_level.clone();
            Some(
                std::thread::Builder::new()
                    .name(format!("boringdb-{}", table_name))
                    .spawn(move || sync_to_disk(recv_change, low_level, write_statement))
                    .unwrap(),
            )
        };
        Self {
            cache: Default::default(),
            send_change: ManuallyDrop::new(send_change),
            thread_handle,
            low_level,
            read_statement: format!("select value from {} where key = $1", table_name),
            gc_threshold: 100000,
        }
    }

    /// Maybe garbage collect
    fn maybe_gc(&self) {
        if nanorand::tls_rng().generate_range(0u32, 1000) == 0 {
            let mut cache = self.cache.write();
            if cache.len() > self.gc_threshold {
                log::debug!("garbage collect started!");
                let oldest_allowed = GLOBAL_PSEUDO_TIME
                    .load(Ordering::Relaxed)
                    .saturating_sub((cache.len() * 9 / 10) as u64);
                let old_len = cache.len();
                cache.retain(|_, v| v.pseudotime.load(Ordering::Relaxed) >= oldest_allowed);
                log::debug!("GC complete: {} => {}", old_len, cache.len());
            }
        }
    }

    /// Writes a key-value pair.
    fn write(&self, key: Bytes, value: Bytes) -> Result<()> {
        // first populate the cache
        self.maybe_gc();
        let mut cache = self.cache.write();
        cache.insert(
            key.clone(),
            CacheEntry {
                value: value.clone(),
                pseudotime: AtomicU64::new(GLOBAL_PSEUDO_TIME.fetch_add(1, Ordering::Relaxed)),
            },
        );
        log::trace!("inserted {:?} => {:?} into cache", key, value);
        // we now signal the background thread
        self.send_change
            .send(SyncInstruction::Write(key, value))
            .map_err(|_| DbError::WriteThreadFailed)?;
        log::trace!("send_change signalled");
        Ok(())
    }

    /// Reads, using the cache
    fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.maybe_gc();
        let cache = self.cache.upgradable_read();
        // first we check the cache
        if let Some(res) = cache.get(key) {
            res.pseudotime.store(
                GLOBAL_PSEUDO_TIME.fetch_add(1, Ordering::Relaxed),
                Ordering::Relaxed,
            );
            let value = res.value.clone();
            Ok(Some(value))
        } else {
            let value = self.read_uncached(key)?;
            let cache = RwLockUpgradableReadGuard::try_upgrade(cache);
            if let Ok(mut cache) = cache {
                if let Some(value) = value.as_ref() {
                    cache.insert(
                        Bytes::copy_from_slice(key),
                        CacheEntry {
                            value: value.clone(),
                            pseudotime: AtomicU64::new(
                                GLOBAL_PSEUDO_TIME.fetch_add(1, Ordering::Relaxed),
                            ),
                        },
                    );
                }
            }
            Ok(value)
        }
    }

    /// Reads, bypassing the cache
    fn read_uncached(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_statement = self.read_statement.clone();
        let result: Option<Vec<u8>> = self.low_level.transaction(move |txn| {
            let mut stmt = txn.prepare_cached(&read_statement)?;
            stmt.query_row(&[key], |r| r.get(0)).optional()
        })?;
        Ok(result.map(|v| v.into()))
    }
}

/// A syncer instruction
#[derive(Debug)]
enum SyncInstruction {
    Flush(Sender<()>),
    WriteBatch(Vec<(Bytes, Bytes)>),
    Write(Bytes, Bytes),
    Delete(Bytes),
}

fn sync_to_disk(
    recv_change: Receiver<SyncInstruction>,
    low_level: Arc<LowLevel>,
    write_statement: String,
) -> Option<()> {
    // To prevent excessively bursty backpressure, we limit the amount of time spent within a transaction.
    // This is done through a TCP-like AIMD approach where we adjust the maximum batch size.
    const MAX_TX_TIME: Duration = Duration::from_millis(200);
    let mut max_batch_size = 100;

    log::debug!("sync_to_disk started");
    let mut instructions = Vec::new();
    for batch_no in 0u64.. {
        instructions.push(recv_change.recv().ok()?);
        while let Ok(instr) = recv_change.try_recv() {
            if instructions.len() >= max_batch_size {
                break;
            }
            instructions.push(instr);
        }
        log::debug!(
            "[{}] sync_to_disk got {}/{} instructions",
            batch_no,
            instructions.len(),
            max_batch_size,
        );
        let tx_start_time = Instant::now();
        low_level
            .transaction(|txn| {
                let mut write_statement = txn.prepare_cached(&write_statement)?;
                for instruction in instructions.drain(0..) {
                    match instruction {
                        SyncInstruction::Write(key, value) => {
                            write_statement.execute(&[&key[..], &value[..]])?;
                        }
                        _ => todo!(),
                    }
                }
                drop(write_statement);
                txn.commit()?;
                Ok(())
            })
            .unwrap();
        let elapsed = tx_start_time.elapsed();
        log::debug!(
            "[{}] sync_to_disk finished in {:?}",
            batch_no,
            tx_start_time.elapsed()
        );
        if elapsed > MAX_TX_TIME {
            max_batch_size = max_batch_size * 8 / 10;
        }
        max_batch_size += 10;
        // std::thread::sleep(Duration::from_millis(500));
    }
    unreachable!()
}
