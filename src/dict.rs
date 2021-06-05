#![allow(clippy::mutable_key_type)]
use std::{
    collections::BTreeMap,
    mem::ManuallyDrop,
    ops::{Bound, RangeBounds},
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
use genawaiter::rc::Gen;
use itertools::Itertools;
use nanorand::RNG;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use priority_queue::PriorityQueue;
use rusqlite::{OptionalExtension, Row};

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
    pub fn insert(&self, key: impl Into<Bytes>, val: impl Into<Bytes>) -> Result<Option<Bytes>> {
        self.inner.write(key.into(), val.into())
    }

    /// Delete a key.
    pub fn remove(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.remove(key)
    }

    /// Create a new mapping based on a table name.
    pub(crate) fn new(low_level: Arc<LowLevel>, table_name: &str) -> Self {
        Self {
            inner: Arc::new(DictInner::new(low_level, table_name)),
        }
    }

    /// Iterate through tuples of keys and values, where the keys fall within the specified range.
    ///
    /// **Note**: currently this function returns an iterator that locks the whole dictionary until it is dropped. This will change the future.
    pub fn range<'a, K: AsRef<[u8]> + Ord + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: R,
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes)>> + 'a> {
        let tx = self.transaction()?;
        let gen = Gen::new(move |co| async move {
            let it = tx.range(range);
            match it {
                Ok(it) => {
                    for val in it {
                        co.yield_(val).await;
                    }
                }
                Err(err) => {
                    co.yield_(Err(err)).await;
                }
            }
        });
        Ok(gen.into_iter())
    }

    /// Runs a transaction. This is NOT optimistic, but rather locking, so care should be taken to avoid long-running transactions.
    pub fn transaction(&'_ self) -> Result<Transaction<'_>> {
        let cache = self.inner.cache.write();
        let txn = Transaction {
            send_change: &self.inner.send_change,
            cache,
            read_uncached: Box::new(move |val| self.inner.read_uncached(val)),
            cache_priorities: &self.inner.cache_priorities,
            dinner: &self.inner,
            to_write: Default::default(),
        };
        Ok(txn)
    }

    /// Flushes to disk. Guarantees that all operations that happens before the call reaches disk before this call terminates.
    ///
    /// **Note**: This can be very slow, especially after a large number of writes. Calling this function is not needed for atomicity or crash-safety, but only when you want to absolutely prevent the database from traveling "back in time" a few seconds in case of a crash. Usually this is only needed if you e.g. want to store a reference to an object in this boringdb database in some other database, and you cannot tolerate "dangling pointers".
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
}

/// A transaction handle, which is passed to transaction closures.
pub struct Transaction<'a> {
    send_change: &'a Sender<SyncInstruction>,
    cache: RwLockWriteGuard<'a, BTreeMap<Bytes, CacheEntry>>,
    cache_priorities: &'a Mutex<PriorityQueue<Bytes, u64>>,
    to_write: Vec<(Bytes, Option<Bytes>)>,
    dinner: &'a DictInner,
    read_uncached: Box<dyn Fn(&[u8]) -> Result<Option<Bytes>> + 'a>,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        let _ = throttled_send(
            &self.send_change,
            SyncInstruction::WriteBatch(self.to_write.clone()),
        );
    }
}

impl<'a> Transaction<'a> {
    /// Inserts a key/value pair.
    pub fn insert(&mut self, key: impl Into<Bytes>, val: impl Into<Bytes>) -> Result<()> {
        let key: Bytes = key.into();
        let val: Bytes = val.into();
        self.cache.insert(key.clone(), CacheEntry::new(val.clone()));
        self.cache_priorities
            .lock()
            .push(key.clone(), get_pseudotime());
        self.to_write.push((key, Some(val)));
        Ok(())
    }

    /// Delete a key.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        if let Some(entry) = self.cache.get_mut(key.as_ref()) {
            entry.deleted = true;
            self.to_write
                .push((Bytes::copy_from_slice(key.as_ref()), None));
        }
        Ok(())
    }

    /// Gets a key/value pair.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(res) = self.cache.get(key) {
            if res.deleted {
                return Ok(None);
            }
            let value = res.value.clone();
            Ok(Some(value))
        } else {
            let value = (self.read_uncached)(key)?;
            // if let Some(value) = value.as_ref() {
            //     self.cache
            //         .insert(Bytes::copy_from_slice(key), CacheEntry::new(value.clone()));
            // }
            Ok(value)
        }
    }

    /// Iterate through tuples of keys and values, where the keys fall within the specified range.
    pub fn range<K: AsRef<[u8]> + Ord, R: RangeBounds<K>>(
        &'_ self,
        range: R,
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes)>> + '_> {
        let start_bound = match range.start_bound() {
            Bound::Included(v) => Bound::Included(v.as_ref()),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(v) => Bound::Included(v.as_ref()),
            Bound::Excluded(v) => Bound::Excluded(v.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let range = (start_bound, end_bound);
        let disk_iter = self.dinner.range_keys_uncached(range)?.into_iter();
        let cache_iter = self.cache.range::<[u8], _>(range).map(|v| v.0.clone());
        // are we sure these are sorted?
        let gen = Gen::new(|co| async move {
            for key in itertools::merge(disk_iter, cache_iter).dedup() {
                let value = self.get(&key);

                match value {
                    Err(e) => co.yield_(Err(e)).await,
                    Ok(None) => continue,
                    Ok(Some(val)) => co.yield_(Ok((key, val))).await,
                }
            }
        });
        Ok(gen.into_iter())
    }
}

/// A cache entry
#[derive(Debug)]
struct CacheEntry {
    value: Bytes,
    deleted: bool,
}

impl CacheEntry {
    fn new(value: Bytes) -> Self {
        Self {
            value,
            deleted: false,
        }
    }
}

// Global pseudotime counter
static GLOBAL_PSEUDO_TIME: AtomicU64 = AtomicU64::new(0);

fn get_pseudotime() -> u64 {
    GLOBAL_PSEUDO_TIME.fetch_add(1, Ordering::Relaxed)
}

/// A non-clonable on-disk mapping
struct DictInner {
    cache: RwLock<BTreeMap<Bytes, CacheEntry>>,
    cache_priorities: Mutex<PriorityQueue<Bytes, u64>>,
    send_change: ManuallyDrop<Sender<SyncInstruction>>,
    thread_handle: Option<JoinHandle<Option<()>>>,
    low_level: Arc<LowLevel>,

    gc_threshold: usize,

    read_statement: String,
    table_name: String,
}

impl Drop for DictInner {
    fn drop(&mut self) {
        let _ = unsafe { ManuallyDrop::take(&mut self.send_change) };
        self.thread_handle.take().unwrap().join().unwrap();
    }
}

fn throttled_send<T>(sender: &Sender<T>, val: T) -> std::result::Result<(), flume::SendError<T>> {
    // TODO something better
    // let throttle = Duration::from_secs_f64(-(100.0 / ((sender.len() as f64) - 10000.0)) - 0.01);
    // std::thread::sleep(throttle);
    sender.send(val)
}

impl DictInner {
    /// Create a new map iner.
    fn new(low_level: Arc<LowLevel>, table_name: &str) -> Self {
        let (send_change, recv_change) = flume::bounded(10000);
        let thread_handle = {
            let table_name = table_name.to_string();
            let write_statement = format!("insert into {}(key, value) values ($1, $2) on conflict(key) do update set value = excluded.value", table_name);
            let delete_statement = format!("delete from {} where key = $1", table_name);
            let low_level = low_level.clone();
            Some(
                std::thread::Builder::new()
                    .name(format!("boringdb-{}", table_name))
                    .spawn(move || {
                        sync_to_disk(recv_change, low_level, write_statement, delete_statement)
                    })
                    .unwrap(),
            )
        };
        Self {
            cache: Default::default(),
            cache_priorities: Default::default(),
            send_change: ManuallyDrop::new(send_change),
            thread_handle,
            low_level,
            read_statement: format!("select value from {} where key = $1", table_name),
            gc_threshold: 10000,
            table_name: table_name.to_string(),
        }
    }

    /// Maybe garbage collect
    fn maybe_gc(&self) -> Result<()> {
        if nanorand::tls_rng().generate_range(0u32, 1000) == 0 {
            let cache = self.cache.upgradable_read();
            let old_len = cache.len();
            if cache.len() > self.gc_threshold {
                if let Ok(mut cache) = RwLockUpgradableReadGuard::try_upgrade(cache) {
                    // we flush everything
                    self.flush()?;
                    assert_eq!(0, self.send_change.len());
                    let mut priorities = self.cache_priorities.lock();
                    log::warn!("garbage collect started! {} priorities", priorities.len());
                    while let Some((k, _)) = priorities.pop() {
                        cache.remove(&k);
                        if cache.len() < self.gc_threshold / 2 {
                            break;
                        }
                    }
                    if cache.len() > self.gc_threshold / 2 {
                        log::warn!("** LRU failed to get the cache size under control? Killing everything just because**");
                        cache.clear();
                    }
                    log::warn!("GC complete: {} => {}", old_len, cache.len());
                }
            }
        }
        Ok(())
    }

    /// Flushes to disk
    fn flush(&self) -> Result<()> {
        let (s, r) = flume::bounded(0);
        throttled_send(&self.send_change, SyncInstruction::Flush(s))
            .map_err(|_| DbError::WriteThreadFailed)?;
        r.recv().map_err(|_| DbError::WriteThreadFailed)
    }

    /// Deletes a key.
    fn remove(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let mut cache = self.cache.write();
        let previous = match cache.get_mut(key) {
            None => {
                let val = self.read_uncached(&key)?;
                if let Some(val) = val.as_ref() {
                    let mut entry = CacheEntry::new(val.clone());
                    entry.deleted = true;
                    cache.insert(Bytes::copy_from_slice(key), entry);
                }
                val
            }
            Some(val) => {
                if val.deleted {
                    return Ok(None);
                }
                val.deleted = true;
                Some(val.value.clone())
            }
        };
        // we now signal the background thread
        throttled_send(
            &self.send_change,
            SyncInstruction::Delete(Bytes::copy_from_slice(key)),
        )
        .map_err(|_| DbError::WriteThreadFailed)?;
        drop(cache);
        Ok(previous)
    }

    /// Writes a key-value pair, returning the previous value
    fn write(&self, key: Bytes, value: Bytes) -> Result<Option<Bytes>> {
        // first populate the cache
        self.maybe_gc()?;
        let mut cache = self.cache.write();
        // we want to return the previous value.
        // if there's a previous value in the cache, perfect! that's our previous value
        // otherwise, the previous value is in the disk. the disk value cannot possibly be out of date, because if there are inflight writes, there would be cache.
        let previous = cache
            .insert(key.clone(), CacheEntry::new(value.clone()))
            .map(|v| v.value);
        // we now signal the background thread
        let actual_previous = match previous {
            None => self.read_uncached(&key)?,
            Some(val) => Some(val),
        };

        throttled_send(
            &self.send_change,
            SyncInstruction::Write(key.clone(), value),
        )
        .map_err(|_| DbError::WriteThreadFailed)?;
        drop(cache);

        self.cache_priorities.lock().push(key, get_pseudotime());
        Ok(actual_previous)
    }

    /// Reads, using the cache
    fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.maybe_gc()?;
        let cache = self.cache.upgradable_read();
        // first we check the cache
        if let Some((key, res)) = cache.get_key_value(key) {
            if res.deleted {
                return Ok(None);
            }
            let value = res.value.clone();
            if nanorand::tls_rng().generate_range(0usize, 100) == 0 {
                self.cache_priorities
                    .lock()
                    .push(key.clone(), get_pseudotime());
            }
            Ok(Some(value))
        } else {
            let value = self.read_uncached(key)?;
            // try to upgrade to write lock. if we can't, that's okay too
            let cache = RwLockUpgradableReadGuard::try_upgrade(cache);
            if let Ok(mut cache) = cache {
                let key = Bytes::copy_from_slice(key);
                if let Some(value) = value.as_ref() {
                    cache.insert(key.clone(), CacheEntry::new(value.clone()));
                }
                drop(cache);
                self.cache_priorities.lock().push(key, get_pseudotime());
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

    /// Gets all the keys, from the disk, within a range.
    fn range_keys_uncached<'a>(&self, range: impl RangeBounds<&'a [u8]>) -> Result<Vec<Bytes>> {
        fn tovec(r: &Row) -> std::result::Result<Vec<u8>, rusqlite::Error> {
            r.get::<_, Vec<u8>>(0)
        }
        Ok(self.low_level.transaction(|txn| {
            let res: rusqlite::Result<Vec<Vec<u8>>> = match (range.start_bound(), range.end_bound())
            {
                (Bound::Included(start), Bound::Included(end)) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key >= $1 and key <= $2 ",
                        self.table_name
                    ))?
                    .query_map(&[start, end], tovec)?
                    .collect(),
                (Bound::Included(start), Bound::Excluded(end)) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key >= $1 and key <  $2 ",
                        self.table_name
                    ))?
                    .query_map(&[start, end], tovec)?
                    .collect(),
                (Bound::Included(start), Bound::Unbounded) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key >= $1 ",
                        self.table_name
                    ))?
                    .query_map(&[start], tovec)?
                    .collect(),
                (Bound::Excluded(start), Bound::Included(end)) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key > $1 and key <= $2",
                        self.table_name
                    ))?
                    .query_map(&[start, end], tovec)?
                    .collect(),
                (Bound::Excluded(start), Bound::Excluded(end)) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key > $1 and key < $2",
                        self.table_name
                    ))?
                    .query_map(&[start, end], tovec)?
                    .collect(),
                (Bound::Excluded(start), Bound::Unbounded) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key > $1",
                        self.table_name
                    ))?
                    .query_map(&[start], tovec)?
                    .collect(),
                (Bound::Unbounded, Bound::Included(end)) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key <= $1",
                        self.table_name
                    ))?
                    .query_map(&[end], tovec)?
                    .collect(),
                (Bound::Unbounded, Bound::Excluded(end)) => txn
                    .prepare_cached(&format!(
                        "select key from {} where key < $1",
                        self.table_name
                    ))?
                    .query_map(&[end], tovec)?
                    .collect(),
                (Bound::Unbounded, Bound::Unbounded) => txn
                    .prepare_cached(&format!("select key from {}", self.table_name))?
                    .query_map([], tovec)?
                    .collect(),
            };
            let mut toret: Vec<Bytes> = Vec::new();
            for row in res? {
                toret.push(row.into());
            }
            for pair in toret.windows(2) {
                assert!(pair[0] < pair[1]);
            }
            Ok(toret)
        })?)
    }
}

/// A syncer instruction
#[derive(Debug)]
enum SyncInstruction {
    Flush(Sender<()>),
    WriteBatch(Vec<(Bytes, Option<Bytes>)>),
    Write(Bytes, Bytes),
    Delete(Bytes),
}

fn sync_to_disk(
    recv_change: Receiver<SyncInstruction>,
    low_level: Arc<LowLevel>,
    write_statement: String,
    delete_statement: String,
) -> Option<()> {
    // To prevent excessively bursty backpressure, we limit the amount of time spent within a transaction.
    // This is done through a TCP-like AIMD approach where we adjust the maximum batch size.
    const MAX_TX_TIME: Duration = Duration::from_millis(1000);
    let mut max_batch_size = 100;

    log::debug!("sync_to_disk started");
    let mut instructions = Vec::new();
    for batch_no in 0u64.. {
        instructions.push(recv_change.recv().ok()?);
        while let Ok(instr) = recv_change.try_recv() {
            instructions.push(instr);
            if instructions.len() >= max_batch_size {
                break;
            }
        }
        log::debug!(
            "[{}] sync_to_disk got {}/{} instructions",
            batch_no,
            instructions.len(),
            max_batch_size,
        );

        // continue;

        if instructions.len() >= max_batch_size {
            max_batch_size += (max_batch_size / 10).max(10);
        }
        let mut flush_buff = Vec::new();

        // Writes
        let mut writes: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
        for instruction in instructions.drain(..) {
            match instruction {
                SyncInstruction::Write(key, value) => {
                    writes.insert(key, Some(value));
                }
                SyncInstruction::Delete(key) => {
                    // if writes.remove(&key).is_none() {
                    writes.insert(key, None);
                    // }
                }
                SyncInstruction::Flush(flush) => flush_buff.push(flush),
                SyncInstruction::WriteBatch(kvv) => {
                    for (key, value) in kvv {
                        writes.insert(key, value);
                    }
                }
            }
        }

        let tx_start_time = Instant::now();
        low_level
            .transaction(|txn| {
                {
                    let mut write_statement = txn.prepare_cached(&write_statement)?;
                    let mut delete_statement = txn.prepare_cached(&delete_statement)?;
                    for (key, value) in writes {
                        if let Some(value) = value {
                            write_statement.execute(&[key.as_ref(), value.as_ref()])?;
                        } else {
                            delete_statement.execute(&[key.as_ref()])?;
                        }
                    }
                }
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
        for flush in flush_buff {
            flush.send(()).ok()?;
        }
        // at least sleep 50ms
        std::thread::sleep(Duration::from_millis(50));
    }
    unreachable!()
}
