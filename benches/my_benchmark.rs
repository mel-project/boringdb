use std::ops::Deref;

use boringdb::{Database, Dict};
use criterion::{criterion_group, criterion_main, Criterion};
use once_cell::sync::Lazy;

static TEST_DB: Lazy<Dict> = Lazy::new(|| {
    let db = Database::open("/home/miyuruasuka/labooyah.db").unwrap();
    db.open_dict("labooyah").unwrap()
});

static TEST_SLED_DB: Lazy<sled::Db> =
    Lazy::new(|| sled::open("/home/miyuruasuka/labooyah-sled.db").unwrap());

trait TestDb {
    fn insert_pair(&self, key: &[u8], value: &[u8]);
}

impl TestDb for sled::Db {
    fn insert_pair(&self, key: &[u8], value: &[u8]) {
        self.insert(key, value).unwrap();
    }
}

impl TestDb for Dict {
    fn insert_pair(&self, key: &[u8], value: &[u8]) {
        self.insert(key.to_vec(), value.to_vec()).unwrap();
    }
}

fn small_key_write(b: &mut criterion::Bencher, db: &impl TestDb) {
    b.iter(move || {
        let key = format!("hello world {}", fastrand::u64(0..=u64::MAX));
        db.insert_pair(key.as_bytes(), b"oh no".as_ref());
        // log::info!("inserted {}", i);
        // log::info!("got {:?}", dict.get(key.as_bytes()).unwrap().unwrap());
        // assert_eq!(db.get(key.as_bytes()).unwrap().unwrap().as_ref(), b"oh no");
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("small_key_write", |b| small_key_write(b, TEST_DB.deref()));
    c.bench_function("small_key_write_sled", |b| {
        small_key_write(b, TEST_SLED_DB.deref())
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
