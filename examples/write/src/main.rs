use std::borrow::Cow;
use std::ops::Range;
use std::time::Instant;

use boringdb::db::Database;
use boringdb::dict::Dict;
use bytes::Bytes;


fn write_to_database() {
    const VALUE_BYTES: &[u8; 15] = b"value goes here";

    let database: Database = Database::open("/tmp/example_database.db").expect(" Could not open test database.");

    let dict: Dict = database.open_dict("testing").expect("Could not open dictionary.");

    let range: Range<i32> = 0..1000;

    let start_time: Instant = Instant::now();

    range.into_iter().for_each(|index: i32| {
        let key: Vec<u8> = index.to_be_bytes().to_vec();

        dict.insert(key, VALUE_BYTES.as_ref())
            .expect("Could not insert into dictionary.");
    });

    println!("Write operation duration: {:?}", start_time.elapsed());
}

fn main() {
    write_to_database();

    let database: Database = Database::open("/tmp/example_database.db").expect(" Could not open test database.");

    let dict: Dict = database.open_dict("testing").expect("Could not open dictionary.");

    let key_string: String = String::from("3");
    let key: &[u8] = key_string.as_bytes();

    let output_bytes: Bytes = dict.get(key).expect("BoringResult was None.").expect("Bytes was None");

    let value: Cow<str> = String::from_utf8_lossy(&output_bytes);

    println!("{}", value);
}