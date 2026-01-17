use linhash::*;
use map_test_generator::*;

#[test]
fn reference_test() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 32, 16).unwrap();

    let mut m = std::collections::HashMap::new();

    let mut g = MapTestGenerator::new(
        32,
        16,
        OpRatio {
            get_miss: 5,
            get_hit: 60,
            insert_new: 15,
            update: 10,
            delete_miss: 1,
            delete_hit: 3,
            len: 5,
        },
    );

    for _ in 0..50000 {
        let op = g.next();
        match op {
            Op::Get(k) => {
                let v1 = db.get(&k).unwrap();
                let v2 = m.get(&k).cloned();
                assert_eq!(v1, v2);
            }
            Op::Insert(k, v) => {
                let v1 = db.insert(k.clone(), v.clone()).unwrap();
                let v2 = m.insert(k.clone(), v.clone());
                assert_eq!(v1, v2);
            }
            Op::Delete(k) => {
                let v1 = db.delete(&k).unwrap();
                let v2 = m.remove(&k);
                assert_eq!(v1, v2);
            }
            Op::Len => {
                let len1 = db.len();
                let len2 = m.len() as u64;
                assert_eq!(len1, len2);
            }
        }
    }
}
