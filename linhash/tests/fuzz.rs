use linhash::*;
use map_test_generator::*;

#[test]
fn reference_test() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

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
        },
    );

    for _ in 0..10000 {
        let op = g.next();
        match op {
            Op::Get(k) => {
                let v1 = fh.get(&k).unwrap();
                let v2 = m.get(&k).cloned();
                assert_eq!(v1, v2);
            }
            Op::Insert(k, v) => {
                let v1 = fh.insert(k.clone(), v.clone()).unwrap();
                let v2 = m.insert(k.clone(), v.clone());
                assert_eq!(v1, v2);
            }
            Op::Delete(k) => {
                let v1 = fh.delete(&k).unwrap();
                let v2 = m.remove(&k);
                assert_eq!(v1, v2);
            }
        }
    }
}
