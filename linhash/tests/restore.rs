use linhash::*;

fn vec(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_restore() {
    for pagesize in [4096, 16384, 65536] {
        do_test_restore(pagesize);
    }
}

fn do_test_restore(pagesize: usize) {
    let dir = tempfile::tempdir().unwrap();
    let config = LinHashConfig::builder()
        .ksize(8)
        .vsize(8)
        .pagesize(pagesize)
        .build();
    let db = LinHash::open(dir.path(), config.clone()).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }
    drop(db);

    let db = LinHash::open(dir.path(), config).unwrap();

    assert_eq!(db.len(), n as u64);

    for i in n..2 * n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..2 * n {
        let v = db.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i));
    }
}
