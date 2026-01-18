use linhash::*;

fn vec(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    let fh = LinHash::open(dir.path(), 8, 8).unwrap();

    assert_eq!(fh.len(), n as u64);

    for i in n..2 * n {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..2 * n {
        let v = fh.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i));
    }
}
