use linhash::*;

fn vec(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;
    let range = 0..n;

    for i in range.clone() {
        db.insert(vec(i), vec(i)).unwrap();
    }

    let fh = LinHash::open(dir.path(), 8, 8).unwrap();

    assert_eq!(fh.len(), n as u64);

    for i in range {
        let v = fh.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i));
    }
}
