use linhash::*;

fn vec(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_open() {
    let dir = tempfile::tempdir().unwrap();
    let _fh = LinHash::open(dir.path()).unwrap();
}

#[test]
fn test_insert() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;
    let range = 0..n;

    for i in range.clone() {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    assert_eq!(fh.len(), n as u64);
}

#[test]
fn test_get() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;
    let range = 0..n;

    for i in range.clone() {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    assert_eq!(fh.len(), n as u64);

    for i in range {
        let v = fh.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i));
    }
}

#[test]
fn test_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;
    let range = 0..n;

    for i in range.clone() {
        assert_eq!(fh.insert(vec(i), vec(i)).unwrap(), None);
    }

    for i in range.clone() {
        assert_eq!(fh.insert(vec(i), vec(i + 1)).unwrap(), Some(vec(i)));
    }

    assert_eq!(fh.len(), n as u64);

    for i in range {
        let v = fh.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i + 1));
    }
}

#[test]
fn test_delete() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;
    let range = 0..n;

    for i in range.clone() {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    for i in range.clone() {
        let removed = fh.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    assert_eq!(fh.len(), 0);

    for i in range {
        let v = fh.get(&vec(i)).unwrap();
        assert!(v.is_none());
    }
}

#[test]
fn test_insert_half_delete_get() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;

    for i in 0..n {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..n / 2 {
        let removed = fh.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    for i in 0..n / 2 {
        let v = fh.get(&vec(i)).unwrap();
        assert!(v.is_none());
    }

    for i in n / 2..n {
        let e = fh.get(&vec(i)).unwrap();
        assert_eq!(e, Some(vec(i)));
    }
}

#[test]
fn test_insert_half_delete_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;

    for i in 0..n {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..n / 2 {
        let removed = fh.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    for i in n / 2..n {
        let old = fh.insert(vec(i), vec(i + 1)).unwrap();
        assert_eq!(old, Some(vec(i)));
    }
}

#[test]
fn test_restore() {
    let dir = tempfile::tempdir().unwrap();
    let mut fh = LinHash::open(dir.path()).unwrap();

    let n = 10000;
    let range = 0..n;

    for i in range.clone() {
        fh.insert(vec(i), vec(i)).unwrap();
    }

    let fh = LinHash::open(dir.path()).unwrap();

    assert_eq!(fh.len(), n as u64);

    for i in range {
        let v = fh.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i));
    }
}
