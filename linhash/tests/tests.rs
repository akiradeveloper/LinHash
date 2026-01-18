use linhash::*;

fn vec(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_open() {
    let dir = tempfile::tempdir().unwrap();
    let _db = LinHash::open(dir.path(), 1, 1).unwrap();
}

#[test]
fn test_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    assert_eq!(db.len(), n as u64);
}

#[test]
fn test_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    assert_eq!(db.len(), n as u64);

    for i in 0..n {
        let v = db.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i));
    }
}

#[test]
fn test_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    let mut expected = vec![];

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
        expected.push((vec(i), vec(i)));
    }

    let mut actual = vec![];
    for (k, v) in db.list() {
        actual.push((k, v));
    }

    expected.sort();
    actual.sort();
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual, expected);
}

#[test]
fn test_list_after_every_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 2000;

    let mut expected = vec![];

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();

        expected.push((vec(i), vec(i)));

        let mut actual = vec![];
        for (k, v) in db.list() {
            actual.push((k, v));
        }

        expected.sort();
        actual.sort();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_list_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    let mut expected = vec![];

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..n / 2 {
        let removed = db.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    for i in n / 2..n {
        expected.push((vec(i), vec(i)));
    }

    let mut actual = vec![];
    for (k, v) in db.list() {
        actual.push((k, v));
    }

    expected.sort();
    actual.sort();
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual, expected);
}

#[test]
fn test_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        assert_eq!(db.insert(vec(i), vec(i)).unwrap(), None);
    }

    for i in 0..n {
        assert_eq!(db.insert(vec(i), vec(i + 1)).unwrap(), Some(vec(i)));
    }

    assert_eq!(db.len(), n as u64);

    for i in 0..n {
        let v = db.get(&vec(i)).unwrap().unwrap();
        assert_eq!(v, vec(i + 1));
    }
}

#[test]
fn test_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..n {
        let removed = db.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    assert_eq!(db.len(), 0);

    for i in 0..n {
        let v = db.get(&vec(i)).unwrap();
        assert!(v.is_none());
    }
}

#[test]
fn test_insert_half_delete_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..n / 2 {
        let removed = db.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    for i in 0..n / 2 {
        let v = db.get(&vec(i)).unwrap();
        assert!(v.is_none());
    }

    for i in n / 2..n {
        let e = db.get(&vec(i)).unwrap();
        assert_eq!(e, Some(vec(i)));
    }
}

#[test]
fn test_insert_half_delete_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = LinHash::open(dir.path(), 8, 8).unwrap();

    let n = 10000;

    for i in 0..n {
        db.insert(vec(i), vec(i)).unwrap();
    }

    for i in 0..n / 2 {
        let removed = db.delete(&vec(i)).unwrap();
        assert_eq!(removed, Some(vec(i)));
    }

    for i in n / 2..n {
        let old = db.insert(vec(i), vec(i + 1)).unwrap();
        assert_eq!(old, Some(vec(i)));
    }
}
