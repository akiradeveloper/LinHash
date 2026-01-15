#[test]
fn example() {
    use linhash::LinHash;

    let dir = tempfile::tempdir().unwrap();
    let ksize = 2;
    let vsize = 4;
    let db = LinHash::open(dir.path(), ksize, vsize).unwrap();

    db.insert(vec![1, 2], vec![3, 4, 5, 6]).unwrap();
    let old = db.insert(vec![1, 2], vec![7, 8, 9, 10]).unwrap();
    assert_eq!(old, Some(vec![3, 4, 5, 6]));

    assert_eq!(db.get(&vec![1, 2]).unwrap(), Some(vec![7, 8, 9, 10]));

    let old = db.delete(&vec![1, 2]).unwrap();
    assert_eq!(old, Some(vec![7, 8, 9, 10]));
    assert_eq!(db.get(&vec![1, 2]).unwrap(), None);
}
