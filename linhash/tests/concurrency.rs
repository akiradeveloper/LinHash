use linhash::*;
use std::sync::Arc;
use std::time::Duration;

fn vec(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn test_parallel_insert_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(LinHash::open(dir.path(), 8, 8).unwrap());

    let mut handles = vec![];
    for i in 0..5000 {
        let hdl = std::thread::spawn({
            let db = db.clone();
            move || {
                db.insert(vec(i), vec(i)).unwrap();

                let random_time = rand::random::<u64>() % 1000;
                std::thread::sleep(Duration::from_millis(random_time));

                let v = db.get(&vec(i)).unwrap().unwrap();
                assert_eq!(v, vec(i));
            }
        });
        handles.push(hdl);
    }

    for hdl in handles {
        hdl.join().unwrap();
    }
}

#[test]
fn test_parallel_insert_delete_get() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(LinHash::open(dir.path(), 8, 8).unwrap());

    let mut handles = vec![];
    for i in 0..5000 {
        let hdl = std::thread::spawn({
            let db = db.clone();
            move || {
                db.insert(vec(i), vec(i)).unwrap();

                let random_time = rand::random::<u64>() % 1000;
                std::thread::sleep(Duration::from_millis(random_time));

                db.delete(&vec(i)).unwrap();

                let random_time = rand::random::<u64>() % 1000;
                std::thread::sleep(Duration::from_millis(random_time));

                let v = db.get(&vec(i)).unwrap();
                assert!(v.is_none());
            }
        });
        handles.push(hdl);
    }

    for hdl in handles {
        hdl.join().unwrap();
    }
}
