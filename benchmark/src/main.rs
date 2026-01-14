use clap::Parser;
use linhash::*;
use rand::Rng;
use std::collections::HashSet;

#[derive(Parser, Debug)]
struct CommandArgs {
    #[arg(long, default_value_t = 32)]
    datasize: u32,
    #[arg(long)]
    warmup: u64,
}

fn main() {
    let args = CommandArgs::parse();
    dbg!(&args);

    let dir = tempfile::tempdir().unwrap();
    let mut db = LinHash::open(dir.path(), 32, args.datasize as usize).unwrap();

    let mut keys = HashSet::new();

    let t = std::time::Instant::now();
    for _ in 0..args.warmup {
        let key = random(32); // 256 bits key
        let data = random(args.datasize as usize);
        db.insert(key.clone(), data).unwrap();
        keys.insert(key);
    }
    let elapsed = t.elapsed();
    eprintln!("Write: {:?}/ops", elapsed / args.warmup as u32);

    eprintln!("Warmup done. Starting benchmark...");

    let mut results = vec![];

    let t = std::time::Instant::now();
    while t.elapsed() < std::time::Duration::from_secs(10) {
        let timer = std::time::Instant::now();
        for k in &keys {
            let _ = db.get(k).unwrap();
        }
        let elapsed = timer.elapsed();
        let latency = elapsed / (keys.len() as u32);
        results.push(latency);
    }

    let mut sum = std::time::Duration::ZERO;
    let n = results.len();
    for r in results {
        sum += r;
    }

    eprintln!("Read: {:?}/ops", sum / n as u32);
}

fn random(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random()).collect()
}
