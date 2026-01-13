use std::collections::HashMap;

enum OpChoice {
    GetMiss,
    GetHit,
    InsertNew,
    Update,
    DeleteHit,
    DeleteMiss,
}

pub struct OpRatio {
    pub get_miss: u32,
    pub get_hit: u32,
    pub insert_new: u32,
    pub update: u32,
    pub delete_miss: u32,
    pub delete_hit: u32,
}

struct OpChoiceGenerator {
    total: u32,
    top_get_miss: u32,
    top_get_hit: u32,
    top_insert_new: u32,
    top_update: u32,
    top_delete_miss: u32,
    top_delete_hit: u32,
}

impl OpChoiceGenerator {
    pub fn new(ratio: OpRatio) -> Self {
        let total = ratio.get_miss
            + ratio.get_hit
            + ratio.insert_new
            + ratio.update
            + ratio.delete_miss
            + ratio.delete_hit;

        let top_get_miss = ratio.get_miss;
        let top_get_hit = top_get_miss + ratio.get_hit;
        let top_insert_new = top_get_hit + ratio.insert_new;
        let top_update = top_insert_new + ratio.update;
        let top_delete_miss = top_update + ratio.delete_miss;
        let top_delete_hit = top_delete_miss + ratio.delete_hit;

        Self {
            total,
            top_get_miss,
            top_get_hit,
            top_insert_new,
            top_update,
            top_delete_miss,
            top_delete_hit,
        }
    }

    fn choose(&mut self) -> OpChoice {
        let r = rand::random::<u32>() % self.total;

        if r < self.top_get_miss {
            return OpChoice::GetMiss;
        }

        if r < self.top_get_hit {
            return OpChoice::GetHit;
        }

        if r < self.top_insert_new {
            return OpChoice::InsertNew;
        }

        if r < self.top_update {
            return OpChoice::Update;
        }

        if r < self.top_delete_miss {
            return OpChoice::DeleteMiss;
        }

        if r < self.top_delete_hit {
            return OpChoice::DeleteHit;
        }

        OpChoice::DeleteMiss
    }
}

#[derive(Debug)]
pub enum Op {
    Get(Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

pub struct MapTestGenerator {
    ksize: usize,
    vsize: usize,
    op_choice_generator: OpChoiceGenerator,
    m: HashMap<Vec<u8>, Vec<u8>>,
}

impl MapTestGenerator {
    pub fn new(ksize: usize, vsize: usize, op_ratio: OpRatio) -> Self {
        Self {
            ksize,
            vsize,
            op_choice_generator: OpChoiceGenerator::new(op_ratio),
            m: HashMap::new(),
        }
    }

    pub fn next(&mut self) -> Op {
        let choice = self.op_choice_generator.choose();
        let op = self.get_op(choice);
        match &op {
            Op::Insert(k, v) => {
                self.m.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                self.m.remove(k);
            }
            Op::Get(_) => {}
        }
        op
    }

    fn k(&self) -> Vec<u8> {
        random(self.ksize)
    }

    fn v(&self) -> Vec<u8> {
        random(self.vsize)
    }

    fn gen_miss_k(&self) -> Vec<u8> {
        let k = loop {
            let k = self.k();
            if !self.m.contains_key(&k) {
                break k;
            }
        };
        k
    }

    fn gen_hit_k(&self) -> Option<Vec<u8>> {
        if self.m.is_empty() {
            return None;
        }

        let keys: Vec<&Vec<u8>> = self.m.keys().collect();
        let idx = rand::random::<usize>() % keys.len();
        Some(keys[idx].clone())
    }

    fn get_op(&self, choice: OpChoice) -> Op {
        use OpChoice::*;

        match choice {
            GetMiss => {
                let k = self.gen_miss_k();
                Op::Get(k)
            }
            GetHit => {
                let k = self.gen_hit_k();
                match k {
                    Some(k) => Op::Get(k),
                    None => return self.get_op(OpChoice::GetMiss),
                }
            }
            InsertNew => {
                let k = self.gen_miss_k();
                let v = self.v();
                Op::Insert(k, v)
            }
            Update => {
                let k = self.gen_hit_k();
                match k {
                    Some(k) => Op::Insert(k, self.v()),
                    None => return self.get_op(OpChoice::InsertNew),
                }
            }
            DeleteMiss => {
                let k = self.gen_miss_k();
                Op::Delete(k)
            }
            DeleteHit => {
                let k = self.gen_hit_k();
                match k {
                    Some(k) => Op::Delete(k),
                    None => return self.get_op(OpChoice::DeleteMiss),
                }
            }
        }
    }
}

fn random(n: usize) -> Vec<u8> {
    (0..n).map(|_| rand::random::<u8>()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loop() {
        let mut g = MapTestGenerator::new(
            32,
            16,
            OpRatio {
                get_miss: 20,
                get_hit: 60,
                insert_new: 15,
                update: 10,
                delete_miss: 1,
                delete_hit: 3,
            },
        );
        for _ in 0..100000 {
            let op = g.next();
        }
    }
}
