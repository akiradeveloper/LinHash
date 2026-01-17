use std::collections::HashMap;

#[derive(Debug)]
enum OpChoice {
    GetMiss,
    GetHit,
    InsertMiss,
    InsertHit,
    DeleteHit,
    DeleteMiss,
    Len,
    List,
}

pub struct OpRatio {
    pub get_miss: u32,
    pub get_hit: u32,
    pub insert_miss: u32,
    pub insert_hit: u32,
    pub delete_miss: u32,
    pub delete_hit: u32,
    pub len: u32,
    pub list: u32,
}

struct OpChoiceGenerator {
    total: u32,
    top_get_miss: u32,
    top_get_hit: u32,
    top_insert_miss: u32,
    top_insert_hit: u32,
    top_delete_miss: u32,
    top_delete_hit: u32,
    top_len: u32,
    top_list: u32,
}

impl OpChoiceGenerator {
    pub fn new(ratio: OpRatio) -> Self {
        let total = ratio.get_miss
            + ratio.get_hit
            + ratio.insert_miss
            + ratio.insert_hit
            + ratio.delete_miss
            + ratio.delete_hit
            + ratio.len
            + ratio.list;

        let top_get_miss = ratio.get_miss;
        let top_get_hit = top_get_miss + ratio.get_hit;
        let top_insert_miss = top_get_hit + ratio.insert_miss;
        let top_insert_hit = top_insert_miss + ratio.insert_hit;
        let top_delete_miss = top_insert_hit + ratio.delete_miss;
        let top_delete_hit = top_delete_miss + ratio.delete_hit;
        let top_len = top_delete_hit + ratio.len;
        let top_list = top_len + ratio.list;

        Self {
            total,
            top_get_miss,
            top_get_hit,
            top_insert_miss,
            top_insert_hit,
            top_delete_miss,
            top_delete_hit,
            top_len,
            top_list,
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

        if r < self.top_insert_miss {
            return OpChoice::InsertMiss;
        }

        if r < self.top_insert_hit {
            return OpChoice::InsertHit;
        }

        if r < self.top_delete_miss {
            return OpChoice::DeleteMiss;
        }

        if r < self.top_delete_hit {
            return OpChoice::DeleteHit;
        }

        if r < self.top_len {
            return OpChoice::Len;
        }

        OpChoice::List
    }
}

#[derive(Debug)]
pub enum Op {
    Get(Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Len,
    List,
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
            Op::Len => {}
            Op::List => {}
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
            InsertMiss => {
                let k = self.gen_miss_k();
                let v = self.v();
                Op::Insert(k, v)
            }
            InsertHit => {
                let k = self.gen_hit_k();
                match k {
                    Some(k) => Op::Insert(k, self.v()),
                    None => return self.get_op(OpChoice::InsertMiss),
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
            Len => Op::Len,
            List => Op::List,
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
                insert_miss: 15,
                insert_hit: 10,
                delete_miss: 1,
                delete_hit: 3,
                len: 5,
                list: 1,
            },
        );
        for _ in 0..10000 {
            let _op = g.next();
        }
    }
}
