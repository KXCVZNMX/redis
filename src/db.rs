use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

pub type Db = Arc<RwLock<HashMap<String, DBData>>>;

pub enum DBVal {
    String(String),
    Int(i64),
}

pub struct DBData {
    data: DBVal,
    created_at: Instant,
    exp: Option<u64>, // Exp time in millis
}

impl DBData {
    pub fn new(data: DBVal, created_at: Instant, exp: Option<u64>) -> Self {
        Self {
            data, created_at, exp
        }
    }

    pub fn data(&self) -> &DBVal {
        &self.data
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    pub fn exp(&self) -> Option<u64> {
        self.exp
    }
}