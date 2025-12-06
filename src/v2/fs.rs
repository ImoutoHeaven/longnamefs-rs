#![allow(dead_code)]

use crate::config::Config;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct LongNameFsV2 {
    pub config: Arc<Config>,
    pub max_name_len: usize,
    pub dir_cache_ttl: Option<Duration>,
}

impl LongNameFsV2 {
    pub fn new(config: Config, max_name_len: usize, dir_cache_ttl: Option<Duration>) -> Self {
        Self {
            config: Arc::new(config),
            max_name_len,
            dir_cache_ttl,
        }
    }
}
