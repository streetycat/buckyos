use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum LogType {
    Connect = 1,
    Time = 2,
    Nat = 3,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum LogLever {
    Trace = 1,
    Debug = 2,
    Info = 3,
    Warn = 4,
    Error = 5,
    Fault = 6,
}

#[derive(Serialize, Deserialize)]
pub struct SystemLogRecord {
    pub seq: u64,
    pub log_type: LogType,
    pub timestamp: SystemTime,
    pub node: String,
    pub level: LogLever,
    pub content: String,
}

#[derive(Serialize)]
pub struct SystemLogRecordRef<'a> {
    pub seq: u64,
    pub log_type: LogType,
    pub timestamp: SystemTime,
    pub node: &'a String,
    pub level: LogLever,
    pub content: &'a String,
}

impl<'a> From<&'a SystemLogRecord> for SystemLogRecordRef<'a> {
    fn from(record: &'a SystemLogRecord) -> Self {
        Self {
            seq: record.seq,
            log_type: record.log_type,
            timestamp: record.timestamp,
            node: &record.node,
            level: record.level,
            content: &record.content,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LogState {
    pub next_seq: u64,
}
