use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::types::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct SystemLogWriteRequest {
    pub log_type: LogType,
    pub timestamp: SystemTime,
    pub node: String,
    pub level: LogLever,
    pub content: String,
}

#[derive(Clone)]
pub enum HttpCommand {
    WriteLog(SystemLogWriteRequest),
}
