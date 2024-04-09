use std::time::SystemTime;

use crate::{inner_types::SystemLogWriteRequest, LogLever, LogType};

pub struct SystemLog {
    log_type: LogType,
    url: tide::http::Url,
    node: String,
}

impl SystemLog {
    pub async fn init(log_type: LogType) -> std::option::Option<Self> {
        // TODO: read config from etcd
        let port = 8080;
        let node = "mock node";

        Some(Self {
            log_type,
            url: tide::http::Url::parse(format!("http://127.0.0.1:{}/log", port).as_str()).unwrap(),
            node: node.to_string(),
        })
    }

    pub async fn write(&self, content: &impl ToString, level: LogLever) {
        let client = reqwest::Client::new();
        match client
            .put(self.url.clone())
            .body(
                serde_json::to_string(&SystemLogWriteRequest {
                    log_type: self.log_type,
                    timestamp: SystemTime::now(),
                    node: self.node.clone(),
                    level,
                    content: content.to_string(),
                })
                .unwrap(),
            )
            .header(
                reqwest::header::CONTENT_TYPE,
                reqwest::header::HeaderValue::from_static("application/json"),
            )
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status() != reqwest::StatusCode::OK {
                    log::error!(
                        "send log from http({}) failed: {}.",
                        self.url,
                        resp.status()
                    );
                }
            }
            Err(err) => {
                log::error!("send log from http({}) failed: {}.", self.url, err);
            }
        }
    }

    pub async fn trace(&self, content: &impl ToString) {
        self.write(content, LogLever::Trace).await
    }

    pub async fn debug(&self, content: &impl ToString) {
        self.write(content, LogLever::Debug).await
    }

    pub async fn info(&self, content: &impl ToString) {
        self.write(content, LogLever::Info).await
    }
    pub async fn warn(&self, content: &impl ToString) {
        self.write(content, LogLever::Warn).await
    }
    pub async fn error(&self, content: &impl ToString) {
        self.write(content, LogLever::Error).await
    }
    pub async fn fault(&self, content: &impl ToString) {
        self.write(content, LogLever::Fault).await
    }
}

pub struct SystemLogSync {
    logger: SystemLog,
}

impl SystemLogSync {
    pub fn new(log_type: LogType) -> std::option::Option<Self> {
        async_std::task::block_on(SystemLog::init(log_type)).map(|logger| Self { logger })
    }

    pub fn write(&self, content: &impl ToString, level: LogLever) {
        async_std::task::block_on(self.logger.write(content, level))
    }

    pub fn trace(&self, content: &impl ToString) {
        self.write(content, LogLever::Trace)
    }

    pub fn debug(&self, content: &impl ToString) {
        self.write(content, LogLever::Debug)
    }

    pub fn info(&self, content: &impl ToString) {
        self.write(content, LogLever::Info)
    }
    pub fn warn(&self, content: &impl ToString) {
        self.write(content, LogLever::Warn)
    }
    pub fn error(&self, content: &impl ToString) {
        self.write(content, LogLever::Error)
    }
    pub fn fault(&self, content: &impl ToString) {
        self.write(content, LogLever::Fault)
    }
}
