use async_std::{
    io::{prelude::SeekExt, ReadExt, WriteExt},
    sync::Arc,
};
use chrono::Datelike;
use regex::Regex;
use std::{io::SeekFrom, path::Path, time::SystemTime};
use walkdir::WalkDir;

use crate::{
    inner_types::SystemLogWriteRequest,
    types::{LogState, SystemLogRecordRef},
};

struct LastLogFile {
    last_log_date: chrono::DateTime<chrono::Utc>,
    writer: async_std::fs::File,
    file_path: String,
}

struct LogFile {
    file: Option<LastLogFile>,
    log_dir: Arc<String>,
}

impl LogFile {
    fn new(log_dir: Arc<String>) -> Self {
        Self {
            file: None,
            log_dir,
        }
    }

    async fn append<'a>(&mut self, log: &SystemLogRecordRef<'a>) {
        let file = match self.check_change_file(log.timestamp).await {
            Some(file) => file,
            None => return,
        };

        if let Err(err) = file.writer.seek(SeekFrom::End(0)).await {
            log::error!(
                "open log file failed with path({}): {}.",
                file.file_path,
                err
            );
            return;
        }

        let mut buf = match (serde_json::to_vec(log)) {
            Ok(buf) => buf,
            Err(err) => {
                log::error!(
                    "encode log file failed with path({}): {}.",
                    file.file_path,
                    err
                );
                return;
            }
        };

        buf.extend_from_slice(b"\n");
        if let Err(err) = file.writer.write(buf.as_slice()).await {
            log::error!(
                "write log file failed with path({}): {}.",
                file.file_path,
                err
            );
        }
    }
    async fn flush(&mut self) {
        if let Some(file) = self.file.as_mut() {
            if let Err(err) = file.writer.flush().await {
                log::error!(
                    "flush log file failed with path({}): {}.",
                    file.file_path,
                    err
                );
            }
        }
    }
    async fn check_change_file<'a>(
        &'a mut self,
        timestamp: SystemTime,
    ) -> Option<&'a mut LastLogFile> {
        let timestamp: chrono::DateTime<chrono::Utc> = timestamp.into();

        let changed = match &mut self.file {
            Some(file) => {
                timestamp.year() != file.last_log_date.year()
                    || timestamp.month() != file.last_log_date.month()
                    || timestamp.day() != file.last_log_date.day()
            }
            None => true,
        };

        if changed {
            let file_path = self.make_log_file_path(&timestamp);
            let writer = match async_std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .open(file_path.as_str())
                .await
            {
                Ok(file) => file,
                Err(err) => {
                    log::error!("open log file failed with path({}): {}.", file_path, err);
                    return None;
                }
            };

            self.file = Some(LastLogFile {
                last_log_date: timestamp,
                writer: writer,
                file_path,
            });
        }

        self.file.as_mut()
    }

    fn make_log_file_path(&self, timestamp: &chrono::DateTime<chrono::Utc>) -> String {
        format!(
            "{}/{}",
            self.log_dir,
            timestamp.format(".system.%Y%m%d.log")
        )
    }

    async fn max_seq(log_file_path: &Path) -> Option<u64> {
        let mut file = match async_std::fs::OpenOptions::new()
            .read(true)
            .open(log_file_path)
            .await
        {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Some(0);
                }

                log::error!(
                    "open log file for seq failed with path({}): {}.",
                    log_file_path.display(),
                    err
                );
                return None;
            }
        };

        let file_len = match file.metadata().await {
            Ok(meta) => {
                if !meta.is_file() {
                    log::error!(
                        "open log file for seq failed with path({}): not file.",
                        log_file_path.display()
                    );
                    return None;
                }
                let file_len = meta.len() as usize;
                if file_len == 0 {
                    return Some(0);
                }
                file_len
            }
            Err(err) => {
                log::error!(
                    "open log file for seq failed with path({}): {}.",
                    log_file_path.display(),
                    err
                );
                return None;
            }
        };

        const READ_BYTES: usize = 4096;

        let mut buf = Vec::new();
        let mut read_len = 0;

        while read_len < file_len {
            let append_len = std::cmp::min(file_len - read_len, READ_BYTES);
            read_len += append_len;
            if buf.len() < read_len {
                buf.resize(read_len, 0);
                buf.copy_within(..(read_len - append_len), append_len);
            }

            if let Err(err) = file.seek(SeekFrom::End(-(read_len as i64))).await {
                log::error!(
                    "read log file for seq failed when seek to({}) with path({}): {}.",
                    read_len,
                    log_file_path.display(),
                    err
                );
                return None;
            }

            if let Err(err) = file.read_exact(&mut buf.as_mut_slice()[..append_len]).await {
                log::error!(
                    "read log file for seq failed with path({}): {}.",
                    log_file_path.display(),
                    err
                );
                return None;
            }

            let line_end = if buf.last() == Some(&b'\n') {
                buf.len() - 1
            } else {
                buf.len()
            };

            let mut line_start_pos = append_len;
            while let Some(pos) = buf.as_slice()[..line_start_pos]
                .iter()
                .rposition(|x| *x == b'\n')
            {
                match serde_json::from_slice::<LogState>(&buf.as_slice()[pos + 1..line_end]) {
                    Ok(state) => return Some(state.next_seq),
                    Err(err) => {
                        log::error!(
                            "parse log file for seq failed with path({}): {}.",
                            log_file_path.display(),
                            err
                        );
                        line_start_pos = pos - 1;
                    }
                }
            }
        }

        None
    }
}

struct LogStateFile {
    state: LogState,
    state_file: async_std::fs::File,
    log_dir: Arc<String>,
}

impl LogStateFile {
    async fn open(log_dir: Arc<String>) -> Option<Self> {
        let mut state_file = match async_std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(Self::state_file_path_from_root(log_dir.as_str()))
            .await
        {
            Ok(file) => file,
            Err(err) => {
                log::error!(
                    "open log state file failed with path({}): {}.",
                    log_dir,
                    err
                );
                return None;
            }
        };

        let mut len_buf = [0u8; 4];
        if let Err(err) = state_file.read_exact(&mut len_buf).await {
            log::error!(
                "read content-length from log state file failed with path({}): {}.",
                log_dir,
                err
            );
            return None;
        };

        let size = u32::from_be_bytes(len_buf) as usize;

        let mut buf = Vec::with_capacity(size);
        unsafe {
            buf.set_len(size);
        }
        if let Err(err) = state_file.read_exact(buf.as_mut_slice()).await {
            log::error!(
                "read log state file failed with path({}): {}.",
                log_dir,
                err
            );
            return None;
        };

        let state: LogState = match serde_json::from_slice(buf.as_slice()) {
            Ok(state) => state,
            Err(err) => {
                log::error!(
                    "parse log state file failed with path({}): {}.",
                    log_dir,
                    err
                );
                return None;
            }
        };

        Some(Self {
            state,
            state_file,
            log_dir,
        })
    }

    async fn create_with_init(log_dir: Arc<String>, state: &LogState) -> Option<Self> {
        let state_file = match async_std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(Self::state_file_path_from_root(log_dir.as_str()))
            .await
        {
            Ok(file) => file,
            Err(err) => {
                log::error!(
                    "open log state file failed with path({}): {}.",
                    log_dir,
                    err
                );
                return None;
            }
        };

        let mut obj = Self {
            state: state.clone(),
            state_file,
            log_dir,
        };

        if obj.update(Some(state)).await {
            Some(obj)
        } else {
            None
        }
    }

    async fn seq_inc(&mut self) -> Option<u64> {
        let seq = self.state.next_seq;
        self.state.next_seq += 1;

        if !self.update(None).await {
            None
        } else {
            Some(seq)
        }
    }

    async fn update(&mut self, state: Option<&LogState>) -> bool {
        let state = state.unwrap_or(&self.state);
        let mut state_buf = match serde_json::to_vec(&state) {
            Ok(buf) => buf,
            Err(err) => {
                log::error!(
                    "encode log state file failed with path({}): {}.",
                    self.log_dir,
                    err
                );
                return false;
            }
        };

        let size = state_buf.len();
        state_buf.extend_from_slice(&[0, 0, 0, 0]);
        state_buf.copy_within(..size, 4);
        state_buf.as_mut_slice()[..4].copy_from_slice(&(size as u32).to_be_bytes());

        if let Err(err) = self.state_file.seek(SeekFrom::Start(0)).await {
            log::error!(
                "write log state file failed when seek with path({}): {}.",
                self.log_dir,
                err
            );
            return false;
        }

        if let Err(err) = self.state_file.write(state_buf.as_slice()).await {
            log::error!(
                "write log state file failed when write with path({}): {}.",
                self.log_dir,
                err
            );
            return false;
        }

        if let Err(err) = self.state_file.flush().await {
            log::error!(
                "write log state file failed when flush with path({}): {}.",
                self.log_dir,
                err
            );
            false
        } else {
            true
        }
    }

    fn state_file_path_from_root(log_dir: &str) -> String {
        format!("{}/{}", log_dir, ".state")
    }
}

pub struct LogFileMgr {
    log_file: LogFile,

    state_file: LogStateFile,
}

impl LogFileMgr {
    pub async fn init(log_dir: &str) -> Option<LogFileMgr> {
        let log_dir = Arc::new(log_dir.to_string());
        let state_file = LogStateFile::open(log_dir.clone()).await;

        let state_file = match state_file {
            Some(state_file) => state_file,
            None => {
                if let Some(state) = Self::recover_state(log_dir.as_str()).await {
                    if let Some(state_file) =
                        LogStateFile::create_with_init(log_dir.clone(), &state).await
                    {
                        state_file
                    } else {
                        return None;
                    }
                } else {
                    return None;
                }
            }
        };

        Some(Self {
            log_file: LogFile::new(log_dir.clone()),
            state_file,
        })
    }

    pub async fn append(&mut self, log: &SystemLogWriteRequest) {
        if let Some(seq) = self.state_file.seq_inc().await {
            let record = SystemLogRecordRef {
                seq,
                log_type: log.log_type,
                timestamp: log.timestamp,
                node: &log.node,
                level: log.level,
                content: &log.content,
            };
            self.log_file.append(&record).await
        }
    }

    pub async fn flush(&mut self) {
        self.log_file.flush().await
    }

    async fn recover_state(log_dir: &str) -> Option<LogState> {
        let mut log_file_paths = Vec::new();

        let re = Regex::new(r"\.system\.(\d{8})\.log$").unwrap();

        // 遍历目录
        for entry in WalkDir::new(log_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
        {
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_str().unwrap();

            if re.is_match(file_name) {
                log_file_paths.push(path.to_owned());
            }
        }

        let seqs =
            futures::future::join_all(log_file_paths.iter().map(|path| LogFile::max_seq(path)))
                .await;

        let mut max_seq = 0;
        for seq in seqs {
            match seq {
                Some(seq) => max_seq = std::cmp::max(seq, max_seq),
                None => return None,
            }
        }

        Some(LogState {
            next_seq: max_seq + 1,
        })
    }
}
