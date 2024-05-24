use std::path::Path;
use backup_lib::{CheckPointVersion, ChunkServerType, ChunkStorage, FileInfo, FileServerType, FileStorageQuerier, TaskId, TaskInfo, TaskKey, TaskStorageDelete, TaskStorageInStrategy, Transaction};

#[async_trait::async_trait]
pub trait TaskStorageClient: TaskStorageInStrategy + TaskStorageDelete + Transaction {
    async fn create_task(
        &self,
        task_key: &TaskKey,
        check_point_version: CheckPointVersion,
        prev_check_point_version: Option<CheckPointVersion>,
        meta: Option<&str>,
        dir_path: &Path,
        priority: u32,
        is_manual: bool,
    ) -> Result<TaskId, Box<dyn std::error::Error + Send + Sync>>;

    async fn add_file(
        &self,
        task_id: TaskId,
        file_path: &Path,
        hash: &str,
        file_size: u64,
        file_seq: u32
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn create_task_with_files(
        &self,
        task_key: &TaskKey,
        check_point_version: CheckPointVersion,
        prev_check_point_version: Option<CheckPointVersion>,
        meta: Option<&str>,
        dir_path: &Path,
        files: &[FileInfo],
        priority: u32,
        is_manual: bool,
    ) -> Result<TaskId, Box<dyn std::error::Error + Send + Sync>> {
        self.begin_transaction().await?;

        let task_id = self
            .create_task(
                &task_key,
                check_point_version,
                prev_check_point_version,
                meta,
                dir_path,
                priority,
                is_manual,
            )
            .await?;

        for (seq, file_info) in files.iter().enumerate() {
            self.add_file(
                task_id,
                file_info.file_path.as_path(),
                file_info.hash.as_str(),
                file_info.file_size,
                seq as u32,
            )
            .await?;
        }

        self.commit_transaction().await?;

        Ok(task_id)
    }

    async fn get_incomplete_tasks(
        &self,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TaskInfo>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_incomplete_files(
        &self,
        task_key: &TaskKey,
        version: CheckPointVersion,
        min_file_seq: usize,
        limit: usize,
    ) -> Result<Vec<FileInfo>, Box<dyn std::error::Error + Send + Sync>>;

    async fn is_task_info_pushed(
        &self,
        task_key: &TaskKey,
        check_point_version: CheckPointVersion,
    ) -> Result<Option<TaskId>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set_task_info_pushed(
        &self,
        task_key: &TaskKey,
        check_point_version: CheckPointVersion,
        task_id: TaskId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // Ok(file-server-name)
    async fn is_file_info_pushed(
        &self,
        task_key: &TaskKey,
        check_point_version: CheckPointVersion,
        file_path: &Path,
    ) -> Result<Option<(FileServerType, String, u32)>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set_file_info_pushed(
        &self,
        task_key: &TaskKey,
        check_point_version: CheckPointVersion,
        file_path: &Path,
        server_type: FileServerType,
        server_name: &str,
        chunk_size: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait::async_trait]
pub trait FileStorageClient: FileStorageQuerier {
    // Ok((chunk-server-type, chunk-server-name, chunk-hash))
    async fn is_chunk_info_pushed(
        &self,

        task_key: &TaskKey,
        version: CheckPointVersion,
        file_path: &Path,
        chunk_seq: u64,
    ) -> Result<Option<(ChunkServerType, String, String)>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set_chunk_info_pushed(
        &self,

        task_key: &TaskKey,
        version: CheckPointVersion,
        file_path: &Path,
        chunk_seq: u64,
        chunk_server_type: ChunkServerType,
        server_name: &str,
        chunk_hash: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait::async_trait]
pub trait ChunkStorageClient: ChunkStorage {
    // Ok(is_uploaded)
    async fn is_chunk_uploaded(
        &self,
        task_key: &TaskKey,
        version: CheckPointVersion,
        file_path: &Path,
        chunk_seq: u64,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;
    async fn set_chunk_uploaded(
        &self,

        task_key: &TaskKey,
        version: CheckPointVersion,
        file_path: &Path,
        chunk_seq: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
