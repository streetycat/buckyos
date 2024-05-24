use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use backup_lib::{
    CheckPointVersion, CheckPointVersionStrategy, ChunkMgrSelector,
    FileMgrSelector, ListOffset, TaskId, TaskInfo, TaskKey,
    TaskMgrSelector, TaskStorageQuerier,
};
use tokio::sync::Mutex;

use crate::{task::{BackupTask, BackupTaskEvent, RestoreTask, Task, TaskInner}, task_storage::{ChunkStorageClient, FileStorageClient, TaskStorageClient}};

// TODO: config
const MAX_RUNNING_TASK_COUNT: usize = 5;

struct BackupTaskMap {
    task_ids: HashMap<TaskKey, HashMap<CheckPointVersion, TaskId>>, // key -> version -> task_id
    tasks: HashMap<TaskId, BackupTask>,                             // task_id -> task
}

impl BackupTaskMap {
    fn new() -> Self {
        Self {
            task_ids: HashMap::new(),
            tasks: HashMap::new(),
        }
    }

    fn try_run(&mut self, backup_task: BackupTask) {
        if self.tasks.len() < MAX_RUNNING_TASK_COUNT {
            let task_key = backup_task.task_key();
            let task_id = backup_task.task_id();
            let version = backup_task.check_point_version();
            self.tasks.insert(task_id, backup_task.clone());
            self.task_ids
                .entry(task_key)
                .or_insert_with(HashMap::new)
                .insert(version, task_id);

            backup_task.start();
        }
    }

    fn remove_task(&mut self, backup_task: &BackupTask) {
        let task_key = backup_task.task_key();
        let task_id = backup_task.task_id();
        let version = backup_task.check_point_version();
        self.tasks.remove(&task_id);
        self.task_ids
            .entry(task_key)
            .and_modify(|v| {v.remove(&version);});
    }
}

enum BackupState {
    Running(tokio::sync::mpsc::Sender<BackupTaskEvent>),
    Stopping(tokio::sync::mpsc::Sender<()>, tokio::sync::mpsc::Sender<BackupTaskEvent>),
    Stopped,
}

pub(crate) struct BackupTaskMgrInner {
    task_storage: Arc<Box<dyn TaskStorageClient>>,
    file_storage: Arc<Box<dyn FileStorageClient>>,
    chunk_storage: Arc<Box<dyn ChunkStorageClient>>,
    task_mgr_selector: Arc<Box<dyn TaskMgrSelector>>,
    file_mgr_selector: Arc<Box<dyn FileMgrSelector>>,
    chunk_mgr_selector: Arc<Box<dyn ChunkMgrSelector>>,
    running_tasks: Arc<Mutex<BackupTaskMap>>,
    state: Arc<Mutex<BackupState>>,
}

impl BackupTaskMgrInner {
    pub(crate) fn task_storage(&self) -> Arc<Box<dyn TaskStorageClient>> {
        self.task_storage.clone()
    }

    pub(crate) fn file_storage(&self) -> Arc<Box<dyn FileStorageClient>> {
        self.file_storage.clone()
    }

    pub(crate) fn chunk_storage(&self) -> Arc<Box<dyn ChunkStorageClient>> {
        self.chunk_storage.clone()
    }

    pub(crate) fn task_mgr_selector(&self) -> Arc<Box<dyn TaskMgrSelector>> {
        self.task_mgr_selector.clone()
    }

    pub(crate) fn file_mgr_selector(&self) -> Arc<Box<dyn FileMgrSelector>> {
        self.file_mgr_selector.clone()
    }

    pub(crate) fn chunk_mgr_selector(&self) -> Arc<Box<dyn ChunkMgrSelector>> {
        self.chunk_mgr_selector.clone()
    }

    pub(crate) fn task_event_sender(&self) -> Option<tokio::sync::mpsc::Sender<BackupTaskEvent>> {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async {
            let state = self.state.lock().await;
            match &*state {
                BackupState::Running(sender) => Some(sender.clone()),
                BackupState::Stopping(_, sender) => Some(sender.clone()),
                _ => None
            }            
        })
    }

    fn try_run(&self, backup_task: BackupTask) -> usize {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async {
            let mut running_tasks = self.running_tasks.lock().await;
            running_tasks.try_run(backup_task);
            running_tasks.tasks.len()
        })
    }

    fn remove_task(&self, backup_task: &BackupTask) -> usize {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async {
            let mut running_tasks = self.running_tasks.lock().await;
            running_tasks.remove_task(backup_task);
            running_tasks.tasks.len()
        })
    }
}

pub struct BackupTaskMgr(Arc<BackupTaskMgrInner>);

impl BackupTaskMgr {
    pub fn new(
        task_storage: Box<dyn TaskStorageClient>,
        file_storage: Box<dyn FileStorageClient>,
        chunk_storage: Box<dyn ChunkStorageClient>,
        task_mgr_selector: Box<dyn TaskMgrSelector>,
        file_mgr_selector: Box<dyn FileMgrSelector>,
        chunk_mgr_selector: Box<dyn ChunkMgrSelector>,
    ) -> Self {
        // let (task_event_sender, task_event_receiver) = tokio::sync::mpsc::channel(1024);
        let task_mgr = Arc::new(BackupTaskMgrInner {
            task_storage: Arc::new(task_storage),
            file_storage: Arc::new(file_storage),
            chunk_storage: Arc::new(chunk_storage),
            task_mgr_selector: Arc::new(task_mgr_selector),
            file_mgr_selector: Arc::new(file_mgr_selector),
            chunk_mgr_selector: Arc::new(chunk_mgr_selector),
            running_tasks: Arc::new(Mutex::new(BackupTaskMap::new())),
            state: Arc::new(Mutex::new(BackupState::Stopped)),
        });

        Self(task_mgr)
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (task_event_sender, mut task_event_receiver) = tokio::sync::mpsc::channel(1024);

        {
            let mut state = self.0.state.lock().await;
            match &*state {
                BackupState::Running(_) => return Err("BackupTaskMgr is already started".into()),
                BackupState::Stopping(_, _) => {
                    return Err("BackupTaskMgr is stopping, you should wait it finish.".into())
                }
                BackupState::Stopped => *state = BackupState::Running(task_event_sender),
            }            
        }

        let task_mgr = self.0.clone();
        // listen events from tasks
        tokio::task::spawn(async move {
            loop {
                // self.makeup_tasks().await.expect("todo: you can retry later");
                if let Some(event) = task_event_receiver.recv().await {
                    match event {
                        BackupTaskEvent::New(backup_task) => {task_mgr.try_run(backup_task);},
                        BackupTaskEvent::Idle(backup_task) => {
                            task_mgr.remove_task(&backup_task);
                        }
                        BackupTaskEvent::ErrorAndWillRetry(backup_task, err) => {
                            task_mgr.remove_task(&backup_task);
                        }
                        BackupTaskEvent::Fail(backup_task, err) => {
                            task_mgr.remove_task(&backup_task);
                        }
                        BackupTaskEvent::Successed(backup_task) => {
                            task_mgr.remove_task(&backup_task);
                        }
                        BackupTaskEvent::Stop(backup_task) => {
                            let task_count = task_mgr.remove_task(&backup_task);
                            let mut state = task_mgr.state.lock().await;
                            match &*state {
                                BackupState::Running(_) => {}
                                BackupState::Stopping(stop_notifier, _) => {
                                    if task_count == 0 {
                                        let stop_notifier = stop_notifier.clone();
                                        *state = BackupState::Stopped;
                                        stop_notifier.send(()).await;
                                        break;
                                    }
                                }
                                BackupState::Stopped => {
                                    unreachable!("should no task running when stopped")
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        {
            let mut state = self.0.state.lock().await;
            match &*state {
                BackupState::Running(task_event_sender) => *state = BackupState::Stopping(sender, task_event_sender.clone()),
                BackupState::Stopping(_, _) => {
                    return Err("BackupTaskMgr is stopping, you should wait it finish.".into())
                }
                BackupState::Stopped => return Err("BackupTaskMgr is already stopped".into()),
            }
        }
        receiver.recv().await;
        Ok(())
    }

    pub async fn backup(
        &self,
        task_key: TaskKey,
        check_point_version: CheckPointVersion,
        prev_check_point_version: Option<CheckPointVersion>,
        meta: Option<String>,
        dir_path: PathBuf,
        chunk_files: Vec<(PathBuf, Option<(String, u64)>)>,
        is_discard_incomplete_versions: bool,
        priority: u32,
        is_manual: bool,
    ) -> Result<BackupTask, Box<dyn std::error::Error + Send + Sync>> {
        let mgr = Arc::downgrade(&self.0);
        let backup_task = BackupTask::create_new(
            mgr,
            task_key.clone(),
            check_point_version,
            prev_check_point_version,
            meta,
            dir_path,
            chunk_files,
            priority,
            is_manual,
        )
        .await?;

        if let Some(task_event_sender) = self.0.task_event_sender() {
            task_event_sender.send(BackupTaskEvent::New(backup_task.clone())).await?;
        }
            

        if let Ok(remote_task_server) = self.0.task_mgr_selector.select(&task_key, None).await {
            if let Ok(strategy) = remote_task_server.get_check_point_strategy(&task_key).await {
                if let Ok(removed_tasks) = self
                    .0
                    .task_storage
                    .prepare_clear_task_in_strategy(
                        &task_key,
                        check_point_version,
                        prev_check_point_version,
                        &strategy,
                    )
                    .await
                {
                    // TODO: stop and remove the removed tasks.

                    self.0
                        .task_storage
                        .delete_tasks_by_id(removed_tasks.as_slice())
                        .await;
                }
            }
        }

        Ok(backup_task)
    }

    pub async fn get_tasks(
        &self,
        task_key: &TaskKey,
        check_point_version: Option<CheckPointVersion>,
    ) -> Result<Vec<TaskInfo>, Box<dyn std::error::Error + Send + Sync>> {
        unimplemented!()
    }

    pub async fn get_last_check_point_version(
        &self,
        task_key: &TaskKey,
    ) -> Result<TaskInfo, Box<dyn std::error::Error + Send + Sync>> {
        self.0
            .task_storage
            .get_last_check_point_version(task_key, false)
            .await
    }

    pub async fn get_check_point_version_list(
        &self,
        task_key: &TaskKey,
        offset: ListOffset,
        limit: u32,
    ) -> Result<Vec<TaskInfo>, Box<dyn std::error::Error + Send + Sync>> {
        self.0
            .task_storage
            .get_check_point_version_list(task_key, offset, limit, false)
            .await
    }

    pub async fn get_check_point_strategy(
        &self,
        task_key: &TaskKey,
    ) -> Result<CheckPointVersionStrategy, Box<dyn std::error::Error + Send + Sync>> {
        self.0.task_mgr_selector
            .select(task_key, None)
            .await?.get_check_point_strategy(task_key).await
    }

    pub async fn update_check_point_strategy(
        &self,
        task_key: &TaskKey,
        strategy: CheckPointVersionStrategy,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0
            .task_mgr_selector
            .select(task_key, None)
            .await?
            .update_check_point_strategy(task_key, strategy)
            .await
    }

    async fn makeup_tasks(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let incomplete_tasks = self.0.task_storage.get_incomplete_tasks(0, 10).await?;
        // TODO: filter by priority
        for task in incomplete_tasks {
            let backup_task = BackupTask::from_storage(Arc::downgrade(&self.0), task);
            self.0.try_run(backup_task);
        }

        Ok(())
    }
}

pub struct RestoreTaskMgr {
    task_storage: Box<dyn TaskStorageQuerier>,
    file_mgr_selector: Box<dyn FileMgrSelector>,
}

impl RestoreTaskMgr {
    pub fn new(
        task_storage: Box<dyn TaskStorageQuerier>,
        file_mgr_selector: Box<dyn FileMgrSelector>,
    ) -> Self {
        RestoreTaskMgr {
            task_storage,
            file_mgr_selector,
        }
    }

    pub async fn restore(
        &self,
        task_key: TaskKey,
        check_point_version: CheckPointVersion,
        dir_path: &Path,
    ) -> Result<RestoreTask, Box<dyn std::error::Error + Send + Sync>> {
        unimplemented!()
    }

    pub async fn get_last_check_point_version(
        &self,
        task_key: &TaskKey,
    ) -> Result<TaskInfo, Box<dyn std::error::Error + Send + Sync>> {
        self.task_storage
            .get_last_check_point_version(task_key, true)
            .await
    }

    pub async fn get_check_point_version_list(
        &self,
        task_key: &TaskKey,
        offset: ListOffset,
        limit: u32,
    ) -> Result<Vec<TaskInfo>, Box<dyn std::error::Error + Send + Sync>> {
        self.task_storage
            .get_check_point_version_list(task_key, offset, limit, true)
            .await
    }
}
