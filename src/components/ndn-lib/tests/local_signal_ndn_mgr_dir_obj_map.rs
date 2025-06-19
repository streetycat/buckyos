use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    ops::{Deref, Index},
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::write;
use buckyos_kit::*;
use chrono::offset;
use cyfs_gateway_lib::*;
use cyfs_warp::*;
use hex::ToHex;
use jsonwebtoken::EncodingKey;
use log::*;
use ndn_lib::*;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

#[derive(Serialize, Deserialize, Clone)]
pub struct DirObject {
    pub name: String,
    pub content: String, //ObjectMapId
    #[serde(default)]
    #[serde(skip_serializing_if = "is_default")]
    pub exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<u64>,
    #[serde(flatten)]
    pub extra_info: HashMap<String, Value>,
}

pub struct FileStorageItem {
    pub obj: FileObject,
    pub chunk_size: Option<u64>,
}

pub struct ChunkItem {
    pub seq: u64,          // sequence number in file
    pub offset: SeekFrom,  // offset in the file
    pub chunk_id: ChunkId, // chunk id
}

pub enum StorageItem {
    Dir(DirObject),
    File(FileStorageItem),
    Chunk(ChunkItem), // (seq, offset, ChunkId)
}

pub enum StorageItemName {
    Name(String),
    ChunkSeq(u64), // seq
}

pub enum StorageItemNameRef<'a> {
    Name(&'a str),
    ChunkSeq(u64), // seq
}

impl StorageItem {
    fn is_dir(&self) -> bool {
        matches!(self, StorageItem::Dir(_))
    }
    fn is_file(&self) -> bool {
        matches!(self, StorageItem::File(_))
    }
    fn is_chunk(&self) -> bool {
        matches!(self, StorageItem::Chunk(_))
    }
    fn name(&self) -> StorageItemNameRef<'_> {
        match self {
            StorageItem::Dir(dir) => StorageItemNameRef::Name(dir.name.as_str()),
            StorageItem::File(file) => StorageItemNameRef::Name(file.obj.name.as_str()),
            StorageItem::Chunk(chunk_item) => StorageItemNameRef::ChunkSeq(chunk_item.seq),
        }
    }
    fn item_type(&self) -> &str {
        match self {
            StorageItem::Dir(_) => "dir",
            StorageItem::File(_) => "file",
            StorageItem::Chunk(_) => "chunk",
        }
    }
}

pub type PathDepth = u64;

#[derive(Clone, Debug)]
pub enum ItemStatus {
    New,
    Scanning,
    Hashing,
    Transfer(ObjId),
    Complete(ObjId),
}

impl ItemStatus {
    pub fn is_new(&self) -> bool {
        matches!(self, ItemStatus::New)
    }
    pub fn is_scanning(&self) -> bool {
        matches!(self, ItemStatus::Scanning)
    }
    pub fn is_hashing_or_transfer(&self) -> bool {
        matches!(self, ItemStatus::Hashing | ItemStatus::Transfer(_))
    }
    pub fn is_complete(&self) -> bool {
        matches!(self, ItemStatus::Complete(_))
    }
    pub fn is_hashing(&self) -> bool {
        matches!(self, ItemStatus::Hashing)
    }
    pub fn is_transfer(&self) -> bool {
        matches!(self, ItemStatus::Transfer(_))
    }

    pub fn get_obj_id(&self) -> Option<&ObjId> {
        match self {
            ItemStatus::Transfer(obj_id) | ItemStatus::Complete(obj_id) => Some(obj_id),
            _ => None,
        }
    }
}

pub trait StrorageCreator<S: Storage<ItemId = Self::ItemId>>:
    AsyncFn(String) -> NdnResult<S> + Send + Sync + Sized
{
    type ItemId: Send + Sync + Clone + std::fmt::Debug + Eq + std::hash::Hash + Sized;
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync + Sized + Clone {
    type ItemId: Send + Sync + Clone + std::fmt::Debug + Eq + std::hash::Hash + Sized;
    async fn create_new_item(
        &self,
        item: &StorageItem,
        depth: PathDepth,
        parent_path: &Path,
        parent_item_id: Option<Self::ItemId>,
    ) -> NdnResult<(Self::ItemId, StorageItem, ItemStatus)>;
    async fn remove_dir(&self, item_id: &Self::ItemId) -> NdnResult<u64>;
    async fn remove_children(&self, item_id: &Self::ItemId) -> NdnResult<u64>;
    async fn begin_hash(&self, item_id: &Self::ItemId) -> NdnResult<()>;
    async fn begin_transfer(&self, item_id: &Self::ItemId, content: &ObjId) -> NdnResult<()>;
    async fn complete(&self, item_id: &Self::ItemId) -> NdnResult<()>;
    async fn get_item(
        &self,
        item_id: &Self::ItemId,
    ) -> NdnResult<(StorageItem, ItemStatus, PathDepth)>;
    async fn select_dir_scan_or_new(
        &self,
    ) -> NdnResult<Option<(Self::ItemId, DirObject, PathBuf, ItemStatus, PathDepth)>>; // to continue scan
    async fn select_file_hashing_or_transfer(
        &self,
    ) -> NdnResult<
        Option<(
            Self::ItemId,
            FileStorageItem,
            PathBuf,
            ItemStatus,
            PathDepth,
        )>,
    >; // to continue transfer file after all child-dir hash and all child-file complete
    async fn select_dir_hashing_with_all_child_dir_transfer_and_file_complete(
        &self,
    ) -> NdnResult<Option<(Self::ItemId, DirObject, PathBuf, ItemStatus, PathDepth)>>; // to continue transfer dir after all children complete
    async fn select_dir_transfer(
        &self,
        depth: Option<PathDepth>,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> NdnResult<Vec<(Self::ItemId, StorageItem, PathBuf, ItemStatus, PathDepth)>>; // to transfer dir
    async fn select_item_transfer_with_all_children_complete(
        &self,
    ) -> NdnResult<Option<(Self::ItemId, StorageItem, PathBuf, ItemStatus, PathDepth)>>; // to continue transfer after all children complete
    async fn list_children_order_by_name(
        &self,
        item_id: &Self::ItemId,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> NdnResult<(
        Vec<(Self::ItemId, StorageItem, ItemStatus, PathDepth)>,
        PathBuf,
    )>;
    async fn list_chunks_by_chunk_id(
        &self,
        chunk_ids: &[ChunkId],
    ) -> NdnResult<Vec<(Self::ItemId, ChunkItem, PathBuf, ItemStatus, PathDepth)>>;
}

pub enum FileSystemItem {
    Dir(DirObject),
    File(FileObject),
}

#[async_trait::async_trait]
pub trait FileSystemReader<D: FileSystemDirReader, F: FileSystemFileReader>:
    Send + Sync + Sized + Clone
{
    async fn info(&self, path: &std::path::Path) -> NdnResult<FileSystemItem>;
    async fn open_dir(&self, path: &std::path::Path) -> NdnResult<D>;
    async fn open_file(&self, path: &std::path::Path) -> NdnResult<F>;
}

#[async_trait::async_trait]
pub trait FileSystemDirReader: Send + Sync + Sized {
    async fn next(&self, limit: Option<u64>) -> NdnResult<Vec<FileSystemItem>>;
}

#[async_trait::async_trait]
pub trait FileSystemFileReader: Send + Sync + Sized {
    async fn read_chunk(&self, offset: SeekFrom, limit: Option<u64>) -> NdnResult<Vec<u8>>;
}

#[async_trait::async_trait]
pub trait FileSystemWriter<W: FileSystemFileWriter>: Send + Sync + Sized {
    async fn mkdir(&self, dir: &DirObject, parent_path: &Path) -> NdnResult<()>;
    async fn open_file(&self, file: &FileObject, parent_path: &Path) -> NdnResult<W>;
}

#[async_trait::async_trait]
pub trait FileSystemDirWriter: Send + Sync + Sized {}

#[async_trait::async_trait]
pub trait FileSystemFileWriter: Send + Sync + Sized {
    async fn write_chunk(&self, offset: SeekFrom, limit: Option<u64>) -> NdnResult<Vec<u8>>;
}

#[async_trait::async_trait]
pub trait NdnWriter: Send + Sync + Sized + Clone {
    async fn push_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<Vec<ObjId>>; // lost child-obj-list
    async fn push_chunk(&self, chunk_id: &ChunkId, chunk_data: &[u8]) -> NdnResult<()>;
    async fn push_container(&self, container_id: &ObjId) -> NdnResult<Vec<ObjId>>; // lost child-obj-list
}

#[async_trait::async_trait]
pub trait NdnReader: Send + Sync + Sized {
    async fn get_object(&self, obj_id: &ObjId) -> NdnResult<Value>; // lost child-obj-list
    async fn get_chunk(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>>;
}

pub async fn file_system_to_ndn<
    S: Storage,
    FDR: FileSystemDirReader,
    FFR: FileSystemFileReader,
    F: FileSystemReader<FDR, FFR>,
    N: NdnWriter,
>(
    path: &Path,
    writer: N,
    reader: F,
    storage: S,
    chunk_size: u64,
    ndn_mgr_id: &str,
) -> NdnResult<ObjId> {
    let is_finish_scan = Arc::new(tokio::sync::Mutex::new(false));
    let (continue_hash_emiter, continue_hash_handler) = tokio::sync::mpsc::channel::<()>(64);

    let scan_task_process = async |path: &Path,
                                   reader: F,
                                   storage: S,
                                   chunk_size: u64,
                                   emiter: tokio::sync::mpsc::Sender<()>|
           -> NdnResult<()> {
        let insert_new_item = async |item: FileSystemItem,
                                     parent_path: &Path,
                                     storage: &S,
                                     level: u64,
                                     parent_item_id: Option<S::ItemId>|
               -> NdnResult<()> {
            match item {
                FileSystemItem::Dir(dir_object) => {
                    storage
                        .create_new_item(
                            &StorageItem::Dir(dir_object),
                            level,
                            parent_path,
                            parent_item_id,
                        )
                        .await?;
                }
                FileSystemItem::File(file_object) => {
                    let file_size = file_object.size;
                    let file_reader = reader
                        .open_file(parent_path.join(file_object.name.as_str()).as_path())
                        .await?;
                    let (file_item_id, file_item, item_status) = storage
                        .create_new_item(
                            &StorageItem::File(FileStorageItem {
                                obj: file_object,
                                chunk_size: Some(chunk_size),
                            }),
                            level,
                            parent_path,
                            parent_item_id.clone(),
                        )
                        .await?;
                    let chunk_size = match file_item {
                        StorageItem::File(file_storage_item) => {
                            file_storage_item.chunk_size.unwrap_or(chunk_size)
                        }
                        _ => Err(NdnError::InvalidObjType(format!(
                            "expect file, got: {:?} in history storage",
                            file_item.item_type()
                        )))?,
                    };
                    for i in 0..(file_size + chunk_size - 1) / chunk_size {
                        let offet = SeekFrom::Start(i * chunk_size);
                        let chunk_data = file_reader.read_chunk(offet, Some(chunk_size)).await?;
                        let hasher = ChunkHasher::new(None).expect("hash failed.");
                        let hash = hasher.calc_from_bytes(&chunk_data);
                        let chunk_id = ChunkId::mix_from_hash_result(
                            chunk_data.len() as u64,
                            &hash,
                            HashMethod::Sha256,
                        );
                        let (chunk_item_id, _, _) = storage
                            .create_new_item(
                                &StorageItem::Chunk(ChunkItem {
                                    seq: i as u64,
                                    offset: SeekFrom::Start(i * chunk_size),
                                    chunk_id: chunk_id.clone(),
                                }),
                                level,
                                parent_path,
                                Some(parent_item_id.clone().unwrap()),
                            )
                            .await?;
                        storage
                            .begin_transfer(&chunk_item_id, &chunk_id.to_obj_id())
                            .await?;
                    }

                    storage.begin_hash(&file_item_id).await?;
                }
            }
            Ok(())
        };

        let none_path = PathBuf::from("");
        let fs_item = reader.info(path).await?;
        insert_new_item(
            fs_item,
            path.parent().unwrap_or(none_path.as_path()),
            &storage,
            0,
            None,
        )
        .await?;

        emiter.send(()).await;

        loop {
            match storage.select_dir_scan_or_new().await? {
                Some((item_id, dir_object, parent_path, item_status, depth)) => {
                    info!("scan dir: {}, item_id: {:?}", dir_object.name, item_id);
                    let dir_reader = reader
                        .open_dir(parent_path.join(dir_object.name).as_path())
                        .await?;

                    loop {
                        let items = dir_reader.next(Some(64)).await?;
                        let is_finish = items.len() < 64;
                        for item in items {
                            insert_new_item(
                                item,
                                parent_path.as_path(),
                                &storage,
                                depth + 1,
                                Some(item_id.clone()),
                            )
                            .await?;

                            emiter.send(()).await;
                        }
                        if is_finish {
                            break;
                        }
                    }
                    storage.begin_hash(&item_id).await?;
                }
                None => break,
            }
        }

        Ok(())
    };

    let scan_task = {
        let path = path.to_path_buf();
        let reader = reader.clone();
        let storage = storage.clone();
        let is_finish_scan = is_finish_scan.clone();
        let emiter = continue_hash_emiter.clone();
        tokio::spawn(async move {
            let ret = scan_task_process(path.as_path(), reader, storage, chunk_size, emiter).await;
            *is_finish_scan.lock().await = true;
            emiter.send(()).await;
            ret
        })
    };

    let transfer_task_process = async |storage: S,
                                       reader: F,
                                       writer: N,
                                       is_finish_scan: Arc<tokio::sync::Mutex<bool>>,
                                       continue_hash_handler: tokio::sync::mpsc::Receiver<()>,
                                       ndn_mgr_id: &str|
           -> NdnResult<()> {
        let mut is_hash_finish_pending = false;
        loop {
            loop {
                // hash files
                match storage.select_file_hashing_or_transfer().await? {
                    Some((item_id, mut file_item, parent_path, mut file_status, depth)) => {
                        info!("hashing file: {:?}, status: {:?}", item_id, file_status);
                        let (file_obj_id, file_obj_str, file_chunk_list_id, file_chunk_list_str) =
                            if file_status.is_hashing() {
                                assert!(
                                    file_item.obj.content.is_empty(),
                                    "file content should be empty for before hashing."
                                );

                                let mut chunk_list_builder = ChunkListBuilder::new(
                                    HashMethod::Sha256,
                                    file_item.chunk_size.map(|chunk_size| {
                                        ((file_item.obj.size + chunk_size - 1) / chunk_size)
                                            as usize
                                    }),
                                )
                                .with_total_size(file_item.obj.size);
                                if let Some(chunk_size) = file_item.chunk_size {
                                    chunk_list_builder =
                                        chunk_list_builder.with_fixed_size(chunk_size);
                                }

                                let mut batch_count = 0;
                                let batch_limit = 64;
                                loop {
                                    let (chunk_items, _) = storage
                                        .list_children_order_by_name(
                                            &item_id,
                                            Some(batch_count * batch_limit),
                                            Some(batch_limit),
                                        )
                                        .await?;
                                    batch_count += 1;
                                    let is_file_ready = (chunk_items.len() as u64) < batch_limit;
                                    for (_, chunk_item, chunk_status, chunk_depth) in chunk_items {
                                        assert_eq!(
                                            chunk_depth,
                                            depth + 1,
                                            "chunk depth should be one more than file depth."
                                        );
                                        assert!(
                                            chunk_status.is_transfer(),
                                            "chunk item status should be Hashing, but: {:?}.",
                                            chunk_status
                                        );
                                        // expect chunk item
                                        match chunk_item {
                                            StorageItem::Chunk(chunk_item) => {
                                                assert_eq!(
                                                    chunk_item
                                                        .chunk_id
                                                        .get_length()
                                                        .expect("chunk id should fix size"),
                                                    file_item.chunk_size.expect(
                                                        "chunk size should be fix for file"
                                                    )
                                                );
                                                assert_eq!(
                                                    chunk_item.seq,
                                                    chunk_list_builder.len() as u64
                                                );
                                                assert_eq!(
                                                    chunk_item.offset,
                                                    SeekFrom::Start(
                                                        chunk_list_builder.len() as u64
                                                            * file_item.chunk_size.unwrap_or(0)
                                                    )
                                                );
                                                chunk_list_builder
                                                    .append(chunk_item.chunk_id.clone());
                                            }
                                            _ => {
                                                unreachable!(
                                                "expect chunk item, got: {:?} in history storage",
                                                chunk_item.item_type()
                                            );
                                            }
                                        }
                                    }
                                    if is_file_ready {
                                        break;
                                    }
                                }

                                let file_chunk_list = chunk_list_builder.build().await?;
                                let (file_chunk_list_id, file_chunk_list_str) =
                                    file_chunk_list.calc_obj_id();
                                NamedDataMgr::put_object(
                                    Some(ndn_mgr_id),
                                    &file_chunk_list_id,
                                    file_chunk_list_str.as_str(),
                                )
                                .await?;

                                file_item.obj.content = file_chunk_list_id.to_string();
                                let (file_obj_id, file_obj_str) = file_item.obj.gen_obj_id();
                                storage.begin_transfer(&item_id, &file_obj_id).await?;

                                (
                                    file_obj_id,
                                    file_obj_str,
                                    file_chunk_list_id,
                                    file_chunk_list_str,
                                )
                            } else if file_status.is_transfer() {
                                info!("transfer file: {:?}, status: {:?}", item_id, file_status);
                                let (file_obj_id, file_obj_str) = file_item.obj.gen_obj_id();
                                assert_eq!(
                                    file_status.get_obj_id(),
                                    Some(&file_obj_id),
                                    "file content should be set to chunk list id before transfer."
                                );

                                let file_chunk_list_id = ObjId::try_from(
                                    file_item.obj.content.as_str(),
                                )
                                .expect("file content should be a valid ObjId for chunk-list.");

                                let chunk_list_json_value = NamedDataMgr::get_object(
                                    Some(ndn_mgr_id),
                                    &file_chunk_list_id,
                                    None,
                                )
                                .await?;
                                let (chunk_list_id, chunk_list_str) = build_named_object_by_json(
                                    OBJ_TYPE_CHUNK_LIST,
                                    &chunk_list_json_value,
                                );
                                assert_eq!(
                                    chunk_list_id, file_chunk_list_id,
                                    "chunk list id should match the file content id."
                                );

                                (
                                    file_obj_id,
                                    file_obj_str,
                                    file_chunk_list_id,
                                    chunk_list_str,
                                )
                            } else {
                                unreachable!(
                                    "item status should be Hashing or Transfer, got: {:?}",
                                    file_status
                                );
                            };

                        let (lost_obj_ids) =
                            writer.push_object(&file_obj_id, &file_obj_str).await?;
                        if let Some(lost_obj_id) = lost_obj_ids.get(0) {
                            debug!(
                                "lost child objects when push file object: {}, lost: {:?}",
                                file_obj_id, lost_obj_ids
                            );
                            assert_eq!(lost_obj_id, &file_chunk_list_id);
                            let (lost_obj_ids) = writer
                                .push_object(&file_chunk_list_id, file_chunk_list_str.as_str())
                                .await?;
                            if let Some(lost_obj_id) = lost_obj_ids.get(0) {
                                debug!(
                                    "lost child objects when push chunk list object: {}, lost: {:?}",
                                    file_chunk_list_id, lost_obj_ids
                                );
                                let lost_chunk_ids = writer.push_container(lost_obj_id).await?;
                                let limit = 16;
                                let file_path = parent_path.join(file_item.obj.name.as_str());
                                for i in 0..(lost_chunk_ids.len() + limit - 1) / limit {
                                    let chunk_ids = &lost_chunk_ids[i * limit
                                        ..std::cmp::min((i + 1) * limit, lost_chunk_ids.len())];
                                    let chunk_ids = chunk_ids
                                        .iter()
                                        .map(|id| ChunkId::from_obj_id(id))
                                        .collect::<Vec<_>>();
                                    let chunk_items = storage
                                        .list_chunks_by_chunk_id(chunk_ids.as_slice())
                                        .await?;
                                    assert_eq!(
                                        chunk_items.len(),
                                        chunk_ids.len(),
                                        "chunk items should match the chunk ids."
                                    );
                                    for ((_, chunk_item, chunk_file_path, _, _), chunk_id) in
                                        chunk_items.iter().zip(chunk_ids.iter())
                                    {
                                        assert_eq!(
                                            chunk_file_path, &file_path,
                                            "chunk file path should match the file path."
                                        );
                                        assert_eq!(
                                            chunk_item.chunk_id, *chunk_id,
                                            "chunk item id should match the chunk id."
                                        );
                                        let chunk_reader =
                                            reader.open_file(file_path.as_path()).await?;
                                        let chunk_data = chunk_reader
                                            .read_chunk(
                                                chunk_item.offset,
                                                Some(
                                                    chunk_item
                                                        .chunk_id
                                                        .get_length()
                                                        .expect("chunk id should have length"),
                                                ),
                                            )
                                            .await?;
                                        writer.push_chunk(chunk_id, chunk_data.as_slice()).await?;
                                    }
                                }
                                if lost_chunk_ids.len() > 0 {
                                    let lost_chunk_ids =
                                        writer.push_container(&lost_obj_id).await?;
                                    assert!(
                                        lost_chunk_ids.is_empty(),
                                        "lost chunk ids should be empty after push container."
                                    );
                                }
                                let lost_obj_container = writer
                                    .push_object(&file_chunk_list_id, &file_chunk_list_str)
                                    .await?;
                                assert!(
                                    lost_obj_container.is_empty(),
                                    "lost object container should be empty after push object."
                                );
                            }
                            let lost_chunk_list =
                                writer.push_object(&file_obj_id, &file_obj_str).await?;
                            assert!(
                                lost_chunk_list.is_empty(),
                                "lost chunk list should be empty after push object."
                            );
                        }

                        storage.complete(&item_id).await?;

                        info!("transfer file: {:?}, status: {:?}", item_id, file_status);
                    }
                    None => {
                        break;
                    }
                }
            }

            loop {
                // hash dirs
                match storage
                    .select_dir_hashing_with_all_child_dir_transfer_and_file_complete()
                    .await?
                {
                    Some((item_id, dir_object, parent_path, item_status, depth)) => {
                        info!("hashing dir: {:?}, status: {:?}", item_id, item_status);
                        assert!(item_status.is_hashing());

                        let mut dir_obj_map = TrieObjectMap::new(
                            HashMethod::Sha256,
                            Some(TrieObjectMapStorageType::JSONFile),
                        )
                        .await?;

                        let mut dir_children_batch_index = 0;
                        let dir_children_batch_limit = 64;

                        loop {
                            let (children, parent_path) = storage
                                .list_children_order_by_name(
                                    &item_id,
                                    Some(dir_children_batch_index * dir_children_batch_limit),
                                    Some(dir_children_batch_limit),
                                )
                                .await?;
                            dir_children_batch_index += 1;
                            let is_dir_ready = (children.len() as u64) < dir_children_batch_limit;
                            for (child_item_id, child_item, child_status, child_depth) in children {
                                assert_eq!(
                                    child_depth,
                                    depth + 1,
                                    "child item depth should be one more than dir depth."
                                );

                                match child_item {
                                    StorageItem::Dir(dir_obj) => {
                                        assert!(
                                            child_status.is_transfer(),
                                            "child dir item should be complete, got: {:?}",
                                            child_status
                                        );
                                        dir_obj_map
                                            .put_object(
                                                dir_obj.name.as_str(),
                                                child_status
                                                    .get_obj_id()
                                                    .expect("child dir item should have obj id."),
                                            )
                                            .await?;
                                    }
                                    StorageItem::File(file_item) => {
                                        assert!(
                                            child_status.is_complete(),
                                            "child file item should be complete, got: {:?}",
                                            child_status
                                        );
                                        dir_obj_map
                                            .put_object(
                                                file_item.obj.name.as_str(),
                                                child_status
                                                    .get_obj_id()
                                                    .expect("child file item should have obj id."),
                                            )
                                            .await?;
                                    }
                                    StorageItem::Chunk(_) => {
                                        unreachable!("should not have chunk item in dir hashing.");
                                    }
                                }
                            }
                            if is_dir_ready {
                                break;
                            }
                        }

                        dir_obj_map.save().await?;
                        let dir_obj_map_id = dir_obj_map.get_obj_id().await;
                        storage.begin_transfer(&item_id, &dir_obj_map_id).await?;
                    }
                    None => {
                        break;
                    }
                }
            }

            if is_hash_finish_pending {
                info!("hash is in finish pending for last search.");
                break;
            }

            if *is_finish_scan.lock().await {
                info!("finish scan, look for hashing items again.");
                is_hash_finish_pending = true;
            } else {
                // wait new item found
                match continue_hash_handler.try_recv() {
                    Ok(_) => {
                        while let Ok(_) = continue_hash_handler.try_recv() {
                            // continue to wait for new item
                        }
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        let _ = continue_hash_handler.recv().await;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        unreachable!("continue hash channel closed.");
                    }
                }
            }
        }

        // transfer dirs
        let mut scan_depth = 0;
        let scan_batch_limit = 64;
        let mut scan_batch_index = 0;
        loop {
            match storage
                .select_dir_transfer(
                    Some(scan_depth),
                    Some(scan_batch_index * scan_batch_limit),
                    Some(scan_batch_limit),
                )
                .await?
            {
                Some((item_id, item, parent_path, item_status, depth)) => {
                    info!("transfer dir: {:?}, status: {:?}", item_id, item_status);
                    assert!(item_status.is_transfer());

                    let dir_obj = match item {
                        StorageItem::Dir(dir_obj) => dir_obj,
                        _ => Err(NdnError::InvalidObjType(format!(
                            "expect dir object, got: {:?} in history storage",
                            item.item_type()
                        )))?,
                    };

                    let (dir_obj_id, dir_obj_str) = dir_obj.gen_obj_id();
                    let lost_obj_ids = writer.push_object(&dir_obj_id, &dir_obj_str).await?;
                    if !lost_obj_ids.is_empty() {
                        debug!(
                            "lost child objects when push dir object: {}, lost: {:?}",
                            dir_obj_id, lost_obj_ids
                        );
                        let lost_container = writer.push_container(&dir_obj_id).await?;
                        assert!(
                            lost_container.is_empty(),
                            "lost container should be empty after push object."
                        );
                    }

                    storage.complete(&item_id).await?;

                    info!("transfer dir: {:?}, status: {:?}", item_id, item_status);
                }
                None => {
                    break;
                }
            }
        }
        Ok(())
    };

    let transfer_task = {
        let reader = reader.clone();
        let writer = writer.clone();
        let storage = storage.clone();
        let is_finish_scan = is_finish_scan.clone();
        let ndn_mgr_id = ndn_mgr_id.to_string();
        tokio::spawn(async move {
            let ret = transfer_task_process(
                storage,
                reader,
                writer,
                is_finish_scan,
                continue_hash_handler,
                ndn_mgr_id.as_str(),
            )
            .await;
            ret
        })
    };

    let root_obj_id = transfer_task.await.expect("task run failed")?;
    scan_task.await.expect("task run failed")?;

    Ok(())
}

fn generate_random_bytes(size: u64) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut buffer = vec![0u8; size as usize];
    rng.fill_bytes(&mut buffer);
    buffer
}

fn generate_random_chunk_mix(size: u64) -> (ChunkId, Vec<u8>) {
    let chunk_data = generate_random_bytes(size);
    let hasher = ChunkHasher::new(None).expect("hash failed.");
    let hash = hasher.calc_from_bytes(&chunk_data);
    let chunk_id = ChunkId::mix_from_hash_result(size, &hash, HashMethod::Sha256);
    info!("chunk_id: {}", chunk_id.to_string());
    (chunk_id, chunk_data)
}

fn generate_random_chunk(size: u64) -> (ChunkId, Vec<u8>) {
    let chunk_data = generate_random_bytes(size);
    let hasher = ChunkHasher::new(None).expect("hash failed.");
    let hash = hasher.calc_from_bytes(&chunk_data);
    let chunk_id = ChunkId::from_hash_result(&hash, HashMethod::Sha256);
    info!("chunk_id: {}", chunk_id.to_string());
    (chunk_id, chunk_data)
}

fn generate_random_chunk_list(count: usize, fix_size: Option<u64>) -> Vec<(ChunkId, Vec<u8>)> {
    let mut chunk_list = Vec::with_capacity(count);
    for _ in 0..count {
        let (chunk_id, chunk_data) = if let Some(size) = fix_size {
            generate_random_chunk_mix(size)
        } else {
            generate_random_chunk_mix(rand::rng().random_range(1024u64..1024 * 1024 * 10))
        };
        chunk_list.push((chunk_id, chunk_data));
    }
    chunk_list
}

async fn write_chunk(ndn_mgr_id: &str, chunk_id: &ChunkId, chunk_data: &[u8]) {
    let (mut chunk_writer, _progress_info) =
        NamedDataMgr::open_chunk_writer(Some(ndn_mgr_id), chunk_id, chunk_data.len() as u64, 0)
            .await
            .expect("open chunk writer failed");
    chunk_writer
        .write_all(chunk_data)
        .await
        .expect("write chunk to ndn-mgr failed");
    NamedDataMgr::complete_chunk_writer(Some(ndn_mgr_id), chunk_id)
        .await
        .expect("wait chunk writer complete failed.");
}

async fn read_chunk(ndn_mgr_id: &str, chunk_id: &ChunkId) -> Vec<u8> {
    let (mut chunk_reader, len) =
        NamedDataMgr::open_chunk_reader(Some(ndn_mgr_id), chunk_id, SeekFrom::Start(0), false)
            .await
            .expect("open reader from ndn-mgr failed.");

    let mut buffer = vec![0u8; len as usize];
    chunk_reader
        .read_exact(&mut buffer)
        .await
        .expect("read chunk from ndn-mgr failed");

    buffer
}

type NdnServerHost = String;

async fn init_ndn_server(ndn_mgr_id: &str) -> (NdnClient, NdnServerHost) {
    let mut rng = rand::rng();
    let tls_port = rng.random_range(10000u16..20000u16);
    let http_port = rng.random_range(10000u16..20000u16);
    let test_server_config = json!({
        "tls_port": tls_port,
        "http_port": http_port,
        "hosts": {
            "*": {
                "enable_cors": true,
                "routes": {
                    "/ndn/": {
                        "named_mgr": {
                            "named_data_mgr_id": ndn_mgr_id,
                            "read_only": false,
                            "guest_access": true,
                            "is_chunk_id_in_path": true,
                            "enable_mgr_file_path": true
                        }
                    }
                }
            }
        }
    });

    let test_server_config: WarpServerConfig = serde_json::from_value(test_server_config).unwrap();

    tokio::spawn(async move {
        info!("start test ndn server(powered by cyfs-warp)...");
        start_cyfs_warp_server(test_server_config)
            .await
            .expect("start cyfs warp server failed.");
    });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let temp_dir = tempfile::tempdir()
        .unwrap()
        .path()
        .join("ndn-test")
        .join(ndn_mgr_id);

    fs::create_dir_all(temp_dir.as_path())
        .await
        .expect("create temp dir failed.");

    let config = NamedDataMgrConfig {
        local_stores: vec![temp_dir.to_str().unwrap().to_string()],
        local_cache: None,
        mmap_cache_dir: None,
    };

    let named_mgr =
        NamedDataMgr::from_config(Some(ndn_mgr_id.to_string()), temp_dir.to_path_buf(), config)
            .await
            .expect("init NamedDataMgr failed.");

    NamedDataMgr::set_mgr_by_id(Some(ndn_mgr_id), named_mgr)
        .await
        .expect("set named data manager by id failed.");

    let host = format!("localhost:{}", http_port);
    let client = NdnClient::new(
        format!("http://{}/ndn/", host),
        None,
        Some(ndn_mgr_id.to_string()),
    );

    (client, host)
}

async fn init_obj_array_storage_factory() -> PathBuf {
    let data_path = std::env::temp_dir().join("test_ndn_chunklist_data");
    if GLOBAL_OBJECT_ARRAY_STORAGE_FACTORY.get().is_some() {
        info!("Object array storage factory already initialized");
        return data_path;
    }
    if !data_path.exists() {
        fs::create_dir_all(&data_path)
            .await
            .expect("create data path failed");
    }

    GLOBAL_OBJECT_ARRAY_STORAGE_FACTORY
        .set(ObjectArrayStorageFactory::new(&data_path))
        .map_err(|_| ())
        .expect("Object array storage factory already initialized");
    data_path
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_build() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_add_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_remove_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_append_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_insert_head_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_insert_random_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_trancation_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_remove_head_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_remove_random_file() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_add_dir() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}

#[tokio::test]
async fn ndn_local_dir_trie_obj_map_remove_dir() {
    init_logging("ndn_local_dir_trie_obj_map_build", false);

    info!("ndn_local_dir_trie_obj_map_build test start...");
    init_obj_array_storage_factory().await;
    let ndn_mgr_id: String = generate_random_bytes(16).encode_hex();
    let (ndn_client, ndn_host) = init_ndn_server(ndn_mgr_id.as_str()).await;

    info!("ndn_local_dir_trie_obj_map_build test end.");
}
