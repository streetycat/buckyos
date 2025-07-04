use std::collections::HashMap;

use ndn_lib::{ChunkId, FileObject, ObjId};
use serde::{Deserialize, Serialize};

use crate::DirObject;

#[derive(Serialize, Deserialize, Clone)]
pub enum RebuildMeta {
    File(FileObject),
    Dir(DirObject),
}

impl std::fmt::Debug for RebuildMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebuildMeta::File(file) => write!(
                f,
                "RebuildMeta::File({})",
                serde_json::to_string(&file).unwrap_or_else(|_| "Invalid FileObject".to_string())
            ),
            RebuildMeta::Dir(dir) => write!(
                f,
                "RebuildMeta::Dir({})",
                serde_json::to_string(&dir).unwrap_or_else(|_| "Invalid DirObject".to_string())
            ),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RebuildObject {
    pub id: ObjId,
    pub meta: RebuildMeta,
    pub chunks: Vec<ChunkId>,
}

impl std::fmt::Debug for RebuildObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RebuildObject(id: {}, meta: {:?}, chunks: {})",
            self.id,
            self.meta,
            debug_iter(self.chunks.iter(), 8)
        )
    }
}

fn debug_iter<T: std::fmt::Debug, I: Iterator<Item = T>>(iter: I, display_count: usize) -> String {
    let mut result = String::new();
    let mut found_count = 0;
    for item in iter {
        found_count += 1;
        if found_count <= display_count {
            result.push_str(&format!("{:?}, ", item));
        }
    }

    let more_count = found_count - display_count;
    if more_count > 0 {
        result.push_str(&format!("...{} more items", more_count));
        result
    } else {
        result.trim_end_matches(", ").to_string()
    }
}

// 客户端先向服务器push一系列Chunk对象，通过RebuildObject接口用这些Chunk重建相应的容器对象；
// 重建过程不递归查询子对象缺失的情况，
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpRequestRebuildObject {
    containers: Vec<RebuildObject>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LostObjDetail {
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub lost: HashMap<ObjId, Vec<ObjId>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub exist: HashMap<ObjId, Vec<ObjId>>,
}

impl std::fmt::Debug for LostObjDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LostObjDetail(lost: {}, exist: {:?})",
            format!(
                "{:?}",
                self.lost
                    .iter()
                    .map(|(container_id, lost_objs)| (
                        container_id,
                        debug_iter(lost_objs.iter(), 8)
                    ))
                    .collect::<HashMap<_, _>>()
            ),
            format!(
                "{:?}",
                self.exist
                    .iter()
                    .map(|(container_id, lost_objs)| (
                        container_id,
                        debug_iter(lost_objs.iter(), 8)
                    ))
                    .collect::<HashMap<_, _>>()
            )
        )
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum LostObjDesc {
    Lost(Vec<ObjId>),      // need push objects, empty for no lost objects
    Exist(Vec<ObjId>),     // push sub-obj exclude in this list, the container is specific
    Detail(LostObjDetail), // detail for every container
    InChunk(Vec<ChunkId>), // for large response, the client should pull all chunks and parse them to get the detail
}

impl std::fmt::Debug for LostObjDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LostObjDesc::Lost(objs) => write!(f, "Lost({})", debug_iter(objs.iter(), 8)),
            LostObjDesc::Exist(objs) => write!(f, "Exist({})", debug_iter(objs.iter(), 8)),
            LostObjDesc::Detail(detail) => write!(f, "Detail({:?})", detail),
            LostObjDesc::InChunk(chunks) => write!(f, "InChunk({})", debug_iter(chunks.iter(), 8)),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpResponseRebuildObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lost_objs: Option<LostObjDesc>,
}

// 经过对象重建，备份的根对象应该已经存在，递归检查所有子对象的存在情况，所有子对象都存在则备份成功；否则继续补充缺失子对象
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpRequestCheckFinish {
    // The root object ID of this backup
    pub root_id: ObjId,
    // The unique backup plan ID within the user's scope, used to distinguish different backup plans; users should be able to read them
    pub unique_plan_id: String,
    // The version number of the backup plan; users can use this version number to identify different versions of the backup plan
    pub version: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpResponseCheckFinish {
    // 如果没有缺失对象，本次备份成功
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lost_objs: Option<LostObjDesc>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpRequestListPlan {
    pub page: usize,
    pub size: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpResponseListPlan {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub plans: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpRequestListVersion {
    pub plan_id: String,
    pub page: usize,
    pub size: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpResponseListVersion {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub versions: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpRequestGetRoot {
    pub plan_id: String,
    pub version: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpResponseGetRoot {
    pub root_id: ObjId,
}
