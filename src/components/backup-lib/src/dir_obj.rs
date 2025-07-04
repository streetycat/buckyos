use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use buckyos_kit::is_default;
use ndn_lib::{OBJ_TYPE_DIR, ObjId, build_named_object_by_json};

#[derive(Serialize, Deserialize, Clone, Debug)]
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

impl DirObject {
    pub fn gen_obj_id(&self) -> (ObjId, String) {
        build_named_object_by_json(
            OBJ_TYPE_DIR,
            &serde_json::to_value(self).expect("json::value from DirObject failed"),
        )
    }
}
