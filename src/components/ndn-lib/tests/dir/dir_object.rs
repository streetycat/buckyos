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
