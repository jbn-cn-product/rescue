use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(default)]
    pub ts: i64,
    #[serde(default)]
    pub data: serde_json::Value,
}

impl Envelope {
    pub fn new(typ: &str, data: serde_json::Value) -> Self {
        Self {
            v: 1,
            id: None,
            typ: typ.into(),
            ts: now_ms(),
            data,
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
