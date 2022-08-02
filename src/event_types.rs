use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct GenericEvent {
    pub standard: String,
    pub version: String,
    pub event: String,
    pub data: serde_json::Value,
    #[serde(skip_deserializing)]
    pub emit_info: Option<EmitInfo>,
}

impl GenericEvent {
    pub fn to_key(&self) -> String {
        format!("{}:{}", self.standard, self.event)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct EmitInfo {
    pub receipt_id: String,
    pub block_timestamp: u64,
    pub block_height: u64,
    pub shard_id: u64,
    pub contract_account_id: String,
}
