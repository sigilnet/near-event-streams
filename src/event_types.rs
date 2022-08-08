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
        if let Some(emit_info) = &self.emit_info {
            return emit_info.contract_account_id.clone();
        }
        self.default_key()
    }

    pub fn default_key(&self) -> String {
        format!("{}.{}", self.standard, self.event)
    }

    pub fn to_topic(&self, prefix: &str) -> String {
        format!("{}.{}", prefix, &self.default_key())
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
