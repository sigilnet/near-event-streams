use serde::{Deserialize, Serialize};

use crate::token::TokenMetadata;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct NearEvent {
    pub standard: String,
    pub version: String,
    pub event: String,
    pub data: EventData,
    pub emit_info: Option<EmitInfo>,
}

impl NearEvent {
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

    pub fn try_flatten_nep171_event(&self) -> Vec<NearEvent> {
        match &self.data {
            EventData::Nep171(data) => match data {
                Nep171Data::Mint(data) => data
                    .iter()
                    .map(|d| {
                        let mut flat_event = self.clone();
                        flat_event.data = EventData::Nep171(Nep171Data::MintFlat(d.clone()));
                        flat_event
                    })
                    .collect(),
                Nep171Data::Transfer(data) => data
                    .iter()
                    .map(|d| {
                        let mut flat_event = self.clone();
                        flat_event.data = EventData::Nep171(Nep171Data::TransferFlat(d.clone()));
                        flat_event
                    })
                    .collect(),
                _ => vec![],
            },
            EventData::Generic(_) => vec![],
        }
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum EventData {
    Nep171(Nep171Data),
    Generic(serde_json::Value),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Nep171Data {
    Mint(Vec<Nep171MintData>),
    Transfer(Vec<Nep171TransferData>),
    MintFlat(Nep171MintData),
    TransferFlat(Nep171TransferData),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Nep171MintData {
    pub owner_id: String,
    pub token_ids: Vec<String>,
    pub memo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadatas: Option<Vec<Option<TokenMetadata>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_extras: Option<Vec<Option<serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _ids: Option<Vec<Option<String>>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Nep171TransferData {
    pub authorized_id: Option<String>,
    pub old_owner_id: String,
    pub new_owner_id: String,
    pub token_ids: Vec<String>,
    pub memo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadatas: Option<Vec<Option<TokenMetadata>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_extras: Option<Vec<Option<serde_json::Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _ids: Option<Vec<Option<String>>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialized() {
        let json = r#"{"standard":"nep171","version":"1.0.0","event":"nft_mint","data":[{"owner_id":"sigilnet.testnet","token_ids":["1:1", "1:2"]}]}"#;
        let event: NearEvent = serde_json::from_str(json).unwrap();
        let flat_events = event.try_flatten_nep171_event();
        println!("flatten events: {:?}", &flat_events);
    }
}
