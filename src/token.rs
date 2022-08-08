use near_indexer::near_primitives::{
    types::{BlockReference, Finality, FunctionArgs},
    views::{QueryRequest, QueryResponseKind},
};
use serde_json::{from_slice, json};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Token {
    pub _id: Option<String>,
    pub token_id: String,
    pub owner_id: String,
    pub metadata: Option<TokenMetadata>,
    pub metadata_extra: Option<serde_json::Value>,
    pub approved_account_ids: Option<HashMap<String, u64>>,
    pub contract_account_id: Option<String>,
}

impl Token {
    #[allow(dead_code)]
    pub fn build_id(contract_id: &str, token_id: &str) -> String {
        format!("{}:{}", contract_id, token_id)
    }

    #[allow(dead_code)]
    pub fn derive_id(&self) -> Option<String> {
        self.contract_account_id
            .clone()
            .map(|contract_id| Self::build_id(&contract_id, &self.token_id))
    }

    #[allow(dead_code)]
    pub fn set_id(&mut self) {
        self._id = self.derive_id();
    }

    #[allow(dead_code)]
    pub fn get_id(&self) -> Option<String> {
        if self._id.is_some() {
            return self._id.clone();
        }

        self.derive_id()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TokenMetadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub media: Option<String>,
    pub media_hash: Option<String>,
    pub copies: Option<u64>,
    pub issued_at: Option<String>,
    pub expires_at: Option<String>,
    pub starts_at: Option<String>,
    pub updated_at: Option<String>,
    pub extra: Option<String>,
    pub reference: Option<String>,
    pub reference_hash: Option<String>,
    pub collection_id: Option<String>,
}

#[allow(dead_code)]
pub async fn get_nft_token(
    client: &actix::Addr<near_client::ViewClientActor>,
    contract_id: &str,
    token_id: &str,
) -> anyhow::Result<Option<Token>> {
    let request = near_client::Query {
        query_id: String::from("TODO:query_id"),
        block_reference: BlockReference::Finality(Finality::Final),
        request: {
            QueryRequest::CallFunction {
                account_id: contract_id.parse()?,
                method_name: "nft_token".to_string(),
                args: FunctionArgs::from(
                    json!({
                        "token_id": token_id,
                    })
                    .to_string()
                    .into_bytes(),
                ),
            }
        },
    };

    let response = client.send(request).await?;

    match response {
        Ok(response) => {
            if let QueryResponseKind::CallResult(result) = response.kind {
                let token = from_slice::<Token>(&result.result);
                match token {
                    Ok(token) => return Ok(Some(token)),
                    Err(_) => return Ok(None),
                }
            }

            Ok(None)
        }
        Err(err) => {
            tracing::error!(
                "get_nft_token unhandled error: {}, {}, {:?}",
                contract_id,
                token_id,
                err
            );
            Ok(None)
        }
    }
}
