use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct NearEvent<'a> {
    standard: &'a str,
    version: &'a str,
    event: &'a str,
    data: serde_json::Value,
}
