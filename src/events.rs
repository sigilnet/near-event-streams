use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{debug, info, warn};

use crate::{
    configs::NesConfig,
    event_types::{EmitInfo, GenericEvent},
};

pub async fn store_events(
    streamer_message: &near_indexer::StreamerMessage,
    producer: &FutureProducer,
    nes_config: &NesConfig,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_timestamp = streamer_message.block.header.timestamp;

    debug!(target: crate::INDEXER, "Block height {}", &block_height);

    let generic_events = streamer_message
        .shards
        .iter()
        .flat_map(|shard| collect_events(shard, block_height, block_timestamp))
        .filter(|e| {
            if nes_config.whitelist_contract_ids.is_empty() {
                return true;
            }
            let emit_info = e.clone().emit_info.unwrap_or_default();
            nes_config
                .whitelist_contract_ids
                .contains(&emit_info.contract_account_id)
        })
        .collect::<Vec<GenericEvent>>();

    for generic_event in generic_events.iter() {
        let event_payload = serde_json::to_string(&generic_event)?;
        let delivery_status = producer
            .send(
                FutureRecord::to(&generic_event.to_topic(&nes_config.near_events_topic_prefix))
                    .payload(&event_payload)
                    .key(&generic_event.to_key()),
                Duration::from_secs(0),
            )
            .await;

        let delivery_status = delivery_status.map_err(|e| e.0);
        let (partition, offset) = delivery_status?;
        info!(
            "Sent event {:?} to Kafka success at partition: {}, offset: {}",
            generic_event, partition, offset
        );
    }

    Ok(())
}

fn collect_events(
    shard: &near_indexer::IndexerShard,
    block_height: u64,
    block_timestamp: u64,
) -> Vec<GenericEvent> {
    shard
        .receipt_execution_outcomes
        .iter()
        .flat_map(|outcome| extract_events(outcome, block_height, block_timestamp, shard.shard_id))
        .collect::<Vec<GenericEvent>>()
}

fn extract_events(
    outcome: &near_indexer::IndexerExecutionOutcomeWithReceipt,
    block_height: u64,
    block_timestamp: u64,
    shard_id: u64,
) -> Vec<GenericEvent> {
    let prefix = "EVENT_JSON:";
    let emit_info = EmitInfo {
        block_timestamp,
        block_height,
        shard_id,
        receipt_id: outcome.receipt.receipt_id.to_string(),
        contract_account_id: outcome.receipt.receiver_id.to_string(),
    };

    outcome.execution_outcome.outcome.logs.iter().filter_map(|untrimmed_log| {
        let log = untrimmed_log.trim();
        if !log.starts_with(prefix) {
            return None;
        }

        match serde_json::from_str::<'_, GenericEvent>(
            log[prefix.len()..].trim(),
        ) {
            Ok(result) => Some(result),
            Err(err) => {
                warn!(
                    target: crate::INDEXER,
                    "Provided event log does not correspond to any of formats defined in NEP. Will ignore this event. \n {:#?} \n{:#?}",
                    err,
                    untrimmed_log,
                );
                None
            }
        }
    }).map(|mut e| {
        e.emit_info = Some(emit_info.clone());
        e
    }).collect()
}
