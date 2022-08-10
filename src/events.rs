use std::time::Duration;

use futures::{stream::FuturesOrdered, TryStreamExt};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};
use tracing::{debug, info, warn};

use crate::{
    configs::NesConfig,
    event_types::{EmitInfo, NearEvent},
};

pub async fn ensure_topic(
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    nes_config: &NesConfig,
    topic: &str,
) -> anyhow::Result<()> {
    if !nes_config.force_create_new_topic {
        return Ok(());
    }
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(1));

    if let Err(err) = &metadata {
        warn!("Could not fetch Kafka metadata: {:?}", err);
        return Ok(());
    }

    let metadata = metadata.unwrap();

    let topic_names = metadata
        .topics()
        .iter()
        .map(|t| t.name())
        .collect::<Vec<&str>>();

    debug!("Kafka topics: {:?}", topic_names);

    let existed = metadata
        .topics()
        .iter()
        .any(|topic_metadata| topic_metadata.name() == topic);

    if !existed {
        let results = admin_client
            .create_topics(
                &[NewTopic::new(
                    topic,
                    nes_config.new_topic_partitions,
                    TopicReplication::Fixed(nes_config.new_topic_replication),
                )],
                &AdminOptions::new(),
            )
            .await?;

        for result in results {
            let status = result.map_err(|e| e.1);
            let status = status?;
            info!("Kafka created new topic: {:?}", status);
        }
    }

    Ok(())
}

pub async fn send_event(
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    nes_config: &NesConfig,
    topic: &str,
    key: &str,
    payload: &str,
) -> anyhow::Result<()> {
    ensure_topic(consumer, admin_client, nes_config, topic).await?;

    let delivery_status = producer
        .send(
            FutureRecord::to(topic).payload(payload).key(key),
            Duration::from_secs(0),
        )
        .await;

    let delivery_status = delivery_status.map_err(|e| e.0);
    delivery_status?;

    Ok(())
}

pub async fn store_events(
    streamer_message: &near_indexer::StreamerMessage,
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    nes_config: &NesConfig,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_timestamp = streamer_message.block.header.timestamp;

    debug!(target: crate::INDEXER, "Block height {}", &block_height);

    let generic_events = streamer_message
        .shards
        .iter()
        .flat_map(|shard| collect_events(shard, block_height, block_timestamp, nes_config))
        .collect::<Vec<NearEvent>>();

    //TODO: transform to stream
    for generic_event in generic_events.iter() {
        let event_payload = serde_json::to_string(&generic_event)?;
        let event_topic = generic_event.to_topic(&nes_config.near_events_topic_prefix);
        let event_key = generic_event.to_key();

        let sender1 = send_event(
            producer,
            consumer,
            admin_client,
            nes_config,
            &nes_config.near_events_all_topic,
            &event_key,
            &event_payload,
        );

        let sender2 = send_event(
            producer,
            consumer,
            admin_client,
            nes_config,
            &event_topic,
            &event_key,
            &event_payload,
        );

        FuturesOrdered::from_iter(vec![sender1, sender2])
            .try_collect::<Vec<()>>()
            .await?;

        info!("Sent event {} to Kafka success", &event_payload);
    }

    Ok(())
}

fn collect_events(
    shard: &near_indexer::IndexerShard,
    block_height: u64,
    block_timestamp: u64,
    nes_config: &NesConfig,
) -> Vec<NearEvent> {
    shard
        .receipt_execution_outcomes
        .iter()
        .flat_map(|outcome| extract_events(outcome, block_height, block_timestamp, shard.shard_id))
        .filter(|e| {
            if nes_config.whitelist_contract_ids.is_empty() {
                return true;
            }
            let emit_info = e.clone().emit_info.unwrap_or_default();
            nes_config
                .whitelist_contract_ids
                .contains(&emit_info.contract_account_id)
        })
        .filter(|e| {
            if nes_config.blacklist_contract_ids.is_empty() {
                return true;
            }
            let emit_info = e.clone().emit_info.unwrap_or_default();
            !nes_config
                .blacklist_contract_ids
                .contains(&emit_info.contract_account_id)
        })
        .collect::<Vec<NearEvent>>()
}

fn extract_events(
    outcome: &near_indexer::IndexerExecutionOutcomeWithReceipt,
    block_height: u64,
    block_timestamp: u64,
    shard_id: u64,
) -> Vec<NearEvent> {
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

        match serde_json::from_str::<'_, NearEvent>(
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
