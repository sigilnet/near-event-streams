use std::time::Duration;

use futures::{
    stream::{self, FuturesOrdered},
    StreamExt, TryStreamExt,
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};
use tracing::{debug, info, warn};

use crate::{
    configs::NesConfig,
    event_types::{EmitInfo, EventData, NearEvent, Nep171Data},
    token::get_metadatas,
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
    event: &NearEvent,
) -> anyhow::Result<()> {
    ensure_topic(consumer, admin_client, nes_config, topic).await?;

    let payload = serde_json::to_string(event)?;
    let key = event.to_key();

    let delivery_status = producer
        .send(
            FutureRecord::to(topic).payload(&payload).key(&key),
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
    view_client: &actix::Addr<near_client::ViewClientActor>,
    nes_config: &NesConfig,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_timestamp = streamer_message.block.header.timestamp;

    debug!(target: crate::INDEXER, "Block height {}", &block_height);

    let events = streamer_message
        .shards
        .iter()
        .flat_map(|shard| collect_events(shard, block_height, block_timestamp, nes_config))
        .collect::<Vec<NearEvent>>();

    //TODO: transform to stream
    for event in events.iter() {
        let event_topic = event.to_topic(&nes_config.near_events_topic_prefix);

        let sending_to_all_topic = send_event(
            producer,
            consumer,
            admin_client,
            nes_config,
            &nes_config.near_events_all_topic,
            event,
        );

        let sending_to_specific_topic = send_event(
            producer,
            consumer,
            admin_client,
            nes_config,
            &event_topic,
            event,
        );

        let sending_event_with_metadata = send_event_with_metadata(
            producer,
            consumer,
            admin_client,
            nes_config,
            view_client,
            &event_topic,
            event,
        );

        tokio::try_join!(
            sending_to_all_topic,
            sending_to_specific_topic,
            sending_event_with_metadata
        )?;

        info!("Sent event {:?} to Kafka success", &event);
    }

    Ok(())
}

async fn send_event_with_metadata(
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    nes_config: &NesConfig,
    view_client: &actix::Addr<near_client::ViewClientActor>,
    event_topic: &str,
    event: &NearEvent,
) -> anyhow::Result<()> {
    if !nes_config.enrich_metadata {
        return Ok(());
    }

    let contract_account_id = event.emit_info.clone().map(|info| info.contract_account_id);
    if contract_account_id.is_none() {
        return Ok(());
    }
    let contract_account_id = contract_account_id.unwrap();
    let topic = format!("{}_metadata", event_topic);

    let events = stream::iter(event.try_flatten_nep171_event());

    let enriched_events = events
        .then(|event| enrich_event_metadata(view_client, event, &contract_account_id))
        .try_collect::<Vec<NearEvent>>()
        .await?;

    enriched_events
        .iter()
        .map(|event| send_event(producer, consumer, admin_client, nes_config, &topic, event))
        .collect::<FuturesOrdered<_>>()
        .try_collect::<Vec<()>>()
        .await?;

    Ok(())
}

async fn enrich_event_metadata(
    view_client: &actix::Addr<near_client::ViewClientActor>,
    event: NearEvent,
    contract_account_id: &str,
) -> anyhow::Result<NearEvent> {
    let mut enriched_data = event.data.clone();
    match enriched_data {
        EventData::Nep171(Nep171Data::MintFlat(ref mut data)) => {
            let metadatas =
                get_metadatas(view_client, contract_account_id, &data.token_ids).await?;

            data.metadatas = Some(metadatas);
        }
        EventData::Nep171(Nep171Data::TransferFlat(ref mut data)) => {
            let metadatas =
                get_metadatas(view_client, contract_account_id, &data.token_ids).await?;

            data.metadatas = Some(metadatas);
        }
        _ => {}
    }

    let mut enriched_event = event.clone();
    enriched_event.data = enriched_data;
    Ok(enriched_event)
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
