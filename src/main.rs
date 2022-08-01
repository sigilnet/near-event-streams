use clap::Parser;
use configs::{Opts, SubCommand};
use near_indexer::{get_default_home, indexer_init_configs, Indexer};
use openssl_probe::init_ssl_cert_env_vars;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

mod configs;
mod event_types;

const INDEXER: &str = "near_event_streams";

fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    init_ssl_cert_env_vars();

    let opts: Opts = Opts::parse();

    init_tracer(&opts);

    let home_dir = opts.home_dir.unwrap_or_else(get_default_home);

    match opts.subcmd {
        SubCommand::Run(args) => {
            let indexer_config = args.to_indexer_config(home_dir);
            let system = actix::System::new();
            system.block_on(async move {
                let indexer = Indexer::new(indexer_config).expect("Indexer::new()");
                let stream = indexer.streamer();
                actix::spawn(listen_blocks(stream));
            });
            system.run()?;
        }
        SubCommand::Init(config) => indexer_init_configs(&home_dir, config.into())?,
    }

    Ok(())
}

fn init_tracer(opts: &Opts) {
    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info",
    );

    if opts.debug {
        env_filter = env_filter.add_directive(
            format!("{}=debug", INDEXER)
                .parse()
                .expect("Failed to parse directive"),
        );
    } else {
        env_filter = env_filter.add_directive(
            format!("{}=info", INDEXER)
                .parse()
                .expect("Failed to parse directive"),
        );
    };

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

async fn listen_blocks(
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
) -> anyhow::Result<()> {
    while let Some(streamer_message) = stream.recv().await {
        store_events(&streamer_message)?;
    }

    Ok(())
}

fn store_events(streamer_message: &near_indexer::StreamerMessage) -> anyhow::Result<()> {
    debug!(
        target: INDEXER,
        "Block height {}", &streamer_message.block.header.height
    );

    streamer_message.shards.iter().for_each(|shard| {
        for outcome in &shard.receipt_execution_outcomes {
            let events = extract_events(outcome);
            for event in events {
                info!(target: INDEXER, "Event: {:?}", event,)
                //TODO: store events to kafka
            }
        }
    });

    Ok(())
}

fn extract_events(
    outcome: &near_indexer::IndexerExecutionOutcomeWithReceipt,
) -> Vec<event_types::NearEvent> {
    let prefix = "EVENT_JSON:";
    outcome.execution_outcome.outcome.logs.iter().filter_map(|untrimmed_log| {
        let log = untrimmed_log.trim();
        if !log.starts_with(prefix) {
            return None;
        }

        match serde_json::from_str::<'_, event_types::NearEvent>(
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
    }).collect()
}
