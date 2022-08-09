use std::sync::Arc;

use clap::Parser;
use configs::{NesConfig, Opts, SubCommand};
use events::store_events;
use futures::StreamExt;
use near_indexer::{get_default_home, indexer_init_configs, Indexer};
use openssl_probe::init_ssl_cert_env_vars;
use rdkafka::{
    admin::AdminClient, client::DefaultClientContext, consumer::StreamConsumer,
    producer::FutureProducer,
};
use stats::{stats_logger, Stats};
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

mod configs;
mod event_types;
mod events;
mod stats;
mod token;

pub const INDEXER: &str = "near_event_streams";

fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    init_ssl_cert_env_vars();

    let opts: Opts = Opts::parse();

    init_tracer(&opts);

    let home_dir = opts.home_dir.unwrap_or_else(get_default_home);

    match opts.subcmd {
        SubCommand::Run(args) => {
            let indexer_config = args.to_indexer_config(home_dir.clone());
            let nes_config = NesConfig::new(home_dir)?;

            let system = actix::System::new();
            system.block_on(async move {
                let indexer = Indexer::new(indexer_config).expect("Indexer::new()");
                let stream = indexer.streamer();
                let view_client = indexer.client_actors().0;

                if nes_config.stats_enabled {
                    let stats: Arc<Mutex<Stats>> = Arc::new(Mutex::new(Stats::new()));
                    actix::spawn(stats_logger(Arc::clone(&stats), view_client));
                }

                listen_blocks(stream, args.concurrency, nes_config)
                    .await
                    .expect("Exitting...");

                actix::System::current().stop();
            });
            system.run()?;
        }
        SubCommand::Init(config) => indexer_init_configs(&home_dir, config.into())?,
    }

    Ok(())
}

fn init_tracer(opts: &Opts) {
    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,stats=info,telemetry=info,indexer=info,aggregated=info,near_chain::doomslug=warn",
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
    stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    concurrency: std::num::NonZeroU16,
    nes_config: NesConfig,
) -> anyhow::Result<()> {
    let producer: FutureProducer = nes_config.kafka_config.create()?;
    let consumer: StreamConsumer = nes_config.kafka_config.create()?;
    let admin_client: AdminClient<DefaultClientContext> = nes_config.kafka_config.create()?;

    let mut handle_messages = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_message(
                streamer_message,
                &producer,
                &consumer,
                &admin_client,
                &nes_config,
            )
        })
        .buffer_unordered(usize::from(concurrency.get()));

    while let Some(handle_message) = handle_messages.next().await {
        handle_message?;
    }

    Ok(())
}

async fn handle_message(
    streamer_message: near_indexer::StreamerMessage,
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    nes_config: &NesConfig,
) -> anyhow::Result<()> {
    store_events(
        &streamer_message,
        producer,
        consumer,
        admin_client,
        nes_config,
    )
    .await?;

    Ok(())
}
