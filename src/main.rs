use clap::Parser;
use configs::{NesConfig, Opts, SubCommand};
use events::store_events;
use near_indexer::{get_default_home, indexer_init_configs, Indexer};
use openssl_probe::init_ssl_cert_env_vars;
use rdkafka::producer::FutureProducer;
use tracing_subscriber::EnvFilter;

mod configs;
mod event_types;
mod events;

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
            let producer: FutureProducer = nes_config.kafka_config.create()?;

            let system = actix::System::new();
            system.block_on(async move {
                let indexer = Indexer::new(indexer_config).expect("Indexer::new()");
                let stream = indexer.streamer();

                actix::spawn(listen_blocks(stream, producer, nes_config))
                    .await
                    .unwrap()
                    .expect("listen_blocks error");
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
    mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    producer: FutureProducer,
    nes_config: NesConfig,
) -> anyhow::Result<()> {
    while let Some(streamer_message) = stream.recv().await {
        store_events(&streamer_message, &producer, &nes_config).await?;
    }

    Ok(())
}
