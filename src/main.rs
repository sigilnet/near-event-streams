use clap::Parser;
use configs::{Opts, SubCommand};
use near_indexer::{
    get_default_home, indexer_init_configs, AwaitForNodeSyncedEnum, Indexer, IndexerConfig,
    SyncModeEnum,
};
use near_o11y::{default_subscriber, tracing_subscriber::EnvFilter};
use openssl_probe::init_ssl_cert_env_vars;

mod configs;

fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    init_ssl_cert_env_vars();
    let env_filter = EnvFilter::new(
        "nearcore=info,indexer_example=info,tokio_reactor=info,near=info,\
         stats=info,telemetry=info,indexer=info,near-performance-metrics=info",
    );
    let runtime = tokio::runtime::Runtime::new()?;
    let _subscriber = runtime.block_on(async {
        default_subscriber(env_filter, &Default::default())
            .await
            .global();
    });

    let opts: Opts = Opts::parse();

    let home_dir = opts.home_dir.unwrap_or_else(get_default_home);

    match opts.subcmd {
        SubCommand::Run => {
            let indexer_config = IndexerConfig {
                home_dir,
                sync_mode: SyncModeEnum::FromInterruption,
                await_for_node_synced: AwaitForNodeSyncedEnum::WaitForFullSync,
            };
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

async fn listen_blocks(mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        eprintln!("{:#?}", serde_json::to_value(streamer_message).unwrap());
    }
}
