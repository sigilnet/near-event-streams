fn main() {
    let args: Vec<String> = std::env::args().collect();
    let home_dir = near_indexer::get_default_home();

    let command = args
        .get(1)
        .map(|arg| arg.as_str())
        .expect("You need to provide a command: `init` or `run` as arg");

    match command {
        "init" => {
            let config_args = near_indexer::InitConfigArgs {
                chain_id: Some("localnet".to_string()),
                account_id: None,
                test_seed: None,
                num_shards: 1,
                fast: false,
                genesis: None,
                download_genesis: false,
                download_genesis_url: None,
                download_config: false,
                download_config_url: None,
                max_gas_burnt_view: None,
                boot_nodes: None,
            };
            near_indexer::indexer_init_configs(&home_dir, config_args).unwrap();
        }
        "run" => {
            let indexer_config = near_indexer::IndexerConfig {
                home_dir: near_indexer::get_default_home(),
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
            };
            let sys = actix::System::new();
            sys.block_on(async move {
                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();
                let stream = indexer.streamer();
                actix::spawn(listen_blocks(stream));
            });
            sys.run().unwrap();
        }
        _ => panic!("You have to pass `init` or `run` arg"),
    }
}

async fn listen_blocks(mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        eprintln!("{}", serde_json::to_value(streamer_message).unwrap());
    }
}
