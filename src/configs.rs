use std::collections::HashMap;

use clap::Parser;
use near_indexer::near_primitives::types::Gas;
use rdkafka::config::ClientConfig;
use serde::Deserialize;

pub const NES_CONFIG_FILENAME: &str = "nes.toml";

#[derive(Parser, Debug)]
#[clap(version = "0.1", author = "Sigil Network <contact@sigilnet.com>")]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(crate) struct Opts {
    /// Sets a custom config dir. Defaults to ~/.near/
    #[clap(short, long)]
    pub home_dir: Option<std::path::PathBuf>,
    /// Enabled Indexer debug level of logs
    #[clap(long)]
    pub debug: bool,
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Parser, Debug)]
pub(crate) enum SubCommand {
    /// Run NEAR Indexer Example. Start observe the network
    Run(RunArgs),
    /// Initialize necessary configs
    Init(InitConfigArgs),
}

#[derive(Parser, Debug)]
pub(crate) struct InitConfigArgs {
    /// chain/network id (localnet, testnet, devnet, betanet)
    #[clap(short, long)]
    pub chain_id: Option<String>,
    /// Account ID for the validator key
    #[clap(long)]
    pub account_id: Option<String>,
    /// Specify private key generated from seed (TESTING ONLY)
    #[clap(long)]
    pub test_seed: Option<String>,
    /// Number of shards to initialize the chain with
    #[clap(short, long, default_value = "1")]
    pub num_shards: u64,
    /// Makes block production fast (TESTING ONLY)
    #[clap(short, long)]
    pub fast: bool,
    /// Genesis file to use when initialize testnet (including downloading)
    #[clap(short, long)]
    pub genesis: Option<String>,
    #[clap(long)]
    /// Download the verified NEAR genesis file automatically.
    pub download_genesis: bool,
    /// Specify a custom download URL for the genesis-file.
    #[clap(long)]
    pub download_genesis_url: Option<String>,
    #[clap(long)]
    /// Download the verified NEAR config file automatically.
    pub download_config: bool,
    /// Specify a custom download URL for the config file.
    #[clap(long)]
    pub download_config_url: Option<String>,
    /// Specify the boot nodes to bootstrap the network
    pub boot_nodes: Option<String>,
    /// Specify a custom max_gas_burnt_view limit.
    #[clap(long)]
    pub max_gas_burnt_view: Option<Gas>,
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct RunArgs {
    /// Force streaming while node is syncing
    #[clap(long)]
    pub stream_while_syncing: bool,
    /// Sets the starting point for indexing
    #[clap(subcommand)]
    pub sync_mode: SyncModeSubCommand,
    /// Sets the concurrency for indexing. Note: concurrency (set to 2+) may lead to warnings due to tight constraints between transactions and receipts (those will get resolved eventually, but unless it is the second pass of indexing, concurrency won't help at the moment).
    #[clap(long, default_value = "1")]
    pub concurrency: std::num::NonZeroU16,
}

impl RunArgs {
    pub(crate) fn to_indexer_config(
        &self,
        home_dir: std::path::PathBuf,
    ) -> near_indexer::IndexerConfig {
        near_indexer::IndexerConfig {
            home_dir,
            sync_mode: self.sync_mode.clone().into(),
            await_for_node_synced: if self.stream_while_syncing {
                near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing
            } else {
                near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync
            },
        }
    }
}

#[allow(clippy::enum_variant_names)] // we want commands to be more explicit
#[derive(Parser, Debug, Clone)]
pub(crate) enum SyncModeSubCommand {
    /// continue from the block Indexer was interrupted
    SyncFromInterruption,
    /// start from the newest block after node finishes syncing
    SyncFromLatest,
    /// start from specified block height
    SyncFromBlock(BlockArgs),
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct BlockArgs {
    /// block height for block sync mode
    #[clap(long)]
    pub height: u64,
}

impl From<SyncModeSubCommand> for near_indexer::SyncModeEnum {
    fn from(sync_mode: SyncModeSubCommand) -> Self {
        match sync_mode {
            SyncModeSubCommand::SyncFromInterruption => Self::FromInterruption,
            SyncModeSubCommand::SyncFromLatest => Self::LatestSynced,
            SyncModeSubCommand::SyncFromBlock(args) => Self::BlockHeight(args.height),
        }
    }
}

impl From<InitConfigArgs> for near_indexer::InitConfigArgs {
    fn from(config_args: InitConfigArgs) -> Self {
        Self {
            chain_id: config_args.chain_id,
            account_id: config_args.account_id,
            test_seed: config_args.test_seed,
            num_shards: config_args.num_shards,
            fast: config_args.fast,
            genesis: config_args.genesis,
            download_genesis: config_args.download_genesis,
            download_genesis_url: config_args.download_genesis_url,
            download_config: config_args.download_config,
            download_config_url: config_args.download_config_url,
            boot_nodes: config_args.boot_nodes,
            max_gas_burnt_view: config_args.max_gas_burnt_view,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct NesConfig {
    pub kafka: HashMap<String, String>,

    #[serde(skip)]
    pub kafka_config: ClientConfig,

    pub near_events_topic_prefix: String,

    pub whitelist_contract_ids: Vec<String>,
    pub new_topic_partitions: i32,
    pub new_topic_replication: i32,
    pub force_create_new_topic: bool,
}

impl NesConfig {
    pub fn new(home_dir: std::path::PathBuf) -> anyhow::Result<Self> {
        let conf_file = home_dir.join(NES_CONFIG_FILENAME);
        let conf = config::Config::builder()
            .add_source(config::File::from(conf_file))
            .build()?;

        let mut nes_conf = conf.try_deserialize::<Self>()?;
        nes_conf.init_kafka_config();

        Ok(nes_conf)
    }

    fn init_kafka_config(&mut self) {
        let mut kafka_conf = ClientConfig::new();
        self.kafka.iter().for_each(|(k, v)| {
            kafka_conf.set(k, v);
        });
        self.kafka_config = kafka_conf;
    }
}
