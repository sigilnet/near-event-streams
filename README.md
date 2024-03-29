## Architecture

![Architecture](/docs/architecture.png?raw=true)

## Start localnet

### Prerequisites

- LocalNear <https://github.com/sigilnet/localnear.git>

### Init config

`export LOCALNEAR_PATH=<YOUR_LOCALNEAR_DIR>`

`export LOCALNET_NODE_PUBKEY=$(cat $LOCALNEAR_PATH/.near/localnet/node_key.json | jq -r ".public_key")`

`cargo run -r -- --home-dir ./.near/localnet init --chain-id localnet $LOCALNET_NODE_PUBKEY@0.0.0.0:34567`

`cp $LOCALNEAR_PATH/.near/localnet/genesis.json ./.near/localnet/genesis.json`

`rm ./.near/localnet/validator_key.json`

Configs for the specified network are in the --home-dir provided folder. We need to ensure that NEAR Indexer follows all the necessary shards, so "tracked_shards" parameters in ./.near/localnet/config.json needs to be configured properly. For example, with a single shared network, you just add the shard #0 to the list:

```
vi ./.near/localnet/config.json

...
"tracked_shards": [0],
...
"consensus": {
  "min_num_peers": 1,
  ...
}
...
```

`cp ./nes.toml.sample ./.near/localnet/nes.toml`

Change near-event-streams config:

`vi ./.near/localnet/nes.toml`

### Run
`cargo run -r -- --home-dir ./.near/localnet run --stream-while-syncing sync-from-interruption`

## Start testnet

### Init config

`cargo run -r -- --home-dir ./.near/testnet init --chain-id testnet --download-config --download-genesis`

### Run

`cargo run -r -- --home-dir ./.near/testnet run --stream-while-syncing sync-from-interruption`

## Cross-compile linux binary from macos

<https://github.com/messense/homebrew-macos-cross-toolchains>

`brew tap messense/macos-cross-toolchains`

`brew install x86_64-unknown-linux-gnu`

```
export CC_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-gcc
export CXX_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-g++
export AR_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-ar
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc
```

`cargo build-linux`

## Demo

![Demo](/docs/demo-nes.png?raw=true)
