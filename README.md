## Architecture

![Architecture](/docs/architecture.png?raw=true)

## Start localnet

### LocalNear

<https://github.com/sigilnet/localnear.git>

### Init config

`export LOCALNET_NEAR_PATH=<YOUR_LOCALNEAR_DIR>`
`export LOCALNET_NODE_PUBKEY=<YOUR_LOCALNEAR_NODE_PUBKEY>`

`cargo run -- --home-dir ./.near/localnet init --chain-id localnet $LOCALNET_NODE_PUBKEY@0.0.0.0:34567`

`cp $LOCALNET_NEAR_PATH/.near-indexer/localnet/genesis.json ./.near/localnet/genesis.json`

Configs for the specified network are in the --home-dir provided folder. We need to ensure that NEAR Indexer follows all the necessary shards, so "tracked_shards" parameters in ./.near/localnet/config.json needs to be configured properly. For example, with a single shared network, you just add the shard #0 to the list:

```
...
"tracked_shards": [0],
...
```

### Run localnet
`cargo run -- --home-dir ./.near/localnet --debug run --stream-while-syncing sync-from-interruption`

## Start testnet
`cargo run -- --home-dir ./.near/testnet init --chain-id testnet --download-config --download-genesis`
