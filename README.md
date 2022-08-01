## Architecture

![Architecture](/docs/architecture.png?raw=true)

## Start localnet

### LocalNear

<https://github.com/sigilnet/localnear.git>

### Init config

`cargo run -- --home-dir ~/.near/localnet init`

Configs for the specified network are in the --home-dir provided folder. We need to ensure that NEAR Indexer follows all the necessary shards, so "tracked_shards" parameters in ~/.near/localnet/config.json needs to be configured properly. For example, with a single shared network, you just add the shard #0 to the list:

```
...
"tracked_shards": [0],
...
```

### Run localnet
`cargo run -- --home-dir ~/.near/localnet run`
