# reth-indexer

reth-indexer reads directly from the reth db and indexes the data into a postgres database with a simple config file and no extra setup.

<img src="./assets/demo.gif" />

## Disclaimer

This is an R&D project and most likely has missing features and bugs. Also most likely plenty of optimistations we can do in rust land. PRs more then welcome to make this even faster and better.

## Why

If you want to get data from the chain you tend to have to use a provider like infura or alchemy, it can get expensive with their usage plans if you are trying to just get event data. On top of that pulling huge amount of data fast is not possible freely. Over the wire JSONRPC calls adds a lot of overhead and are slow. You have the TLS handshake, you may be calling a API which is located the other side of the world to you, it adds TCP connections to your backend and scaling this is not easy mainly because of how over the wire JSONRPC calls work and with that your bill increases with your provider.

If you wish to build a big data lake or even just fetch dynamic events from the chain this task is near impossible without a third party paid tool and most do not let you pull in millions of rows at a time. This data should be able to be fetched for free, fast and customisable to your needs. This is what reth-indexer does.

This project aims to solve this by reading directly from the reth node db and indexing the data into a postgres database you can then query fast with indexes already applied for you. You can also scale this easily by running multiple instances of this program on different boxes and pointing them at the same postgres database (we should build that in the tool directly).

This tool is perfect for all kinds of people from developers, to data anaylsis, to ML developers to anyone who wants to just get a snapshot of event data and use it in a production app or just for their own reports.

## Features

- Creates postgres tables for you automatically
- Creates indexes on the tables for you automatically to allow you to query the data fast
- Indexes any events from the reth node db
- Supports indexing any events from multiple contracts or all contracts at the same time
- Supports filtering even on the single input types so allowing you to filter on every element of the event
- Snapshot between from and to block numbers
- No code required it is all driven by a json config file that is easy to edit and understand

## Benchmarks

Very hard to benchmark as it is all down to the block range and how often your event is emitted but roughly: (most likely could be speed up with more some optimisations.)

- indexes around 29,000 events a second (depending on how far away the events are in each block)
- scans around 10,000 blocks which have no events within 580ms
- scans around 10,000 blocks for all contract events within 13 seconds

## Indexes

This right now it is just focusing on indexing events it does not currently index ethereum transactions or blocks, that said it would not be hard to add this functionality on top of the base logic now built - PR welcome.

- [] Indexes blocks
- [] Indexes transactions
- [] Indexes eth transfers
- [x] Indexes and decodes event logs

## Requirements

- This must be ran on the same box and the reth node is running
- You must have a postgres database running on the box

## How it works

reth-indexer goes block by block using reth db directly searching for any events that match the event mappings you have supplied in the config file. It then writes the data to a csv file and then bulk copies the data into the postgres database. It uses blooms to disregard blocks it does not need to care about. It uses CSVs and the postgres `COPY` syntax as that can write thousands of records a second bypassing some of the processing and logging overhead associated with individual `INSERT` statements, when you are dealing with big data this is a really nice optimisation.

### Bottlenecks

We should compare this tool to other resync tools not one which has already resynced. If you have the resynced information already then the response will always be faster as the data is already indexed. This goes block by block scanning each one so bigger block ranges may take longer then smaller ones. How fast it is depeneds on how many events are present in the blocks.

## How to use

- git clone this repo on your box - `git clone https://github.com/joshstevens19/reth-indexer.git`
- create a `reth-indexer-config.json` in the root of the project an example of the structure is in `reth-indexer-config-example.json`, you can use `cp reth-indexer-config-example.json reth-indexer-config.json` to create the file with the template.
- map your config file (we going through what else property means below)
- run `RUSTFLAGS="-C target-cpu=native" cargo run --profile maxperf --features  jemalloc` to run the indexer
- see all the data get synced to your postgres database

### Advise

reth-indexer goes block by block this means if you put block 0 to an end block it will have to check all the blocks - it does use blooms so its very fast at knowing if a block have nothing we need, but if the contract was not deployed till block x then its pointless use of resources, put in the block number the contract was deployed at as the from block number if you wanted all the events for that contract. Of course you should use the from and to block number as you wish but this is just a tip.

## Config file

### rethDBLocation - required

The location of the reth node db on the box.

example: `"rethDBLocation": "/home/ubuntu/.local/share/reth/mainnet/db",`

### csvLocation - required

The location the application uses to write temp csvs file, the folder needs to be able to be read by the user running the program, alongside the postgres user must be able to read it. On ubuntu using `/tmp/` is the best option.

example: `"csvLocation": "/tmp/",`

### fromBlockNumber - required

The block number to start indexing from.

example: `"fromBlockNumber": 17569693,`

### toBlockNumber - required

The block number to stop indexing at, if you want to index up to the latest reth block that it is synced to do not supply.

example: `"toBlockNumber": 17569794,`

### postgres - required

Holds the postgres connection and settings info

example:

```json
"postgres": {
  "dropTableBeforeSync": true,
  "connectionString": "postgresql://postgres:password@localhost:5432/reth_indexer"
}
```

#### dropTableBeforeSync - required

If you want to drop the table before syncing the data to it, this is useful if you want to reindex the data. The tables are auto created for you everytime. Advised you have it on or you could get duplicate data.

example: `"dropTableBeforeSync": true,`

#### connectionString - required

The connection string to connect to the postgres database.

example: `"connectionString": "postgresql://postgres:password@localhost:5432/reth_indexer"`

### eventMappings

An array of event mappings that you want to index, each mapping will create a table in the database and index the events from the reth node db. You can index data based on an contract address or if you do not supply a contract address it will index all events from all contracts for that event.

#### filterByContractAddress - optional

The contract addresses you want to only index events from

example: `"filterByContractAddress": ["0xdAC17F958D2ee523a2206206994597C13D831ec7", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],`

#### syncBackRoughlyEveryNLogs

How often you want to sync the data to postgres, it uses rough math on KB size per row to work out when to sync the data to postgres, the smaller you set this more often it will write to postgres, the bigger you set it the less often it will write to postgres. If you are syncing millions of rows or do not care to see it update as fast in the database its best to go for a bigger range like 20,000+ - Roughly 20,000 is 7KB of data.

This config is set per each input so it allow you in a mappings to define for example transfer events to sync back on a bigger range then something else you want to see more often in the db.

#### decodeAbiItems

An array of ABI objects for the events you want to decode the logs for, you only need the events ABI object you care about, you can paste as many as you like in here.

example:

```json
 "decodeAbiItems": [
        {
          "anonymous": false,
          "inputs": [
            {
              "indexed": true,
              "internalType": "address",
              "name": "owner",
              "type": "address"
            },
            {
              "indexed": true,
              "internalType": "address",
              "name": "spender",
              "type": "address"
            },
            {
              "indexed": false,
              "internalType": "uint256",
              "name": "value",
              "type": "uint256"
            }
          ],
          "name": "Approval",
          "type": "event"
        },
        {
          "anonymous": false,
          "inputs": [
            {
              "indexed": true,
              "internalType": "address",
              "name": "from",
              "type": "address"
            },
            {
              "indexed": true,
              "internalType": "address",
              "name": "to",
              "type": "address"
            },
            {
              "indexed": false,
              "internalType": "uint256",
              "name": "value",
              "type": "uint256"
            }
          ],
          "name": "Transfer",
          "type": "event"
        }
      ]
```

##### custom regex per input type - (rethRegexMatch)

You can also apply a custom regex on the input type to filter down what you care about more, this is useful if you only care about a certain address or a certain token id or a value over x - anything you wish to filter on which has the same events. This allows you to sync in every direction you wish with unlimited filters on it.

example below is saying i want all the transfer events from all contracts if the `from` is `0x545a25cBbCB63A5b6b65AF7896172629CA763645` or `0x60D5B4d6Df0812b4493335E7c2209f028F3d19eb`. You can see how powerful the `rethRegexMatch` is. It supports regex so this means you can do any filtering, it is NOT case sensitive.

```json
  "eventMappings": [
    {
      "decodeAbiItems": [
        {
          "anonymous": false,
          "inputs": [
            {
              "indexed": true,
              "internalType": "address",
              "name": "from",
              "type": "address",
              "rethRegexMatch": "^(0x545a25cBbCB63A5b6b65AF7896172629CA763645|0x60D5B4d6Df0812b4493335E7c2209f028F3d19eb)$"
            },
            {
              "indexed": true,
              "internalType": "address",
              "name": "to",
              "type": "address"
            },
            {
              "indexed": false,
              "internalType": "uint256",
              "name": "value",
              "type": "uint256"
            }
          ],
          "name": "Transfer",
          "type": "event"
        }
      ]
    }
  ]
```
