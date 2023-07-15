# reth-indexer

reth-indexer reads directly from the reth db and indexes the data into a postgres database all decoded with a simple config file and no extra setup alongside exposing a API ready to query the data.

<img src="./assets/demo.gif" />

## Disclaimer

This is an R&D project and most likely has missing features and bugs. Also most likely plenty of optimistations we can do in rust land. PRs more then welcome to make this even faster and better.

## Why

If you want to get data from the chain you tend to have to use a provider like infura or alchemy, it can get expensive with their usage plans if you are trying to just get event data. On top of that pulling huge amount of data fast is not possible freely. Over the wire JSONRPC calls adds a lot of overhead and are slow. You have the TLS handshake, you may be calling a API which is located the other side of the world to you, it adds TCP connections to your backend and scaling this is not easy mainly because of how over the wire JSONRPC calls work and with that your bill increases with your provider.

If you wish to build a big data lake or even just fetch dynamic events from the chain this task is near impossible without a third party paid tool or something like thegraph hoster services and most do not let you pull in millions of rows at a time fast. This data should be able to be fetched for free, blazing fast and customisable to your needs. This is what reth-indexer does.

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
- Created on your own infurstructure so you can scale it as you wish
- Exposes a ready to go API for you to query the data

## Benchmarks

Very hard to benchmark as it is all down to the block range and how often your event is emitted but roughly: (most likely could be speed up with more some optimisations.)

- indexes around 30,000 events a second (depending on how far away the events are in each block)
- scans around 10,000 blocks which have no events within 400ms using blooms

### Head to head

Only compared what I can right now but happy for others to do head to head

providers:

- The Graph Hosted (Substreams) - note we are not comparing legacy as it is 100x slower then substream so you can do the maths on legacy

| Task                                                                                                                                                                                                                               | The Graph Hosted (Substreams)                                                                    | reth Indexer | reth % faster |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ------------ | ------------- |
| indexing reth (rocket pool eth) transfer/approval events both starting on block 11446767 and finishing on block 17576926<br/>config to run yourself [here](./benchmark-config/reth-transfer-event-config/reth-indexer-config.json) | 19.5 hours <br/> https://thegraph.com/hosted-service/subgraph/data-nexus/reth-substreams-mainnet | 15.9 minutes | 73.5x faster  |

### Note on benchmarking

We should compare this tool to other resync tools which go from block N not one which is already resynced. If you have the resynced information already then the results will always be faster as the data is already indexed. This goes block by block scanning each blockm for information so the bigger block ranges you have of course the longer it takes to process. How fast it is depeneds on how many events are present in the blocks.

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

## How to use

### Syncing

- git clone this repo on your box - `git clone https://github.com/joshstevens19/reth-indexer.git`
- create a `reth-indexer-config.json` in the root of the project an example of the structure is in `reth-indexer-config-example.json`, you can use `cp reth-indexer-config-example.json reth-indexer-config.json` to create the file with the template.
- map your config file (we going through what else property means below)
- run `RUSTFLAGS="-C target-cpu=native" cargo run --profile maxperf --features jemalloc` to run the indexer
- see all the data get synced to your postgres database

#### Advise

reth-indexer goes block by block this means if you put block 0 to an end block it will have to check all the blocks - it does use blooms so its very fast at knowing if a block have nothing we need, but if the contract was not deployed till block x then its pointless use of resources, put in the block number the contract was deployed at as the from block number if you wanted all the events for that contract. Of course you should use the from and to block number as you wish but this is just a tip.

### API

You can also run an basic API alongside this which exposes a REST API for you to query the data. This is not meant to be a full blown API but just a simple way to query the data if you wish. This is not required to run the syncing logic. Alongside you can resync data and then load the API up to query the data.

- you need the same mapping as what you synced as that is the source of truth
- run `RUSTFLAGS="-C target-cpu=native" API=true cargo run --profile maxperf --features jemalloc` to run the api
- it will expose an endpoints on `127.0.0.1:3030/api/`
  - The rest structure is the name of ABI event name you are calling so:
  ```json
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
  ```
  - if you wanted data from this event you query `localhost:3030/api/transfer` and it will return all the data for that event.
- you can use `limit` to define the amount you want brought back - `localhost3030/api/transfer?limit=100`
- you can use `offset` to page the results - `localhost:3030/api/transfer?limit=100&offset=100`
- you can use `latest` to order the results in ASC(false) or DESC(true) order - `localhost:3030/api/transfer?latest=false` - defaults to true
- the result of the rest API are dependant on the event ABI you have supplied so it always includes the fields in the ABI input and then the additional fields of `blockNumber`, `txHash`, `blockHash`, `contractAddress`, `indexedId`.

```json
{
  "events:": [
    {
      "from": "0x8d263F61D0F67A75868F831D83Ef51F24d10A003",
      "to": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
      "value": "1020203030",
      "blockNumber": 13578900,
      "indexedId": "aae863fb-2d13-4da5-9db7-55707ae93d8a",
      "contractAddress": "0xae78736cd615f374d3085123a210448e74fc6393",
      "txHash": "0xb4702508ef5170cecf95ca82cb3465278fc2ef212eadd08c60498264a216f378",
      "blockHash": "0x8f493854e6d10e4fdd2b5b0d42834d331caa80ad739225e2feb1b89cb9a1dd3c"
    }
  ],
  "pagingInfo": {
    "next": "127.0.0.1:3030/api/transfer?limit=100&offset=200",
    "previous": "127.0.0.1:3030/api/transfer?limit=100&offset=100" // < this will be null if no previous page
  }
}
```

A note numbers are stored as numeric in the database but when brought back they will be strings due to overflows and limits in other languages.

#### Searching

The api allows you to filter on every element on the data with query string parameters for example if i wanted to filter on the `from` and `to` address i would do:

`curl "127.0.0.1:3030/api/transfer?from=0x8d263F61D0F67A75868F831D83Ef51F24d10A003&to=0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"` - remember the quotes around the endpoint if using curl as it will only bring in certain query string parameters if you dont.

you can mix and match with ANY fields that you want including the common fields no limit with the amount of fields either. Bare in mind the postgres database automatically creates an index for the fields which are marked as "indexed" true on the ABI so if you filter on those fields it will be very fast, if you filter on a field which is not indexed it will not be as fast, you can of course add your index in, will create a ticket to allow you to pass in a custom index for the future

## Config file

you can see an example config [here](./reth-indexer-config-example.json) but its important to read the below config options as it has many different features which can be enabled by the config setup.

### rethDBLocation - required

The location of the reth node db on the box.

example: `"rethDBLocation": "/home/ubuntu/.local/share/reth/mainnet/db",`

### csvLocation - required

The location the application uses to write temp csvs file, the folder needs to be able to be read by the user running the program, alongside the postgres user must be able to read it. On ubuntu using `/tmp/` is the best option.

example: `"csvLocation": "/tmp/",`

### fromBlockNumber - required

The block number to start indexing from.

example: `"fromBlockNumber": 17569693,`

### toBlockNumber - optional

The block number to stop indexing at, if you want to a live indexer leave it blank and it will index all the data and once caught up to the head sync live.

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

#### applyIndexesBeforeSync - optional - default false

Added more indexes to a table speeds up the queries but that doesn't come without a cost, writing to tables with loads of indexes is slower than writing to one without. Without indexes a database with a lot of data will be very slow to query, indexes make the DB fast. This flag allows you to toggle if you wish the indexes to be on from the moment it syncs meaning the queries will be fast straight away as syncing, this may be useful if you have a lot of data to sync and you want to query it as it syncs (this will be slower to resync). If you do not mind waiting for the data to resync then you can leave this off and it will apply the indexes after the sync is complete (this will be faster to resync but data slower to query until its complete), note this doesn't matter once it reaches the head as it will be syncing live with all indexes applied.

example: `"applyIndexesBeforeSync": true,`

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

##### setting custom db indexes - (customDbIndexes) - optional

Note before reading its worth noting about `applyIndexesBeforeSync` setting under `postgres` this flag is important to understand what it means when it is on, having indexes as it writes bulk data slows down the write speed so you need to toggle that setting depending on your need.

If you want the database to be fast you will want to add some custom indexes based on how you wish to query this data. reth-indexer already added indexes for all the `indexed` ABI input fields but you can add more if you wish to query on other fields. A DB can only be as fast as its indexes so if your looking for API speed on querying or just general querying speed up this is the optimisation you want to do.

In the example below this config is indexing all of the uniswap `swap` events and adding a custom index on the `amount1In` field and the `amount0In` this is composite index meaning it is based on multiple columns. Now when you try to query `amount1In` and `amount0In` together in a `WHERE` clause it will be fast. You can also just add single fields in the `customDbIndexes`, so if you look below the on-chain field `amount1Out` is not indexed so an index will not be automatically created but without an index it will be very slow so just add it in the array of `customDbIndexes`.

```JSON
  "decodeAbiItems": [
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "address",
          "name": "sender",
          "type": "address"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount0In",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount1In",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount0Out",
          "type": "uint256"
        },
        {
          "indexed": false,
          "internalType": "uint256",
          "name": "amount1Out",
          "type": "uint256"
        },
        {
          "indexed": true,
          "internalType": "address",
          "name": "to",
          "type": "address"
        }
      ],
      "name": "Swap",
      "type": "event",
      "customDbIndexes": [
        [
          "amount1In",
          "amount0In"
        ],
        [
          "amount1Out"
        ]
      ]
    }
  ]
```

Note: `customDbIndexes` is a `Option<Vec<Vec<String>>>` so you can add as many indexes as you like, remember each item in the `Vec` is a SINGLE index so if you put:

```JSON
"customDbIndexes": [
  [
    "amount1In",
    "amount0In"
  ],
  [
    "amount1Out"
  ]
]
```

this will create 2 indexes:

1. `CREATE INDEX IF NOT EXISTS swap_idx__amount1in_amount0in ON swap("amount1in", "amount0in");`
2. `CREATE INDEX IF NOT EXISTS swap_idx__amount1out ON swap("amount1out");`

##### custom regex per input type - (rethRegexMatch) - optional

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
