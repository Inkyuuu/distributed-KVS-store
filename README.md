# CSE 138 Assignment 4: KVS Store

This project is a distributed key-value store built with FastAPI. It supports sharding, causal consistency, gossip-based state sharing, and view changes that move keys between shards.

Most of the implementation is in [app/routes.py](app/routes.py), and the FastAPI app is created in [main.py](main.py).

## What the Project Does

The system stores key-value pairs across multiple shards instead of keeping everything on one node.

- Each key is assigned to a shard using consistent hashing.
- A client can send a request to any node.
- If that node does not own the key, it forwards the request to the correct shard.
- Writes carry causal metadata so later reads can respect causal order.
- Nodes share metadata with each other using gossip.
- When the cluster view changes, keys are redistributed to the correct shards.

## Main Distributed Systems Ideas

### Sharding

The store is split across shards so that different groups of nodes are responsible for different keys. This helps the system scale and keeps each shard responsible for only part of the data.

### Consistent Hashing

The project uses a hash ring with virtual nodes to decide which shard owns a key. This makes it easier to add or remove shards without moving every key in the system.

### Causal Consistency

The store tracks causal metadata so operations respect dependency order. If a client reads one value and then writes another value that depends on it, the system tries to preserve that relationship.

### Vector Clocks

Vector clocks are used to compare versions of data and tell whether one version happened before another or whether two versions are concurrent.

### Gossip

Nodes periodically exchange metadata with each other. This helps replicas catch up after delays or partitions and supports eventual convergence.

### Re-sharding on View Changes

When the shard view changes, the system rebuilds the hash ring and moves keys that no longer belong on the current shard.

## Important Files

- [main.py](main.py): FastAPI application setup
- [app/routes.py](app/routes.py): core logic for routing, sharding, causal metadata, gossip, and view changes
- [tests/README.md](tests/README.md): how to run the provided test framework
- [tests/kvs_test/hw4_tests/sharded.py](tests/kvs_test/hw4_tests/sharded.py): shard-related tests

## API Overview

Client-facing endpoints:

- `GET /ping`
- `PUT /view`
- `PUT /data/{key}`
- `GET /data/{key}`
- `GET /data`

Internal endpoints:

- `PUT /gossip`
- `PUT /internal/data/{key}`
- `POST /internalGet/{key}`
- `POST /internal/receive_new_shard_keys`
- `POST /giveMeStuff`

## Running the Project

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the server:

```bash
uvicorn main:app --host 0.0.0.0 --port 8080
```

Useful environment variables:

- `NODE_IDENTIFIER`
- `NODE_ADDRESS`

## Running the Tests

The provided test bench is inside [tests/](tests/).

Basic usage:

```bash
cd tests
pip install -r requirements.txt
python -m kvs_test ..
```

More details are in [tests/README.md](tests/README.md).

## Notes

- The store is in-memory only, so data is not persistent across restarts.
- The design focuses on causal consistency and eventual convergence, not strong global consistency.
- The implementation includes proxying and shard rebalancing to hide placement details from clients.
