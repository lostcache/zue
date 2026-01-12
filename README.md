# Zue

A distributed, replicated log-structured storage engine written in Zig.

## Architecture

```
                        ┌────────────┐
                        │   Client   │
                        └─────┬──────┘
                              │
            Append (writes)   │   Read (any node)
            ──────────────────┼──────────────────
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         CLUSTER                             │
│                                                             │
│   ┌──────────┐    ReplicateRequest    ┌──────────┐         │
│   │  Leader  │ ─────────────────────▶ │ Follower │         │
│   │  :9001   │ ◀───────────────────── │  :9002   │         │
│   │          │    ReplicateResponse   │          │         │
│   │   Log    │                        │   Log    │         │
│   │ ┌──────┐ │         Heartbeat      │ ┌──────┐ │         │
│   │ │.log  │ │ ─────────────────────▶ │ │.log  │ │         │
│   │ │.index│ │                        │ │.index│ │         │
│   │ └──────┘ │                        │ └──────┘ │         │
│   └──────────┘                        └──────────┘         │
│        │                                                    │
│        │              ReplicateRequest    ┌──────────┐     │
│        └────────────────────────────────▶ │ Follower │     │
│                                           │  :9003   │     │
│                                           │   Log    │     │
│                                           │ ┌──────┐ │     │
│                                           │ │.log  │ │     │
│                                           │ │.index│ │     │
│                                           │ └──────┘ │     │
│                                           └──────────┘     │
└─────────────────────────────────────────────────────────────┘

Write: Client → Leader → append local → replicate to followers → quorum ack → respond
Read:  Client → any node → read from local log → respond
```

## Features

### Storage
- Append-only log with O(1) writes
- Segmented storage (`.log` + `.index` files per segment)
- Sparse indexing at configurable byte intervals
- Memory-mapped I/O
- CRC32 checksums

### Replication
- Leader-follower with quorum-based commits
- ISR (In-Sync Replica) tracking
- Parallel non-blocking replication
- Background repair for lagging followers

### Protocol
- Length-prefixed binary format: `[4-byte length][1-byte type][payload]`
- Operations: `Append`, `Read`, `Replicate`, `Heartbeat`

## Build

Requires Zig 0.15.1+

```bash
zig build
```

Outputs `zue-server` and `zue-client` to `zig-out/bin/`.

## Usage

### Server

Standalone:
```bash
./zig-out/bin/zue-server <port> <data_dir>
```

Cluster:
```bash
./zig-out/bin/zue-server <port> <data_dir> <cluster.conf> <node_id>
```

### Client

```bash
./zig-out/bin/zue-client append <host> <port> <key> <value>
./zig-out/bin/zue-client append <host> <port> - <value>   # null key
./zig-out/bin/zue-client read <host> <port> <offset>
```

### Cluster Config

```
node 1 192.168.1.10 9001 leader
node 2 192.168.1.11 9002 follower
node 3 192.168.1.12 9003 follower

quorum 2
timeout 5000
max_lag 1000
heartbeat 2000
```

## Tests

```bash
zig build test                   # unit tests
zig build test-integration       # integration tests  
zig build test-replication       # replication tests
```

## Project Structure

```
src/
├── server.zig              # TCP server
├── client.zig              # Client library
├── cli_client.zig          # CLI
├── config.zig              # Cluster config parser
├── log/
│   ├── mmap_log.zig        # Multi-segment log
│   ├── mmap_segment.zig    # Segment with mmap
│   ├── mmap_index.zig      # Sparse index
│   └── record.zig          # Record format
├── network/
│   └── protocol.zig        # Wire protocol
└── replication/
    ├── leader.zig          # Leader logic
    └── follower.zig        # Follower logic
```

## Status

### Implemented
- [x] Log-structured storage with segments and sparse indexing
- [x] Memory-mapped I/O (mmap segments, index, log writer)
- [x] Leader-follower replication with quorum commits
- [x] ISR tracking and background repair
- [x] Binary network protocol
- [x] CLI client
- [x] Multi-VM deployment tooling

### Not Implemented
- [ ] Leader election (static leader assignment)
- [ ] Log compaction
- [ ] Cluster status API
- [ ] Snapshots

## License

MIT
