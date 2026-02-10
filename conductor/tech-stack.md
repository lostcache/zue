# Tech Stack

## Core Language & Runtime
- **Zig (v0.15.1+):** Chosen for its performance, manual memory management without hidden allocations, and powerful metaprogramming (comptime). It is ideal for building low-level system components like storage engines.

## Storage Engine
- **Log-Structured Storage:** Implemented using append-only segments to maximize write throughput and ensure O(1) appends.
- **Memory-Mapped I/O (mmap):** Used for both data segments and indexes to leverage the OS page cache and provide high-performance file access with minimal syscall overhead.
- **Sparse Indexing:** Maintains an in-memory or on-disk index at configurable intervals to enable fast lookups within log segments.
- **Data Integrity:** Employs CRC32 checksums for every record to detect and prevent data corruption.

## Networking & Protocol
- **TCP/IP:** The primary transport layer for node-to-node communication and client-to-node interaction.
- **Custom Binary Protocol:** A length-prefixed protocol optimized for low overhead, supporting operations like `Append`, `Read`, `Replicate`, and `Heartbeat`.

## Replication & Consistency
- **Leader-Follower Architecture:** A single leader handles all write operations, ensuring a total ordering of events.
- **Quorum-Based Commits:** Uses synchronous replication to a majority of nodes to guarantee durability before acknowledging writes.
- **In-Sync Replicas (ISR):** Tracks the health and lag of followers to manage the quorum and ensure high availability.

## Build & Tooling
- **Zig Build System:** Standard `build.zig` for compiling the server, client, and running tests.
- **Testing:** Comprehensive suite including unit tests, integration tests, and specialized replication tests.
