# Zue Storage Engine

Zue is a distributed, replicated log-structured storage engine written in Zig, providing total ordering and strong consistency guarantees through Raft-like replication.

## Core Design

The design is based on the following principles:

*   **Append-Only Log**: All writes are sequential appends to a log file, which provides O(1) write complexity and is optimal for most storage hardware.
*   **Segmentation**: The log is partitioned into segments, each consisting of a `.log` file for data and a `.index` file. This allows for efficient, file-level data retention and cleanup.
*   **Sparse Indexing**: To avoid the overhead of indexing every record, an index entry is created only at configurable intervals (e.g., every 4KB). This minimizes the index's memory footprint while still allowing for fast lookups.
*   **Data Integrity**: All records and index entries are protected by CRC32 checksums to prevent data corruption.

For a more detailed breakdown, see the [wiki](wiki/).

## Getting Started

### Prerequisites

*   Zig `0.15.1` or later
*   Python `3.7+` (for plotting benchmarks)

### Build and Test

To run all unit tests:

```bash
zig build test --summary all
```

To run integration tests:

```bash
zig build test-integration --summary all
```

To run replication tests:

```bash
zig build test-replication --summary all
```

## Usage

(This section is under development. See the source code in `src/log/log.zig` for the public API or see the [wiki](wiki/).)

## Benchmarking

The project includes a benchmark suite. A convenience script is provided to run the benchmarks and generate plots:

```bash
./run_benchmarks_and_plot.sh
```

Benchmark data is stored in `/tmp/zue_bench_results/`, and plots are in `benchmark_plots/`.

### Completed Features

- [x] **Core Log-Structured Storage Engine**: Append-only log with segmentation and sparse indexing
- [x] **Leader-Follower Replication**: Raft-like consensus with parallel, non-blocking I/O
  - Quorum-based commits with In-Sync Replica (ISR) tracking
  - Leader-driven log repair for consistency
  - Hybrid inline recovery for minimal latency (≤10 entry lag recovers inline)
  - Heartbeat monitoring and automatic failure detection

### In Progress / Future Work

- [ ] **Memory-Mapped I/O Integration**: mmap implementations exist but not yet integrated
  - `MmapSegment`, `MmapIndex`, and `MmapLogReader`/`Writer` implementations complete
  - Expected 10-100x faster index lookups, 5-50x faster log reads
  - Needs integration into main `Log` and replication system
- [ ] **Log Compaction**: Automatic cleanup of old/duplicate entries
- [ ] **Leader Election**: Automatic failover on leader failure (currently static leader)
- [ ] **Cluster Status API**: Programmatic monitoring of cluster state, ISR, and follower lag

## Contributing

Contributions are welcome. Please open an issue to discuss your proposed changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
