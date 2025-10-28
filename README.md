# Zue Storage Engine

Zue is a log-structured storage engine written in Zig, designed for high write throughput and configurable read latency.

## Core Design

The design is based on the following principles:

*   **Append-Only Log**: All writes are sequential appends to a log file, which provides O(1) write complexity and is optimal for most storage hardware.
*   **Segmentation**: The log is partitioned into segments, each consisting of a `.log` file for data and a `.index` file. This allows for efficient, file-level data retention and cleanup.
*   **Sparse Indexing**: To avoid the overhead of indexing every record, an index entry is created only at configurable intervals (e.g., every 4KB). This minimizes the index's memory footprint while still allowing for fast lookups.
*   **Data Integrity**: All records and index entries are protected by CRC32 checksums to prevent data corruption.

For a more detailed breakdown, see the [wiki](wiki/HOME.md).

## Getting Started

### Prerequisites

*   Zig `0.15.1` or later
*   Python `3.7+` (for plotting benchmarks)

### Build and Test

To build the project and run all unit tests:

```bash
zig build test --summary all
```

## Usage

(This section is under development. See the source code in `src/log/log.zig` for the public API.)

## Benchmarking

The project includes a benchmark suite. A convenience script is provided to run the benchmarks and generate plots:

```bash
./run_benchmarks_and_plot.sh
```

Benchmark data is stored in `/tmp/zue_bench_results/`, and plots are in `benchmark_plots/`.

## Configuration

The system's performance can be tuned via the `LogConfig` and `SegmentConfig` structs. The most critical parameters are:

*   `log_file_max_size_bytes`: Controls the size of log segments. Smaller segments allow for more granular data cleanup, while larger segments reduce file management overhead.
*   `bytes_per_index`: Controls the density of the sparse index, allowing a direct trade-off between read latency and memory usage.

## Project Status

Zue is currently in active development. The core single-node storage engine is functional. Future work will focus on:

*   Log Compaction
*   Leader-Follower Replication
*   Memory-Mapped I/O for indices

## Contributing

Contributions are welcome. Please open an issue to discuss your proposed changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.