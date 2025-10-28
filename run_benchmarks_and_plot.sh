#!/bin/bash
# Run benchmarks and generate plots
#
# Usage:
#   ./run_benchmarks_and_plot.sh

set -e

echo "Running benchmarks..."
zig build bench

echo ""
echo "Generating plots..."
python3 plot_benchmarks.py

echo ""
echo "✓ Complete!"
echo "  - Benchmark data: /tmp/zue_bench_results/"
echo "  - Plots: ./benchmark_plots/"
