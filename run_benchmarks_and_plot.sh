#!/bin/bash

# Run benchmarks and generate plots

set -e

echo "Running benchmarks..."
zig build bench

echo "Initializing Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo "Installing Python dependencies..."
pip3 install -r requirements.txt

echo ""
echo "Generating plots..."
python3 plot_benchmarks.py

echo ""
echo "✓ Complete!"
echo "  - Benchmark data: /tmp/zue_bench_results/"
echo "  - Plots: ./benchmark_plots/"
