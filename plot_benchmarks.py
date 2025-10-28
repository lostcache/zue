#!/usr/bin/env python3
"""
Plot benchmark results from the Zue storage engine benchmarks.

Usage:
    python3 plot_benchmarks.py [--input-dir /tmp/zue_bench_results] [--output-dir ./benchmark_plots]
"""

import argparse
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from matplotlib.patches import Patch
import numpy as np

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

LATENCY_METRICS = ['avg_latency_us', 'p50_us', 'p95_us', 'p99_us']
LATENCY_LABELS = ['Avg', 'P50', 'P95', 'P99']
COLOR_PALETTE = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c', '#9b59b6', '#1abc9c']
MARKER_STYLES = ['o', 's', '^', 'd', 'v', 'p']


def format_bytes(size):
    """Convert byte size to human-readable string."""
    if isinstance(size, str):
        return 'Random' if size == 'random' else size
    if size < 1024:
        return f'{int(size)}B'
    elif size < 1024 * 1024:
        return f'{int(size // 1024)}KB'
    else:
        return f'{int(size // (1024 * 1024))}MB'


def extract_record_sizes(df: pd.DataFrame, size_column: str = 'record_size_bytes'):
    """Extract and sort record sizes, handling both numeric and 'random' values."""
    numeric_sizes = []
    has_random = False

    for val in df[size_column].unique():
        if val == 'random':
            has_random = True
        else:
            numeric_sizes.append(int(val))

    sizes = sorted(numeric_sizes)
    if has_random:
        sizes.append('random')

    return sizes


def save_and_close_plot(output_path: Path, message: str):
    """Common plot finalization logic."""
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}" if not message else f"{message}: {output_path}")


def add_bar_labels(ax, bars, values, format_str='{:.2f}'):
    """Add value labels on top of bars."""
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                format_str.format(val), ha='center', va='bottom', fontsize=9)


def plot_latency_histogram(csv_path: Path, output_path: Path):
    """Plot histogram and box plot of latency distribution."""
    df = pd.read_csv(csv_path)
    fig, (histogram_ax, boxplot_ax) = plt.subplots(1, 2, figsize=(15, 5))

    histogram_ax.hist(df['latency_us'], bins=50, edgecolor='black', alpha=0.7)
    histogram_ax.set_xlabel('Latency (µs)')
    histogram_ax.set_ylabel('Frequency')
    histogram_ax.set_title(f'{csv_path.stem} - Latency Distribution')
    histogram_ax.grid(True, alpha=0.3)

    boxplot_ax.boxplot(df['latency_us'], vert=True)
    boxplot_ax.set_ylabel('Latency (µs)')
    boxplot_ax.set_title(f'{csv_path.stem} - Latency Box Plot')
    boxplot_ax.grid(True, alpha=0.3)

    save_and_close_plot(output_path, "Saved histogram")


def plot_latency_over_time(csv_path: Path, output_path: Path):
    """Plot latency time series with rolling average."""
    df = pd.read_csv(csv_path)
    fig, ax = plt.subplots(figsize=(15, 6))

    ax.plot(df['operation'], df['latency_us'], alpha=0.6, linewidth=0.5)

    window_size = max(1, len(df) // 100)
    rolling_avg = df['latency_us'].rolling(window=window_size, center=True).mean()
    ax.plot(df['operation'], rolling_avg, color='red', linewidth=2,
            label=f'Rolling avg (window={window_size})')

    ax.set_xlabel('Operation Number')
    ax.set_ylabel('Latency (µs)')
    ax.set_title(f'{csv_path.stem} - Latency Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)

    save_and_close_plot(output_path, "Saved time series")


def plot_percentile_chart(csv_path: Path, output_path: Path):
    """Plot bar chart of latency percentiles."""
    df = pd.read_csv(csv_path)
    percentiles = [0, 50, 90, 95, 99, 99.9, 100]
    values = [np.percentile(df['latency_us'], p) for p in percentiles]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(range(len(percentiles)), values, edgecolor='black', alpha=0.7)
    ax.set_xticks(range(len(percentiles)))
    ax.set_xticklabels([f'P{p}' if p < 100 else 'Max' for p in percentiles])
    ax.set_ylabel('Latency (µs)')
    ax.set_title(f'{csv_path.stem} - Percentile Distribution')
    ax.grid(True, alpha=0.3, axis='y')

    add_bar_labels(ax, bars, values)
    save_and_close_plot(output_path, "Saved percentile chart")


def plot_throughput_comparison(df: pd.DataFrame, output_dir: Path):
    """Plot horizontal bar chart comparing throughput across benchmarks."""
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(df['benchmark'], df['throughput_ops_sec'], edgecolor='black', alpha=0.7)
    ax.set_xlabel('Throughput (ops/sec)')
    ax.set_title('Throughput Comparison Across Benchmarks')
    ax.grid(True, alpha=0.3, axis='x')

    for bar, val in zip(bars, df['throughput_ops_sec']):
        ax.text(bar.get_width(), bar.get_y() + bar.get_height()/2., f'{val:.0f}',
                ha='left', va='center', fontsize=9, fontweight='bold')

    save_and_close_plot(output_dir / 'throughput_comparison.png', "Saved throughput comparison")


def plot_latency_percentiles_comparison(df: pd.DataFrame, output_dir: Path):
    """Plot grouped bar chart comparing latency percentiles across benchmarks."""
    fig, ax = plt.subplots(figsize=(14, 8))
    x = np.arange(len(df))
    bar_width = 0.15

    percentile_cols = ['p50_us', 'p95_us', 'p99_us', 'p999_us']
    percentile_labels = ['P50', 'P95', 'P99', 'P99.9']
    colors = COLOR_PALETTE[:4]

    for i, (col, label, color) in enumerate(zip(percentile_cols, percentile_labels, colors)):
        offset = bar_width * (i - len(percentile_cols)/2 + 0.5)
        ax.bar(x + offset, df[col], bar_width, label=label, color=color, alpha=0.8)

    ax.set_xlabel('Benchmark')
    ax.set_ylabel('Latency (µs)')
    ax.set_title('Latency Percentiles Comparison')
    ax.set_xticks(x)
    ax.set_xticklabels(df['benchmark'], rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')

    save_and_close_plot(output_dir / 'latency_percentiles_comparison.png', "Saved latency comparison")


def plot_summary_comparison(summary_csv: Path, output_dir: Path):
    """Generate throughput and latency comparison plots from summary CSV."""
    df = pd.read_csv(summary_csv)
    plot_throughput_comparison(df, output_dir)
    plot_latency_percentiles_comparison(df, output_dir)


def extract_value_size_from_name(name: str) -> int:
    """Extract value size in bytes from benchmark name."""
    size_patterns = {
        '100B': 100,
        '1024B': 1024, '1KB': 1024,
        '10240B': 10240, '10KB': 10240,
        '65536B': 65536, '64KB': 65536
    }
    for pattern, size in size_patterns.items():
        if pattern in name:
            return size
    return None


def plot_value_size_comparison(summary_csv: Path, output_dir: Path):
    """Plot throughput and latency vs value size."""
    df = pd.read_csv(summary_csv)
    value_size_df = df[df['benchmark'].str.contains('Append Throughput', na=False)].copy()

    if len(value_size_df) == 0:
        return

    value_size_df['size_bytes'] = value_size_df['benchmark'].apply(extract_value_size_from_name)
    value_size_df = value_size_df[value_size_df['size_bytes'].notna()].sort_values('size_bytes')

    if len(value_size_df) == 0:
        return

    value_size_df['size_label'] = value_size_df['size_bytes'].apply(format_bytes)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(value_size_df['size_label'], value_size_df['throughput_ops_sec'],
            marker='o', linewidth=2, markersize=8, color=COLOR_PALETTE[1])
    ax.set_xlabel('Value Size')
    ax.set_ylabel('Throughput (ops/sec)')
    ax.set_title('Throughput vs Value Size')
    ax.grid(True, alpha=0.3)

    for x, y in zip(value_size_df['size_label'], value_size_df['throughput_ops_sec']):
        ax.text(x, y, f'{y:.0f}', ha='center', va='bottom', fontsize=9)

    save_and_close_plot(output_dir / 'value_size_throughput.png', "Saved value size throughput")

    fig, ax = plt.subplots(figsize=(12, 6))
    for metric, label, color, marker in zip(LATENCY_METRICS, LATENCY_LABELS, COLOR_PALETTE, MARKER_STYLES):
        ax.plot(value_size_df['size_label'], value_size_df[metric],
                marker=marker, linewidth=2, markersize=8, label=label, color=color)

    ax.set_xlabel('Value Size')
    ax.set_ylabel('Latency (µs)')
    ax.set_title('Latency Percentiles vs Value Size')
    ax.legend()
    ax.grid(True, alpha=0.3)

    save_and_close_plot(output_dir / 'value_size_latency.png', "Saved value size latency")


def extract_index_efficiency_params(benchmark_name: str):
    """Extract bytes_per_index and record_size from benchmark name."""
    bytes_match = re.search(r'bytes_per_index=(\d+)', benchmark_name)
    bytes_per_index = int(bytes_match.group(1)) if bytes_match else None

    if 'record_size=random' in benchmark_name:
        record_size = 'random'
    else:
        size_match = re.search(r'record_size=(\d+)B', benchmark_name)
        record_size = int(size_match.group(1)) if size_match else None

    return bytes_per_index, record_size


def extract_index_efficiency_data(df: pd.DataFrame):
    """Filter and prepare index efficiency data from summary dataframe."""
    index_df = df[df['benchmark'].str.contains('Index Efficiency', na=False)].copy()

    if len(index_df) == 0:
        return None

    params = index_df['benchmark'].apply(extract_index_efficiency_params)
    index_df['bytes_per_index'] = params.apply(lambda x: x[0])
    index_df['record_size'] = params.apply(lambda x: x[1])
    index_df = index_df[index_df['bytes_per_index'].notna() & index_df['record_size'].notna()]

    return index_df if len(index_df) > 0 else None


def prepare_index_plot_data(index_df: pd.DataFrame):
    """Organize record sizes and index labels for plotting."""
    numeric_sizes = [s for s in index_df['record_size'].unique() if isinstance(s, (int, float))]
    has_random = 'random' in index_df['record_size'].values

    unique_bytes_per_index = sorted(index_df['bytes_per_index'].unique())
    unique_record_sizes = sorted(numeric_sizes)
    if has_random:
        unique_record_sizes.append('random')

    index_labels = [format_bytes(b) for b in unique_bytes_per_index]
    return index_labels, unique_record_sizes


def plot_single_index_efficiency_metric(index_df, index_labels, unique_record_sizes,
                                         metric, metric_name, ylabel, output_path):
    """Plot one latency metric across different index granularities and record sizes."""
    fig, ax = plt.subplots(figsize=(12, 7))

    for i, record_size in enumerate(unique_record_sizes):
        size_df = index_df[index_df['record_size'] == record_size].sort_values('bytes_per_index')

        if len(size_df) > 0:
            label = 'random sizes' if record_size == 'random' else f'{format_bytes(record_size)} records'
            ax.plot(index_labels, size_df[metric],
                    marker=MARKER_STYLES[i % len(MARKER_STYLES)],
                    linewidth=2.5, markersize=10,
                    label=label, color=COLOR_PALETTE[i % len(COLOR_PALETTE)])

    ax.set_xlabel('Bytes Per Index Entry', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(f'{metric_name} vs Index Granularity (Different Record Sizes)',
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=10, title='Record Size', title_fontsize=11)
    ax.grid(True, alpha=0.3)

    save_and_close_plot(output_path, f"Saved {metric_name}")


def plot_index_efficiency_comparison(summary_csv: Path, output_dir: Path):
    """Generate index efficiency comparison plots for different percentiles."""
    df = pd.read_csv(summary_csv)
    index_df = extract_index_efficiency_data(df)

    if index_df is None:
        return

    index_labels, unique_record_sizes = prepare_index_plot_data(index_df)

    percentile_specs = [
        ('p50_us', 'P50 Read Latency', 'P50 Read Latency (µs)'),
        ('p95_us', 'P95 Read Latency', 'P95 Read Latency (µs)'),
        ('p99_us', 'P99 Read Latency', 'P99 Read Latency (µs)'),
        ('avg_latency_us', 'Average Read Latency', 'Average Read Latency (µs)')
    ]

    for metric, metric_name, ylabel in percentile_specs:
        output_path = output_dir / f'index_efficiency_{metric}.png'
        plot_single_index_efficiency_metric(index_df, index_labels, unique_record_sizes,
                                             metric, metric_name, ylabel, output_path)


def plot_grouped_latency_bars(ax, df, record_sizes, size_column, metrics, labels, colors):
    """Helper to create grouped bar charts for latency metrics."""
    x = np.arange(len(record_sizes))
    bar_width = 0.2

    for i, (metric, label, color) in enumerate(zip(metrics, labels, colors)):
        values = [df[df[size_column] == str(rs)][metric].values[0] for rs in record_sizes]
        offset = bar_width * (i - len(metrics)/2 + 0.5)
        ax.bar(x + offset, values, bar_width, label=label, color=color, alpha=0.8)

    return x


def plot_read_latencies_combined(seq_csv: Path, rand_csv: Path, output_dir: Path):
    """Plot sequential and random read latencies side by side."""
    seq_df = pd.read_csv(seq_csv, dtype={'record_size_bytes': str})
    rand_df = pd.read_csv(rand_csv, dtype={'record_size_bytes': str})

    record_sizes = extract_record_sizes(seq_df)
    record_labels = [format_bytes(rs) for rs in record_sizes]

    fig, (seq_ax, rand_ax) = plt.subplots(1, 2, figsize=(16, 6))

    plot_grouped_latency_bars(seq_ax, seq_df, record_sizes, 'record_size_bytes',
                              LATENCY_METRICS, LATENCY_LABELS, COLOR_PALETTE[:4])
    seq_ax.set_xlabel('Record Size', fontsize=12)
    seq_ax.set_ylabel('Latency (µs)', fontsize=12)
    seq_ax.set_title('Sequential Read Latencies', fontsize=13, fontweight='bold')
    seq_ax.set_xticks(range(len(record_sizes)))
    seq_ax.set_xticklabels(record_labels)
    seq_ax.legend(fontsize=10)
    seq_ax.grid(True, alpha=0.3, axis='y')

    plot_grouped_latency_bars(rand_ax, rand_df, record_sizes, 'record_size_bytes',
                              LATENCY_METRICS, LATENCY_LABELS, COLOR_PALETTE[:4])
    rand_ax.set_xlabel('Record Size', fontsize=12)
    rand_ax.set_ylabel('Latency (µs)', fontsize=12)
    rand_ax.set_title('Random Read Latencies', fontsize=13, fontweight='bold')
    rand_ax.set_xticks(range(len(record_sizes)))
    rand_ax.set_xticklabels(record_labels)
    rand_ax.legend(fontsize=10)
    rand_ax.grid(True, alpha=0.3, axis='y')

    fig.suptitle('Read Latency Comparison (4KB Index Granularity)', fontsize=14, fontweight='bold')
    save_and_close_plot(output_dir / 'read_latencies_comparison.png', "Saved")


def plot_append_latencies(csv_path: Path, output_dir: Path):
    """Plot append latencies for all record sizes."""
    df = pd.read_csv(csv_path, dtype={'record_size_bytes': str})
    record_sizes = extract_record_sizes(df)
    record_labels = [format_bytes(rs) for rs in record_sizes]

    fig, ax = plt.subplots(figsize=(12, 6))
    plot_grouped_latency_bars(ax, df, record_sizes, 'record_size_bytes',
                              LATENCY_METRICS, LATENCY_LABELS, COLOR_PALETTE[:4])

    ax.set_xlabel('Record Size', fontsize=12)
    ax.set_ylabel('Latency (µs)', fontsize=12)
    ax.set_title('Append Latencies (4KB Index Granularity)', fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(record_sizes)))
    ax.set_xticklabels(record_labels)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')

    save_and_close_plot(output_dir / 'append_latencies.png', "Saved")


def create_subplot_grid(record_sizes, index_labels, df, metrics, labels, colors, markers, plot_type='line'):
    """Create 2x3 subplot grid for index efficiency visualizations."""
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()

    for idx, record_size in enumerate(record_sizes):
        ax = axes[idx]
        size_df = df[df['record_size_bytes'] == str(record_size)].sort_values('bytes_per_index')

        if plot_type == 'line':
            for metric, label, color, marker in zip(metrics, labels, colors, markers):
                ax.plot(index_labels, size_df[metric],
                       marker=marker, linewidth=2, markersize=8,
                       label=label, color=color)
        else:
            x = range(len(index_labels))
            bar_width = 0.2
            for i, (metric, label, color) in enumerate(zip(metrics, labels, colors)):
                offset = bar_width * (i - len(metrics)/2 + 0.5)
                ax.bar([p + offset for p in x], size_df[metric], bar_width,
                      label=label, color=color, alpha=0.8)
            ax.set_xticks(x)
            ax.set_xticklabels(index_labels)

        ax.set_xlabel('Index Granularity', fontsize=11)
        ax.set_ylabel('Read Latency (µs)', fontsize=11)
        ax.set_title(f'{format_bytes(record_size)} Records', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    for idx in range(len(record_sizes), len(axes)):
        axes[idx].axis('off')

    return fig


def print_best_index_granularity_table(df, record_sizes):
    """Print recommendation table for best index granularity per record size."""
    print("\n" + "="*80)
    print("BEST INDEX GRANULARITY PER RECORD SIZE (by P95 latency)")
    print("="*80)
    print(f"{'Record Size':<25} {'Best Index':<15} {'P95 (µs)':<12} {'P99 (µs)':<12}")
    print("-"*80)

    for record_size in record_sizes:
        size_df = df[df['record_size_bytes'] == str(record_size)]
        best_row = size_df.loc[size_df['p95_us'].idxmin()]
        print(f"{format_bytes(record_size):<25} "
              f"{format_bytes(best_row['bytes_per_index']):<15} "
              f"{best_row['p95_us']:<12.2f} "
              f"{best_row['p99_us']:<12.2f}")

    print("="*80 + "\n")


def plot_index_efficiency_subplots(csv_path: Path, output_dir: Path):
    """Generate line and bar chart subplots for index efficiency analysis."""
    df = pd.read_csv(csv_path, dtype={'record_size_bytes': str})
    record_sizes = extract_record_sizes(df)
    bytes_per_index_values = sorted(df['bytes_per_index'].unique())
    index_labels = [format_bytes(b) for b in bytes_per_index_values]

    fig = create_subplot_grid(record_sizes, index_labels, df,
                             LATENCY_METRICS, LATENCY_LABELS,
                             COLOR_PALETTE[:4], MARKER_STYLES[:4], 'line')
    save_and_close_plot(output_dir / 'index_efficiency_line.png', "Saved")

    fig = create_subplot_grid(record_sizes, index_labels, df,
                             LATENCY_METRICS, LATENCY_LABELS,
                             COLOR_PALETTE[:4], MARKER_STYLES[:4], 'bar')
    save_and_close_plot(output_dir / 'index_efficiency_bar.png', "Saved")

    print_best_index_granularity_table(df, record_sizes)


def plot_index_creation_overhead(index_creating_csv: Path, normal_csv: Path, output_dir: Path):
    """Compare normal append vs index-creating append latencies with overhead percentages."""
    index_creating_df = pd.read_csv(index_creating_csv, dtype={'record_size_bytes': str})
    normal_df = pd.read_csv(normal_csv, dtype={'record_size_bytes': str})

    record_sizes = extract_record_sizes(index_creating_df)
    record_labels = [format_bytes(rs) for rs in record_sizes]

    fig, ax = plt.subplots(figsize=(14, 7))
    x = np.arange(len(record_sizes))
    bar_width = 0.1

    for i, (metric, label, color) in enumerate(zip(LATENCY_METRICS, LATENCY_LABELS, COLOR_PALETTE[:4])):
        normal_data = {}
        for j, rs in enumerate(record_sizes):
            match = normal_df[normal_df['record_size_bytes'] == str(rs)][metric]
            if len(match) > 0:
                normal_data[j] = match.values[0]

        offset = bar_width * (i * 2 - len(LATENCY_METRICS) + 0.5)
        if normal_data:
            ax.bar(np.array(list(normal_data.keys())) + offset, list(normal_data.values()),
                   bar_width, color=color, alpha=0.8, edgecolor='black', linewidth=0.5)

        index_x, index_values = [], []
        for j, rs in enumerate(record_sizes):
            match = index_creating_df[index_creating_df['record_size_bytes'] == str(rs)][metric]
            if len(match) > 0:
                index_x.append(j)
                index_values.append(match.values[0])

        offset = bar_width * (i * 2 - len(LATENCY_METRICS) + 1.5)
        if index_values:
            bars = ax.bar(np.array(index_x) + offset, index_values, bar_width,
                          color=color, alpha=0.8, edgecolor='black', linewidth=0.5, hatch='///')

            for j, (bar, val) in enumerate(zip(bars, index_values)):
                record_idx = index_x[j]
                if record_idx in normal_data:
                    normal_val = normal_data[record_idx]
                    pct = ((val - normal_val) / normal_val) * 100
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                           f'{pct:.1f}%', ha='center', va='bottom', fontsize=7, fontweight='bold')

    legend_elements = [Patch(facecolor=color, edgecolor='black', label=label, alpha=0.8)
                      for color, label in zip(COLOR_PALETTE[:4], LATENCY_LABELS)]
    legend_elements.append(Patch(facecolor='white', edgecolor='black', hatch='///', label='Index-Creating'))

    ax.set_xlabel('Record Size', fontsize=12)
    ax.set_ylabel('Latency (µs)', fontsize=12)
    ax.set_title('Append Latencies: Normal (Solid) vs Index-Creating (Hatched)',
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(record_labels)
    ax.legend(handles=legend_elements, fontsize=10, loc='upper left')
    ax.grid(True, alpha=0.3, axis='y')

    save_and_close_plot(output_dir / 'index_creation_overhead.png', "Saved")

    print("\n" + "="*80)
    print("INDEX CREATION OVERHEAD ANALYSIS")
    print("="*80)
    print(f"{'Record Size':<15} {'Metric':<8} {'Normal (µs)':<15} {'Index-Creating (µs)':<20} "
          f"{'Overhead (%)':<12} {'Samples (I/N)':<15}")
    print("-"*80)

    for rs in record_sizes:
        idx_creating_match = index_creating_df[index_creating_df['record_size_bytes'] == str(rs)]
        normal_match = normal_df[normal_df['record_size_bytes'] == str(rs)]

        if len(idx_creating_match) == 0:
            continue

        idx_creating_row = idx_creating_match.iloc[0]
        idx_samples = int(idx_creating_row['num_samples'])

        has_normal = len(normal_match) > 0
        norm_samples = int(normal_match.iloc[0]['num_samples']) if has_normal else 0

        for metric, label in zip(['avg_latency_us', 'p95_us'], ['Avg', 'P95']):
            idx_val = idx_creating_row[metric]

            if has_normal:
                normal_val = normal_match.iloc[0][metric]
                overhead_pct = (idx_val - normal_val) / normal_val * 100 if normal_val > 0 else 0
                overhead_str = f"{overhead_pct:>10.2f}%"
                normal_str = f"{normal_val:.2f}"
            else:
                overhead_str = "N/A (all index)"
                normal_str = "N/A"

            sample_str = f"{idx_samples}/{norm_samples}" if label == 'Avg' else ""
            print(f"{format_bytes(rs):<15} {label:<8} {normal_str:<15} "
                  f"{idx_val:<20.2f} {overhead_str:<12} {sample_str:<15}")

    print("="*80 + "\n")


def process_latency_csvs(input_dir: Path, output_dir: Path):
    """Process individual latency CSV files and generate plots."""
    for csv_file in input_dir.glob('*_latencies.csv'):
        if csv_file.name == 'summary.csv':
            continue

        print(f"\nProcessing: {csv_file.name}")
        base_name = csv_file.stem.replace('_latencies', '')

        if 'Index_Efficiency' in base_name or base_name == 'append':
            continue

        plot_latency_histogram(csv_file, output_dir / f'{base_name}_histogram.png')
        plot_latency_over_time(csv_file, output_dir / f'{base_name}_timeseries.png')
        plot_percentile_chart(csv_file, output_dir / f'{base_name}_percentiles.png')


def process_summary_and_aggregates(input_dir: Path, output_dir: Path):
    """Process summary CSV and generate aggregate comparison plots."""
    summary_csv = input_dir / 'summary.csv'
    if summary_csv.exists():
        print(f"\nProcessing summary: {summary_csv.name}")
        plot_summary_comparison(summary_csv, output_dir)
        plot_value_size_comparison(summary_csv, output_dir)
        plot_index_efficiency_comparison(summary_csv, output_dir)
    else:
        print(f"\nWarning: Summary CSV not found at {summary_csv}")

    index_csv = input_dir / 'index_efficiency.csv'
    if index_csv.exists():
        print(f"\nProcessing index efficiency: {index_csv.name}")
        plot_index_efficiency_subplots(index_csv, output_dir)

    seq_csv = input_dir / 'sequential_reads.csv'
    rand_csv = input_dir / 'random_reads.csv'
    if seq_csv.exists() and rand_csv.exists():
        print(f"\nProcessing read latencies comparison")
        plot_read_latencies_combined(seq_csv, rand_csv, output_dir)

    append_csv = input_dir / 'append_latencies.csv'
    if append_csv.exists():
        print(f"\nProcessing append latencies")
        plot_append_latencies(append_csv, output_dir)

    index_creating_csv = input_dir / 'append_with_index_creation.csv'
    normal_append_csv = input_dir / 'append_normal.csv'
    if index_creating_csv.exists() and normal_append_csv.exists():
        print(f"\nProcessing index creation overhead comparison")
        plot_index_creation_overhead(index_creating_csv, normal_append_csv, output_dir)


def main():
    parser = argparse.ArgumentParser(description='Plot benchmark results from Zue storage engine')
    parser.add_argument('--input-dir', type=str, default='/tmp/zue_bench_results',
                        help='Directory containing benchmark CSV files')
    parser.add_argument('--output-dir', type=str, default='./benchmark_plots',
                        help='Directory to save plot images')
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        print(f"Error: Input directory {input_dir} does not exist")
        print("Please run the benchmarks first to generate CSV files")
        return

    output_dir.mkdir(exist_ok=True)
    print(f"Saving plots to: {output_dir}")

    process_latency_csvs(input_dir, output_dir)
    process_summary_and_aggregates(input_dir, output_dir)

    print(f"\n✓ All plots saved to: {output_dir}")


if __name__ == '__main__':
    main()
