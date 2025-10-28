const std = @import("std");
const log = @import("log/log.zig");
const record = @import("log/record.zig");
const segment = @import("log/segment.zig");

const Log = log.Log;
const LogConfig = log.LogConfig;
const Record = record.Record;
const SegmentConfig = segment.SegmentConfig;

pub const BenchmarkResult = struct {
    name: []const u8,
    total_ops: u64,
    duration_ns: u64,
    latencies_ns: []u64,
    allocator: std.mem.Allocator,

    pub fn init(name: []const u8, total_ops: u64, latencies: []u64, allocator: std.mem.Allocator) !BenchmarkResult {
        var total: u64 = 0;
        for (latencies) |lat| {
            total += lat;
        }

        const owned_name = try allocator.dupe(u8, name);

        return BenchmarkResult{
            .name = owned_name,
            .total_ops = total_ops,
            .duration_ns = total,
            .latencies_ns = latencies,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BenchmarkResult) void {
        self.allocator.free(self.latencies_ns);
        self.allocator.free(self.name);
    }

    pub fn throughput(self: *const BenchmarkResult) f64 {
        const duration_sec = @as(f64, @floatFromInt(self.duration_ns)) / 1_000_000_000.0;
        return @as(f64, @floatFromInt(self.total_ops)) / duration_sec;
    }

    pub fn avgLatencyNs(self: *const BenchmarkResult) f64 {
        if (self.latencies_ns.len == 0) return 0.0;
        var sum: u64 = 0;
        for (self.latencies_ns) |lat| {
            sum += lat;
        }
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(self.latencies_ns.len));
    }

    pub fn avgLatencyUs(self: *const BenchmarkResult) f64 {
        return self.avgLatencyNs() / 1_000.0;
    }

    pub fn percentile(self: *const BenchmarkResult, p: f64) u64 {
        if (self.latencies_ns.len == 0) return 0;

        // Make a copy and sort it
        const sorted = self.allocator.dupe(u64, self.latencies_ns) catch return 0;
        defer self.allocator.free(sorted);

        std.mem.sort(u64, sorted, {}, comptime std.sort.asc(u64));

        const idx = @as(usize, @intFromFloat(@as(f64, @floatFromInt(sorted.len)) * p));
        const clamped = @min(idx, sorted.len - 1);
        return sorted[clamped];
    }

    pub fn print(self: *const BenchmarkResult) void {
        std.debug.print("\n=== {s} ===\n", .{self.name});
        std.debug.print("Total operations: {d}\n", .{self.total_ops});
        std.debug.print("Total duration: {d:.2} ms\n", .{@as(f64, @floatFromInt(self.duration_ns)) / 1_000_000.0});
        std.debug.print("Throughput: {d:.2} ops/sec\n", .{self.throughput()});
        std.debug.print("Avg latency: {d:.2} µs\n", .{self.avgLatencyUs()});
        std.debug.print("P50 latency: {d:.2} µs\n", .{@as(f64, @floatFromInt(self.percentile(0.50))) / 1_000.0});
        std.debug.print("P95 latency: {d:.2} µs\n", .{@as(f64, @floatFromInt(self.percentile(0.95))) / 1_000.0});
        std.debug.print("P99 latency: {d:.2} µs\n", .{@as(f64, @floatFromInt(self.percentile(0.99))) / 1_000.0});
        std.debug.print("P99.9 latency: {d:.2} µs\n", .{@as(f64, @floatFromInt(self.percentile(0.999))) / 1_000.0});
        std.debug.print("Min latency: {d:.2} µs\n", .{@as(f64, @floatFromInt(self.percentile(0.0))) / 1_000.0});
        std.debug.print("Max latency: {d:.2} µs\n", .{@as(f64, @floatFromInt(self.percentile(1.0))) / 1_000.0});
    }

    pub fn writeCSV(self: *const BenchmarkResult, file: std.fs.File, allocator: std.mem.Allocator) !void {
        var buffer: std.ArrayList(u8) = .empty;
        errdefer buffer.deinit(allocator);
        const writer = buffer.writer(allocator);

        try writer.print("operation,latency_ns,latency_us\n", .{});
        for (self.latencies_ns, 0..) |lat, idx| {
            const lat_us = @as(f64, @floatFromInt(lat)) / 1_000.0;
            try writer.print("{d},{d},{d:.2}\n", .{ idx, lat, lat_us });
        }

        try file.writeAll(buffer.items);
        buffer.deinit(allocator);
    }

    pub fn writeSummaryCSV(self: *const BenchmarkResult, file: std.fs.File, allocator: std.mem.Allocator) !void {
        var buffer: std.ArrayList(u8) = .empty;
        errdefer buffer.deinit(allocator);
        const writer = buffer.writer(allocator);

        // Quote the benchmark name to handle commas properly
        try writer.print("\"{s}\",{d},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2}\n", .{
            self.name,
            self.total_ops,
            @as(f64, @floatFromInt(self.duration_ns)) / 1_000_000.0,
            self.throughput(),
            self.avgLatencyUs(),
            @as(f64, @floatFromInt(self.percentile(0.50))) / 1_000.0,
            @as(f64, @floatFromInt(self.percentile(0.95))) / 1_000.0,
            @as(f64, @floatFromInt(self.percentile(0.99))) / 1_000.0,
            @as(f64, @floatFromInt(self.percentile(0.999))) / 1_000.0,
            @as(f64, @floatFromInt(self.percentile(0.0))) / 1_000.0,
            @as(f64, @floatFromInt(self.percentile(1.0))) / 1_000.0,
        });

        try file.writeAll(buffer.items);
        buffer.deinit(allocator);
    }
};

pub const Timer = struct {
    start_time: i128,

    pub fn start() Timer {
        return Timer{
            .start_time = std.time.nanoTimestamp(),
        };
    }

    pub fn lap(self: *Timer) u64 {
        const now = std.time.nanoTimestamp();
        const elapsed_ns = now - self.start_time;
        self.start_time = now;
        return @intCast(elapsed_ns);
    }

    pub fn elapsed(self: *const Timer) u64 {
        const now = std.time.nanoTimestamp();
        return @intCast(now - self.start_time);
    }
};

fn createTestDir(allocator: std.mem.Allocator) ![]const u8 {
    const timestamp = std.time.milliTimestamp();
    const dir_name = try std.fmt.allocPrint(allocator, "/tmp/zue_bench_{d}", .{timestamp});
    try std.fs.makeDirAbsolute(dir_name);
    return dir_name;
}

fn cleanupTestDir(dir_path: []const u8) void {
    std.fs.deleteTreeAbsolute(dir_path) catch |err| {
        std.debug.print("Warning: Failed to cleanup test dir: {}\n", .{err});
    };
}

fn saveBenchmarkResults(result: *const BenchmarkResult, output_dir: []const u8, allocator: std.mem.Allocator) !void {
    // Create output directory if it doesn't exist
    std.fs.makeDirAbsolute(output_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };

    // Sanitize filename by replacing spaces with underscores
    const sanitized_name = try allocator.alloc(u8, result.name.len);
    defer allocator.free(sanitized_name);
    for (result.name, 0..) |c, i| {
        sanitized_name[i] = if (c == ' ' or c == '(' or c == ')' or c == ',') '_' else c;
    }

    // Save detailed latencies CSV
    const detailed_path = try std.fmt.allocPrint(allocator, "{s}/{s}_latencies.csv", .{ output_dir, sanitized_name });
    defer allocator.free(detailed_path);
    const detailed_file = try std.fs.createFileAbsolute(detailed_path, .{});
    defer detailed_file.close();
    try result.writeCSV(detailed_file, allocator);

    std.debug.print("Saved detailed results to: {s}\n", .{detailed_path});
}

pub fn benchAppendThroughput(allocator: std.mem.Allocator, num_records: u64, record_size: usize) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const config = LogConfig.default();
    var log_instance = try Log.create(config, test_dir, allocator);
    defer log_instance.close();

    const key_size = @min(record_size, 512);
    const key = try allocator.alloc(u8, key_size);
    defer allocator.free(key);
    @memset(key, 'k');

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'v');

    // Measure individual append latencies
    var latencies = try allocator.alloc(u64, num_records);
    errdefer allocator.free(latencies);

    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        const rec = Record{ .key = key, .value = value };
        var timer = Timer.start();
        _ = try log_instance.append(rec);
        latencies[i] = timer.elapsed();
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Append Throughput ({d} records, {d}B each)", .{ num_records, record_size });
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_records, latencies, allocator);
}

pub fn benchRandomReadLatency(allocator: std.mem.Allocator, num_writes: u64, num_reads: u64, record_size: usize) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const config = LogConfig.default();
    var log_instance = try Log.create(config, test_dir, allocator);
    defer log_instance.close();

    const key_size = @min(record_size, 512);
    const key = try allocator.alloc(u8, key_size);
    defer allocator.free(key);
    @memset(key, 'k');

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'v');

    var offsets = try allocator.alloc(u64, num_writes);
    defer allocator.free(offsets);

    var i: u64 = 0;
    while (i < num_writes) : (i += 1) {
        const rec = Record{ .key = key, .value = value };
        offsets[i] = try log_instance.append(rec);
    }

    var latencies = try allocator.alloc(u64, num_reads);
    errdefer allocator.free(latencies);

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    const random = prng.random();

    i = 0;
    while (i < num_reads) : (i += 1) {
        const offset_idx = random.intRangeAtMost(u64, 0, num_writes - 1);
        const offset = offsets[offset_idx];

        var timer = Timer.start();
        const read_rec = try log_instance.read(offset, allocator);
        latencies[i] = timer.elapsed();

        // Verify correctness
        if (!std.mem.eql(u8, read_rec.value, value)) {
            return error.IncorrectRead;
        }
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Random Read Latency ({d} reads from {d} records)", .{ num_reads, num_writes });
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_reads, latencies, allocator);
}

/// Benchmark: Sequential read latency
pub fn benchSequentialReadLatency(allocator: std.mem.Allocator, num_writes: u64, record_size: usize) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const config = LogConfig.default();
    var log_instance = try Log.create(config, test_dir, allocator);
    defer log_instance.close();

    // Prepare test data
    // Keys are limited to 1KB by default, so keep them small
    const key_size = @min(record_size, 512);
    const key = try allocator.alloc(u8, key_size);
    defer allocator.free(key);
    @memset(key, 'k');

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'v');

    // Write records first
    var offsets = try allocator.alloc(u64, num_writes);
    defer allocator.free(offsets);

    var i: u64 = 0;
    while (i < num_writes) : (i += 1) {
        const rec = Record{ .key = key, .value = value };
        offsets[i] = try log_instance.append(rec);
    }

    // Sequential read benchmark
    var latencies = try allocator.alloc(u64, num_writes);
    errdefer allocator.free(latencies);

    i = 0;
    while (i < num_writes) : (i += 1) {
        const offset = offsets[i];

        var timer = Timer.start();
        const read_rec = try log_instance.read(offset, allocator);
        latencies[i] = timer.elapsed();

        // Verify correctness
        if (!std.mem.eql(u8, read_rec.value, value)) {
            return error.IncorrectRead;
        }
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Sequential Read Latency ({d} records, {d}B each)", .{ num_writes, record_size });
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_writes, latencies, allocator);
}

/// Benchmark: Segment rotation overhead
pub fn benchSegmentRotation(allocator: std.mem.Allocator, record_size: usize) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    // Small segment size to force frequent rotations
    const segment_config = SegmentConfig.init(1024 * 1024, 10 * 1024); // 1MB log, 10KB index
    const config = LogConfig{
        .segment_config = segment_config,
        .initial_offset = 0,
    };

    var log_instance = try Log.create(config, test_dir, allocator);
    defer log_instance.close();

    // Prepare test data
    // Keys are limited to 1KB by default, so keep them small
    const key_size = @min(record_size, 512);
    const key = try allocator.alloc(u8, key_size);
    defer allocator.free(key);
    @memset(key, 'k');

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'v');

    // Write enough to cause multiple rotations
    // With 1KB records and 1MB segments, we should get ~1000 records per segment
    const num_records: u64 = 5000; // Should cause ~5 segment rotations

    var latencies = try allocator.alloc(u64, num_records);
    errdefer allocator.free(latencies);

    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        const rec = Record{ .key = key, .value = value };
        var timer = Timer.start();
        _ = try log_instance.append(rec);
        latencies[i] = timer.elapsed();
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Segment Rotation ({d} records, {d}B each, {d} segments)", .{ num_records, record_size, log_instance.getSegmentCount() });
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_records, latencies, allocator);
}

/// Benchmark: Different record sizes
pub fn benchRecordSizes(allocator: std.mem.Allocator, output_dir: []const u8, summary_file: std.fs.File) !void {
    const sizes = [_]usize{ 100, 1024, 10 * 1024, 64 * 1024 };
    const num_records: u64 = 1000;

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("RECORD SIZE COMPARISON\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});

    for (sizes) |size| {
        var result = try benchAppendThroughput(allocator, num_records, size);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }
}

/// Benchmark: Random value sizes
pub fn benchRandomValueSizes(allocator: std.mem.Allocator, num_records: u64) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const config = LogConfig.default();
    var log_instance = try Log.create(config, test_dir, allocator);
    defer log_instance.close();

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    const random = prng.random();

    // Measure individual append latencies with random sizes
    var latencies = try allocator.alloc(u64, num_records);
    errdefer allocator.free(latencies);

    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        // Random value size between 100B and 64KB
        const value_size = random.intRangeAtMost(usize, 100, 65536);
        const key_size = @min(value_size / 2, 512);

        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, value_size);
        defer allocator.free(value);
        @memset(value, 'v');

        const rec = Record{ .key = key, .value = value };
        var timer = Timer.start();
        _ = try log_instance.append(rec);
        latencies[i] = timer.elapsed();
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Random Value Sizes ({d} records, 100B-64KB)", .{num_records});
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_records, latencies, allocator);
}

/// Benchmark: Index efficiency with different bytes_per_index
pub fn benchIndexEfficiency(allocator: std.mem.Allocator, output_dir: []const u8, summary_file: std.fs.File) !void {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const bytes_per_index_values = [_]u64{ 1024, 4096, 16384 }; // 1KB, 4KB, 16KB
    const num_records: u64 = 10000;
    const record_size: usize = 1024;

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("INDEX EFFICIENCY COMPARISON\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});

    for (bytes_per_index_values) |bytes_per_index| {
        // Create fresh log for each test
        const dir_name = try std.fmt.allocPrint(allocator, "{s}/idx_{d}", .{ test_dir, bytes_per_index });
        defer allocator.free(dir_name);
        try std.fs.makeDirAbsolute(dir_name);

        var segment_config = SegmentConfig.default();
        segment_config.index_config.bytes_per_index = bytes_per_index;
        const config = LogConfig{
            .segment_config = segment_config,
            .initial_offset = 0,
        };

        var log_instance = try Log.create(config, dir_name, allocator);
        defer log_instance.close();

        // Prepare test data
        const key = try allocator.alloc(u8, record_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, record_size);
        defer allocator.free(value);
        @memset(value, 'v');

        // Write records
        var offsets = try allocator.alloc(u64, num_records);
        defer allocator.free(offsets);

        var i: u64 = 0;
        while (i < num_records) : (i += 1) {
            const rec = Record{ .key = key, .value = value };
            offsets[i] = try log_instance.append(rec);
        }

        // Random read benchmark
        const num_reads: u64 = 1000;
        var latencies = try allocator.alloc(u64, num_reads);
        // Note: latencies will be freed by result.deinit()

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp() + @as(i64, @intCast(bytes_per_index))));
        const random = prng.random();

        i = 0;
        while (i < num_reads) : (i += 1) {
            const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
            const offset = offsets[offset_idx];

            var timer = Timer.start();
            _ = try log_instance.read(offset, allocator);
            latencies[i] = timer.elapsed();
        }

        const bench_name = try std.fmt.allocPrint(allocator, "Index Efficiency (bytes_per_index={d}, {d} reads)", .{ bytes_per_index, num_reads });
        defer allocator.free(bench_name);

        var result = try BenchmarkResult.init(bench_name, num_reads, latencies, allocator);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const output_dir = "/tmp/zue_bench_results";
    std.fs.makeDirAbsolute(output_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };

    // Create summary CSV file
    const summary_path = try std.fmt.allocPrint(allocator, "{s}/summary.csv", .{output_dir});
    defer allocator.free(summary_path);
    const summary_file = try std.fs.createFileAbsolute(summary_path, .{});
    defer summary_file.close();

    // Write summary CSV header
    try summary_file.writeAll("benchmark,total_ops,duration_ms,throughput_ops_sec,avg_latency_us,p50_us,p95_us,p99_us,p999_us,min_us,max_us\n");

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("ZUE STORAGE ENGINE PERFORMANCE BENCHMARKS\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});

    // 1. Append throughput
    {
        std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
        std.debug.print("APPEND THROUGHPUT\n", .{});
        std.debug.print("=" ** 80 ++ "\n", .{});

        var result = try benchAppendThroughput(allocator, 10000, 1024);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }

    // 2. Random read latency
    {
        std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
        std.debug.print("RANDOM READ LATENCY\n", .{});
        std.debug.print("=" ** 80 ++ "\n", .{});

        var result = try benchRandomReadLatency(allocator, 10000, 1000, 1024);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }

    // 3. Sequential read latency
    {
        std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
        std.debug.print("SEQUENTIAL READ LATENCY\n", .{});
        std.debug.print("=" ** 80 ++ "\n", .{});

        var result = try benchSequentialReadLatency(allocator, 10000, 1024);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }

    // 4. Segment rotation
    {
        std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
        std.debug.print("SEGMENT ROTATION OVERHEAD\n", .{});
        std.debug.print("=" ** 80 ++ "\n", .{});

        var result = try benchSegmentRotation(allocator, 1024);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }

    // 5. Record sizes
    try benchRecordSizes(allocator, output_dir, summary_file);

    // 6. Random value sizes
    {
        std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
        std.debug.print("RANDOM VALUE SIZES\n", .{});
        std.debug.print("=" ** 80 ++ "\n", .{});

        var result = try benchRandomValueSizes(allocator, 5000);
        defer result.deinit();
        result.print();
        try saveBenchmarkResults(&result, output_dir, allocator);
        try result.writeSummaryCSV(summary_file, allocator);
    }

    // 7. Index efficiency
    try benchIndexEfficiency(allocator, output_dir, summary_file);

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("BENCHMARKS COMPLETE\n", .{});
    std.debug.print("Results saved to: {s}\n", .{output_dir});
    std.debug.print("=" ** 80 ++ "\n\n", .{});
}
