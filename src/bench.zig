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
        return percentileHelper(self.latencies_ns, p, self.allocator);
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

const TestData = struct {
    key: []u8,
    value: []u8,
    allocator: std.mem.Allocator,

    fn init(record_size: usize, allocator: std.mem.Allocator) !TestData {
        const key_size = @min(record_size, 512);
        const key = try allocator.alloc(u8, key_size);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, record_size);
        @memset(value, 'v');

        return .{ .key = key, .value = value, .allocator = allocator };
    }

    fn deinit(self: TestData) void {
        self.allocator.free(self.key);
        self.allocator.free(self.value);
    }

    fn toRecord(self: TestData) Record {
        return .{ .key = self.key, .value = self.value };
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

fn createLogWithConfig(config: LogConfig, dir: []const u8, allocator: std.mem.Allocator) !Log {
    return try Log.create(config, dir, allocator);
}

fn saveBenchmarkResults(result: *const BenchmarkResult, output_dir: []const u8, allocator: std.mem.Allocator) !void {
    std.fs.makeDirAbsolute(output_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };

    const sanitized_name = try allocator.alloc(u8, result.name.len);
    defer allocator.free(sanitized_name);
    for (result.name, 0..) |c, i| {
        sanitized_name[i] = if (c == ' ' or c == '(' or c == ')' or c == ',') '_' else c;
    }

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
    var log_instance = try createLogWithConfig(config, test_dir, allocator);
    defer log_instance.close();

    const test_data = try TestData.init(record_size, allocator);
    defer test_data.deinit();

    var latencies = try allocator.alloc(u64, num_records);
    errdefer allocator.free(latencies);

    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        var timer = Timer.start();
        _ = try log_instance.append(test_data.toRecord());
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
    var log_instance = try createLogWithConfig(config, test_dir, allocator);
    defer log_instance.close();

    const test_data = try TestData.init(record_size, allocator);
    defer test_data.deinit();

    var offsets = try allocator.alloc(u64, num_writes);
    defer allocator.free(offsets);

    var i: u64 = 0;
    while (i < num_writes) : (i += 1) {
        offsets[i] = try log_instance.append(test_data.toRecord());
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

        if (!std.mem.eql(u8, read_rec.value, test_data.value)) {
            return error.IncorrectRead;
        }
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Random Read Latency ({d} reads from {d} records)", .{ num_reads, num_writes });
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_reads, latencies, allocator);
}

pub fn benchSequentialReadLatency(allocator: std.mem.Allocator, num_writes: u64, record_size: usize) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const config = LogConfig.default();
    var log_instance = try createLogWithConfig(config, test_dir, allocator);
    defer log_instance.close();

    const test_data = try TestData.init(record_size, allocator);
    defer test_data.deinit();

    var offsets = try allocator.alloc(u64, num_writes);
    defer allocator.free(offsets);

    var i: u64 = 0;
    while (i < num_writes) : (i += 1) {
        offsets[i] = try log_instance.append(test_data.toRecord());
    }

    var latencies = try allocator.alloc(u64, num_writes);
    errdefer allocator.free(latencies);

    i = 0;
    while (i < num_writes) : (i += 1) {
        var timer = Timer.start();
        const read_rec = try log_instance.read(offsets[i], allocator);
        latencies[i] = timer.elapsed();

        if (!std.mem.eql(u8, read_rec.value, test_data.value)) {
            return error.IncorrectRead;
        }
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Sequential Read Latency ({d} records, {d}B each)", .{ num_writes, record_size });
    defer allocator.free(bench_name);

    return BenchmarkResult.init(bench_name, num_writes, latencies, allocator);
}

pub fn benchSegmentRotation(allocator: std.mem.Allocator, record_size: usize) !BenchmarkResult {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const segment_config = SegmentConfig.init(1024 * 1024, 10 * 1024);
    const config = LogConfig{
        .segment_config = segment_config,
        .initial_offset = 0,
    };

    var log_instance = try createLogWithConfig(config, test_dir, allocator);
    defer log_instance.close();

    const test_data = try TestData.init(record_size, allocator);
    defer test_data.deinit();

    const num_records: u64 = 5000;
    var latencies = try allocator.alloc(u64, num_records);
    errdefer allocator.free(latencies);

    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        var timer = Timer.start();
        _ = try log_instance.append(test_data.toRecord());
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

fn runIndexEfficiencyBench(
    allocator: std.mem.Allocator,
    test_dir: []const u8,
    bytes_per_index: u64,
    record_size: usize,
    num_records: u64,
    num_reads: u64,
    output_dir: []const u8,
    summary_file: std.fs.File,
) !void {
    const dir_name = try std.fmt.allocPrint(allocator, "{s}/idx_{d}_rec_{d}", .{ test_dir, bytes_per_index, record_size });
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

    const key_size = @min(record_size, 512);
    const key = try allocator.alloc(u8, key_size);
    defer allocator.free(key);
    @memset(key, 'k');

    const value = try allocator.alloc(u8, record_size);
    defer allocator.free(value);
    @memset(value, 'v');

    var offsets = try allocator.alloc(u64, num_records);
    defer allocator.free(offsets);

    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        const rec = Record{ .key = key, .value = value };
        offsets[i] = try log_instance.append(rec);
    }

    var latencies = try allocator.alloc(u64, num_reads);

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp() + @as(i64, @intCast(bytes_per_index)) + @as(i64, @intCast(record_size))));
    const random = prng.random();

    i = 0;
    while (i < num_reads) : (i += 1) {
        const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
        const offset = offsets[offset_idx];

        var timer = Timer.start();
        _ = try log_instance.read(offset, allocator);
        latencies[i] = timer.elapsed();
    }

    const bench_name = try std.fmt.allocPrint(allocator, "Index Efficiency (bytes_per_index={d}, record_size={d}B, {d} reads)", .{ bytes_per_index, record_size, num_reads });
    defer allocator.free(bench_name);

    var result = try BenchmarkResult.init(bench_name, num_reads, latencies, allocator);
    defer result.deinit();
    result.print();
    try saveBenchmarkResults(&result, output_dir, allocator);
    try result.writeSummaryCSV(summary_file, allocator);
}

/// Benchmark: Index efficiency with different bytes_per_index and record sizes
pub fn benchIndexEfficiency(allocator: std.mem.Allocator, output_dir: []const u8, summary_file: std.fs.File) !void {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const bytes_per_index_values = [_]u64{ 1024, 4096, 16384 }; // 1KB, 4KB, 16KB
    const record_sizes = [_]usize{ 256, 512, 1024, 4096, 10240 }; // 256B, 512B, 1KB, 4KB, 10KB
    const num_records: u64 = 10000;
    const num_reads: u64 = 1000;

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("INDEX EFFICIENCY COMPARISON\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});

    for (record_sizes) |record_size| {
        for (bytes_per_index_values) |bytes_per_index| {
            try runIndexEfficiencyBench(allocator, test_dir, bytes_per_index, record_size, num_records, num_reads, output_dir, summary_file);
        }
    }
}

/// Benchmark: Index efficiency with random record sizes
pub fn benchIndexEfficiencyRandomSizes(allocator: std.mem.Allocator, output_dir: []const u8, summary_file: std.fs.File) !void {
    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const bytes_per_index_values = [_]u64{ 1024, 4096, 16384 }; // 1KB, 4KB, 16KB
    const num_records: u64 = 10000;
    const num_reads: u64 = 1000;

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("INDEX EFFICIENCY WITH RANDOM RECORD SIZES\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});

    for (bytes_per_index_values) |bytes_per_index| {
        const dir_name = try std.fmt.allocPrint(allocator, "{s}/idx_{d}_random", .{ test_dir, bytes_per_index });
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

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp() + @as(i64, @intCast(bytes_per_index))));
        const random = prng.random();

        var offsets = try allocator.alloc(u64, num_records);
        defer allocator.free(offsets);

        // Write records with random sizes
        var i: u64 = 0;
        while (i < num_records) : (i += 1) {
            const record_size = random.intRangeAtMost(usize, 256, 4096);
            const key_size = @min(record_size / 2, 512);

            const key = try allocator.alloc(u8, key_size);
            defer allocator.free(key);
            @memset(key, 'k');

            const value = try allocator.alloc(u8, record_size);
            defer allocator.free(value);
            @memset(value, 'v');

            const rec = Record{ .key = key, .value = value };
            offsets[i] = try log_instance.append(rec);
        }

        // Random read benchmark
        var latencies = try allocator.alloc(u64, num_reads);

        i = 0;
        while (i < num_reads) : (i += 1) {
            const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
            const offset = offsets[offset_idx];

            var timer = Timer.start();
            _ = try log_instance.read(offset, allocator);
            latencies[i] = timer.elapsed();
        }

        const bench_name = try std.fmt.allocPrint(allocator, "Index Efficiency (bytes_per_index={d}, record_size=random, {d} reads)", .{ bytes_per_index, num_reads });
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

    const test_dir = try createTestDir(allocator);
    defer {
        cleanupTestDir(test_dir);
        allocator.free(test_dir);
    }

    const output_dir = "/tmp/zue_bench_results";
    std.fs.makeDirAbsolute(output_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };

    // Test configuration
    const record_sizes = [_]usize{ 256, 512, 1024, 4096, 10240 }; // 256B, 512B, 1KB, 4KB, 10KB
    const bytes_per_index_values = [_]u64{ 1024, 4096, 16384 }; // 1KB, 4KB, 16KB
    const chosen_index_granularity: u64 = 4096; // 4KB - chosen based on index efficiency results
    const num_records: u64 = 10000;
    const num_reads: u64 = 1000;

    // ==================== INDEX EFFICIENCY BENCHMARK ====================
    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("INDEX EFFICIENCY BENCHMARK\n", .{});
    std.debug.print("Testing {d} record sizes × {d} index granularities\n", .{ record_sizes.len, bytes_per_index_values.len });
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    const index_csv_path = try std.fmt.allocPrint(allocator, "{s}/index_efficiency.csv", .{output_dir});
    defer allocator.free(index_csv_path);
    const index_csv = try std.fs.createFileAbsolute(index_csv_path, .{});
    defer index_csv.close();
    try index_csv.writeAll("record_size_bytes,bytes_per_index,avg_latency_us,p50_us,p95_us,p99_us,p999_us\n");

    // Index efficiency for fixed record sizes
    for (record_sizes) |record_size| {
        std.debug.print("Testing record size: {d}B\n", .{record_size});

        for (bytes_per_index_values) |bytes_per_index| {
            const dir_name = try std.fmt.allocPrint(allocator, "{s}/idx_{d}_rec_{d}", .{ test_dir, bytes_per_index, record_size });
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

            const key_size = @min(record_size, 512);
            const key = try allocator.alloc(u8, key_size);
            defer allocator.free(key);
            @memset(key, 'k');

            const value = try allocator.alloc(u8, record_size);
            defer allocator.free(value);
            @memset(value, 'v');

            var offsets = try allocator.alloc(u64, num_records);
            defer allocator.free(offsets);

            var i: u64 = 0;
            while (i < num_records) : (i += 1) {
                const rec = Record{ .key = key, .value = value };
                offsets[i] = try log_instance.append(rec);
            }

            var latencies = try allocator.alloc(u64, num_reads);
            defer allocator.free(latencies);

            var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp() + @as(i64, @intCast(bytes_per_index)) + @as(i64, @intCast(record_size))));
            const random = prng.random();

            i = 0;
            while (i < num_reads) : (i += 1) {
                const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
                const offset = offsets[offset_idx];

                var timer = Timer.start();
                _ = try log_instance.read(offset, allocator);
                latencies[i] = timer.elapsed();
            }

            const stats = LatencyStats.compute(latencies, allocator);

            const line = try std.fmt.allocPrint(allocator, "{d},{d},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2}\n", .{
                record_size,
                bytes_per_index,
                stats.avg_us,
                stats.p50_us,
                stats.p95_us,
                stats.p99_us,
                stats.p999_us,
            });
            defer allocator.free(line);
            try index_csv.writeAll(line);

            std.debug.print("  Index {d}B:", .{bytes_per_index});
            stats.print();
        }
        std.debug.print("\n", .{});
    }

    // Index efficiency with random record sizes
    std.debug.print("Testing record size: random (256B-10KB)\n", .{});

    for (bytes_per_index_values) |bytes_per_index| {
        const dir_name = try std.fmt.allocPrint(allocator, "{s}/idx_{d}_rec_random", .{ test_dir, bytes_per_index });
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

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp() + @as(i64, @intCast(bytes_per_index))));
        const random = prng.random();

        var offsets = try allocator.alloc(u64, num_records);
        defer allocator.free(offsets);

        var i: u64 = 0;
        while (i < num_records) : (i += 1) {
            const rec_size = random.intRangeAtMost(usize, 256, 10240);
            const key_size = @min(rec_size / 2, 512);

            const key = try allocator.alloc(u8, key_size);
            defer allocator.free(key);
            @memset(key, 'k');

            const value = try allocator.alloc(u8, rec_size);
            defer allocator.free(value);
            @memset(value, 'v');

            const rec = Record{ .key = key, .value = value };
            offsets[i] = try log_instance.append(rec);
        }

        var latencies = try allocator.alloc(u64, num_reads);
        defer allocator.free(latencies);

        i = 0;
        while (i < num_reads) : (i += 1) {
            const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
            const offset = offsets[offset_idx];

            var timer = Timer.start();
            _ = try log_instance.read(offset, allocator);
            latencies[i] = timer.elapsed();
        }

        const stats = LatencyStats.compute(latencies, allocator);

        const line = try std.fmt.allocPrint(allocator, "random,{d},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2}\n", .{
            bytes_per_index,
            stats.avg_us,
            stats.p50_us,
            stats.p95_us,
            stats.p99_us,
            stats.p999_us,
        });
        defer allocator.free(line);
        try index_csv.writeAll(line);

        std.debug.print("  Index {d}B:", .{bytes_per_index});
        stats.print();
    }
    std.debug.print("\n", .{});

    std.debug.print("=" ** 80 ++ "\n", .{});
    std.debug.print("Index efficiency results saved to: {s}\n", .{index_csv_path});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    // ==================== SEQUENTIAL & RANDOM READ BENCHMARKS ====================
    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("READ LATENCY BENCHMARKS (4KB Index Granularity)\n", .{});
    std.debug.print("Testing {d} record sizes + random\n", .{record_sizes.len});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    const seq_csv_path = try std.fmt.allocPrint(allocator, "{s}/sequential_reads.csv", .{output_dir});
    defer allocator.free(seq_csv_path);
    const seq_csv = try std.fs.createFileAbsolute(seq_csv_path, .{});
    defer seq_csv.close();
    try seq_csv.writeAll("record_size_bytes,avg_latency_us,p50_us,p95_us,p99_us,p999_us\n");

    const rand_csv_path = try std.fmt.allocPrint(allocator, "{s}/random_reads.csv", .{output_dir});
    defer allocator.free(rand_csv_path);
    const rand_csv = try std.fs.createFileAbsolute(rand_csv_path, .{});
    defer rand_csv.close();
    try rand_csv.writeAll("record_size_bytes,avg_latency_us,p50_us,p95_us,p99_us,p999_us\n");

    // Test fixed record sizes
    for (record_sizes) |record_size| {
        std.debug.print("Testing record size: {d}B\n", .{record_size});

        const dir_name = try std.fmt.allocPrint(allocator, "{s}/read_{d}", .{ test_dir, record_size });
        defer allocator.free(dir_name);
        try std.fs.makeDirAbsolute(dir_name);

        var segment_config = SegmentConfig.default();
        segment_config.index_config.bytes_per_index = chosen_index_granularity;
        const config = LogConfig{
            .segment_config = segment_config,
            .initial_offset = 0,
        };

        var log_instance = try Log.create(config, dir_name, allocator);
        defer log_instance.close();

        const key_size = @min(record_size, 512);
        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, record_size);
        defer allocator.free(value);
        @memset(value, 'v');

        var offsets = try allocator.alloc(u64, num_records);
        defer allocator.free(offsets);

        var i: u64 = 0;
        while (i < num_records) : (i += 1) {
            const rec = Record{ .key = key, .value = value };
            offsets[i] = try log_instance.append(rec);
        }

        // Sequential reads
        var seq_latencies = try allocator.alloc(u64, num_records);
        defer allocator.free(seq_latencies);

        i = 0;
        while (i < num_records) : (i += 1) {
            var timer = Timer.start();
            _ = try log_instance.read(offsets[i], allocator);
            seq_latencies[i] = timer.elapsed();
        }

        const seq_stats = LatencyStats.compute(seq_latencies, allocator);
        const record_size_str = try std.fmt.allocPrint(allocator, "{d}", .{record_size});
        defer allocator.free(record_size_str);
        try seq_stats.writeCsv(seq_csv, record_size_str, allocator);

        std.debug.print("  Sequential:", .{});
        seq_stats.print();

        var rand_latencies = try allocator.alloc(u64, num_reads);
        defer allocator.free(rand_latencies);

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp() + @as(i64, @intCast(record_size))));
        const random = prng.random();

        i = 0;
        while (i < num_reads) : (i += 1) {
            const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
            var timer = Timer.start();
            _ = try log_instance.read(offsets[offset_idx], allocator);
            rand_latencies[i] = timer.elapsed();
        }

        const rand_stats = LatencyStats.compute(rand_latencies, allocator);
        try rand_stats.writeCsv(rand_csv, record_size_str, allocator);

        std.debug.print("  Random:", .{});
        rand_stats.print();
        std.debug.print("\n", .{});
    }

    // Test with random record sizes
    std.debug.print("Testing record size: random (256B-10KB)\n", .{});

    const rand_dir_name = try std.fmt.allocPrint(allocator, "{s}/rec_random", .{test_dir});
    defer allocator.free(rand_dir_name);
    try std.fs.makeDirAbsolute(rand_dir_name);

    var segment_config = SegmentConfig.default();
    segment_config.index_config.bytes_per_index = chosen_index_granularity;
    const rand_config = LogConfig{
        .segment_config = segment_config,
        .initial_offset = 0,
    };

    var rand_log = try Log.create(rand_config, rand_dir_name, allocator);
    defer rand_log.close();

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    const random = prng.random();

    var rand_offsets = try allocator.alloc(u64, num_records);
    defer allocator.free(rand_offsets);

    // Write records with random sizes
    var i: u64 = 0;
    while (i < num_records) : (i += 1) {
        const rec_size = random.intRangeAtMost(usize, 256, 10240);
        const key_size = @min(rec_size / 2, 512);

        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, rec_size);
        defer allocator.free(value);
        @memset(value, 'v');

        const rec = Record{ .key = key, .value = value };
        rand_offsets[i] = try rand_log.append(rec);
    }

    // Sequential reads
    var rand_seq_latencies = try allocator.alloc(u64, num_records);
    defer allocator.free(rand_seq_latencies);

    i = 0;
    while (i < num_records) : (i += 1) {
        var timer = Timer.start();
        _ = try rand_log.read(rand_offsets[i], allocator);
        rand_seq_latencies[i] = timer.elapsed();
    }

    const rand_seq_stats = LatencyStats.compute(rand_seq_latencies, allocator);
    try rand_seq_stats.writeCsv(seq_csv, "random", allocator);

    std.debug.print("  Sequential:", .{});
    rand_seq_stats.print();

    var rand_rand_latencies = try allocator.alloc(u64, num_reads);
    defer allocator.free(rand_rand_latencies);

    i = 0;
    while (i < num_reads) : (i += 1) {
        const offset_idx = random.intRangeAtMost(u64, 0, num_records - 1);
        var timer = Timer.start();
        _ = try rand_log.read(rand_offsets[offset_idx], allocator);
        rand_rand_latencies[i] = timer.elapsed();
    }

    const rand_rand_stats = LatencyStats.compute(rand_rand_latencies, allocator);
    try rand_rand_stats.writeCsv(rand_csv, "random", allocator);

    std.debug.print("  Random:", .{});
    rand_rand_stats.print();
    std.debug.print("\n", .{});

    std.debug.print("=" ** 80 ++ "\n", .{});
    std.debug.print("Read latency results saved to:\n", .{});
    std.debug.print("  Sequential reads: {s}\n", .{seq_csv_path});
    std.debug.print("  Random reads: {s}\n", .{rand_csv_path});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    // ==================== APPEND LATENCY BENCHMARK ====================
    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("APPEND LATENCY BENCHMARK (4KB Index Granularity)\n", .{});
    std.debug.print("Testing {d} record sizes + random\n", .{record_sizes.len});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    const append_csv_path = try std.fmt.allocPrint(allocator, "{s}/append_latencies.csv", .{output_dir});
    defer allocator.free(append_csv_path);
    const append_csv = try std.fs.createFileAbsolute(append_csv_path, .{});
    defer append_csv.close();
    try append_csv.writeAll("record_size_bytes,avg_latency_us,p50_us,p95_us,p99_us,p999_us\n");

    // Test fixed record sizes
    for (record_sizes) |record_size| {
        std.debug.print("Testing record size: {d}B\n", .{record_size});

        const dir_name = try std.fmt.allocPrint(allocator, "{s}/append_{d}", .{ test_dir, record_size });
        defer allocator.free(dir_name);
        try std.fs.makeDirAbsolute(dir_name);

        var append_seg_config = SegmentConfig.default();
        append_seg_config.index_config.bytes_per_index = chosen_index_granularity;
        const config = LogConfig{
            .segment_config = append_seg_config,
            .initial_offset = 0,
        };

        var log_instance = try Log.create(config, dir_name, allocator);
        defer log_instance.close();

        const key_size = @min(record_size, 512);
        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, record_size);
        defer allocator.free(value);
        @memset(value, 'v');

        // Append benchmark
        var append_latencies = try allocator.alloc(u64, num_records);
        defer allocator.free(append_latencies);

        var j: u64 = 0;
        while (j < num_records) : (j += 1) {
            const rec = Record{ .key = key, .value = value };
            var timer = Timer.start();
            _ = try log_instance.append(rec);
            append_latencies[j] = timer.elapsed();
        }

        const append_stats = LatencyStats.compute(append_latencies, allocator);
        const rec_size_str = try std.fmt.allocPrint(allocator, "{d}", .{record_size});
        defer allocator.free(rec_size_str);
        try append_stats.writeCsv(append_csv, rec_size_str, allocator);

        std.debug.print("  Append:", .{});
        append_stats.print();
        std.debug.print("\n", .{});
    }

    // Test with random record sizes
    std.debug.print("Testing record size: random (256B-10KB)\n", .{});

    const append_rand_dir = try std.fmt.allocPrint(allocator, "{s}/append_random", .{test_dir});
    defer allocator.free(append_rand_dir);
    try std.fs.makeDirAbsolute(append_rand_dir);

    var append_rand_seg_config = SegmentConfig.default();
    append_rand_seg_config.index_config.bytes_per_index = chosen_index_granularity;
    const append_rand_config = LogConfig{
        .segment_config = append_rand_seg_config,
        .initial_offset = 0,
    };

    var append_rand_log = try Log.create(append_rand_config, append_rand_dir, allocator);
    defer append_rand_log.close();

    var append_prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    const append_random = append_prng.random();

    var append_rand_latencies = try allocator.alloc(u64, num_records);
    defer allocator.free(append_rand_latencies);

    var j: u64 = 0;
    while (j < num_records) : (j += 1) {
        const rec_size = append_random.intRangeAtMost(usize, 256, 10240);
        const key_size = @min(rec_size / 2, 512);

        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, rec_size);
        defer allocator.free(value);
        @memset(value, 'v');

        const rec = Record{ .key = key, .value = value };
        var timer = Timer.start();
        _ = try append_rand_log.append(rec);
        append_rand_latencies[j] = timer.elapsed();
    }

    const append_rand_stats = LatencyStats.compute(append_rand_latencies, allocator);
    try append_rand_stats.writeCsv(append_csv, "random", allocator);

    std.debug.print("  Append:", .{});
    append_rand_stats.print();
    std.debug.print("\n", .{});

    std.debug.print("=" ** 80 ++ "\n", .{});
    std.debug.print("Append latency results saved to: {s}\n", .{append_csv_path});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    // ==================== INDEXING OVERHEAD BENCHMARK ====================
    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("INDEX CREATION OVERHEAD ANALYSIS\n", .{});
    std.debug.print("Comparing appends that trigger index creation vs normal appends\n", .{});
    std.debug.print("Testing {d} record sizes + random with 4KB indexing\n", .{record_sizes.len});
    std.debug.print("=" ** 80 ++ "\n\n", .{});

    const index_creating_csv_path = try std.fmt.allocPrint(allocator, "{s}/append_with_index_creation.csv", .{output_dir});
    defer allocator.free(index_creating_csv_path);
    const index_creating_csv = try std.fs.createFileAbsolute(index_creating_csv_path, .{});
    defer index_creating_csv.close();
    try index_creating_csv.writeAll("record_size_bytes,avg_latency_us,p50_us,p95_us,p99_us,p999_us,num_samples\n");

    const normal_append_csv_path = try std.fmt.allocPrint(allocator, "{s}/append_normal.csv", .{output_dir});
    defer allocator.free(normal_append_csv_path);
    const normal_append_csv = try std.fs.createFileAbsolute(normal_append_csv_path, .{});
    defer normal_append_csv.close();
    try normal_append_csv.writeAll("record_size_bytes,avg_latency_us,p50_us,p95_us,p99_us,p999_us,num_samples\n");

    // Test fixed record sizes
    for (record_sizes) |record_size| {
        std.debug.print("Testing record size: {d}B\n", .{record_size});

        const dir_name = try std.fmt.allocPrint(allocator, "{s}/idx_overhead_{d}", .{ test_dir, record_size });
        defer allocator.free(dir_name);
        try std.fs.makeDirAbsolute(dir_name);

        var seg_config = SegmentConfig.default();
        seg_config.index_config.bytes_per_index = chosen_index_granularity;
        const config = LogConfig{
            .segment_config = seg_config,
            .initial_offset = 0,
        };

        var log_instance = try Log.create(config, dir_name, allocator);
        defer log_instance.close();

        const key_size = @min(record_size, 512);
        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, record_size);
        defer allocator.free(value);
        @memset(value, 'v');

        // Calculate approximate record size on disk (header + key_len + value_len + key + value)
        // Record format: header (timestamp 8B + tombstone 1B) + key_len (8B) + value_len (8B) + key + value
        const record_disk_size = 25 + key_size + record_size;

        // Allocate arrays to hold latencies
        var index_creating_latencies = try allocator.alloc(u64, num_records);
        defer allocator.free(index_creating_latencies);
        var idx_count: usize = 0;

        var normal_latencies = try allocator.alloc(u64, num_records);
        defer allocator.free(normal_latencies);
        var norm_count: usize = 0;

        var bytes_since_last_index: u64 = 0;

        var k: u64 = 0;
        while (k < num_records) : (k += 1) {
            const rec = Record{ .key = key, .value = value };

            // Check if this append will cross index boundary
            const will_create_index = (bytes_since_last_index + record_disk_size) >= chosen_index_granularity;

            var timer = Timer.start();
            _ = try log_instance.append(rec);
            const latency = timer.elapsed();

            if (will_create_index) {
                index_creating_latencies[idx_count] = latency;
                idx_count += 1;
                bytes_since_last_index = (bytes_since_last_index + record_disk_size) % chosen_index_granularity;
            } else {
                normal_latencies[norm_count] = latency;
                norm_count += 1;
                bytes_since_last_index += record_disk_size;
            }
        }

        if (idx_count > 0) {
            const idx_slice = index_creating_latencies[0..idx_count];
            const idx_stats = LatencyStats.compute(idx_slice, allocator);
            const rec_size_str = try std.fmt.allocPrint(allocator, "{d}", .{record_size});
            defer allocator.free(rec_size_str);
            try idx_stats.writeCsvWithSamples(index_creating_csv, rec_size_str, idx_count, allocator);

            std.debug.print("  Index-creating appends ({d} samples):", .{idx_count});
            idx_stats.print();
        }

        if (norm_count > 0) {
            const norm_slice = normal_latencies[0..norm_count];
            const norm_stats = LatencyStats.compute(norm_slice, allocator);
            const rec_size_str = try std.fmt.allocPrint(allocator, "{d}", .{record_size});
            defer allocator.free(rec_size_str);
            try norm_stats.writeCsvWithSamples(normal_append_csv, rec_size_str, norm_count, allocator);

            std.debug.print("  Normal appends ({d} samples):", .{norm_count});
            norm_stats.print();
        }

        std.debug.print("\n", .{});
    }

    // Test with random record sizes
    std.debug.print("Testing record size: random (256B-10KB)\n", .{});

    const overhead_rand_dir = try std.fmt.allocPrint(allocator, "{s}/idx_overhead_random", .{test_dir});
    defer allocator.free(overhead_rand_dir);
    try std.fs.makeDirAbsolute(overhead_rand_dir);

    var overhead_rand_seg_config = SegmentConfig.default();
    overhead_rand_seg_config.index_config.bytes_per_index = chosen_index_granularity;
    const overhead_rand_config = LogConfig{
        .segment_config = overhead_rand_seg_config,
        .initial_offset = 0,
    };

    var overhead_rand_log = try Log.create(overhead_rand_config, overhead_rand_dir, allocator);
    defer overhead_rand_log.close();

    var overhead_prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));
    const overhead_random = overhead_prng.random();

    var rand_index_creating_latencies = try allocator.alloc(u64, num_records);
    defer allocator.free(rand_index_creating_latencies);
    var rand_idx_count: usize = 0;

    var rand_normal_latencies = try allocator.alloc(u64, num_records);
    defer allocator.free(rand_normal_latencies);
    var rand_norm_count: usize = 0;

    var rand_bytes_since_last_index: u64 = 0;

    var k: u64 = 0;
    while (k < num_records) : (k += 1) {
        const rec_size = overhead_random.intRangeAtMost(usize, 256, 10240);
        const key_size = @min(rec_size / 2, 512);

        const key = try allocator.alloc(u8, key_size);
        defer allocator.free(key);
        @memset(key, 'k');

        const value = try allocator.alloc(u8, rec_size);
        defer allocator.free(value);
        @memset(value, 'v');

        const record_disk_size = 25 + key_size + rec_size;

        const will_create_index = (rand_bytes_since_last_index + record_disk_size) >= chosen_index_granularity;

        const rec = Record{ .key = key, .value = value };
        var timer = Timer.start();
        _ = try overhead_rand_log.append(rec);
        const latency = timer.elapsed();

        if (will_create_index) {
            rand_index_creating_latencies[rand_idx_count] = latency;
            rand_idx_count += 1;
            rand_bytes_since_last_index = (rand_bytes_since_last_index + record_disk_size) % chosen_index_granularity;
        } else {
            rand_normal_latencies[rand_norm_count] = latency;
            rand_norm_count += 1;
            rand_bytes_since_last_index += record_disk_size;
        }
    }

    if (rand_idx_count > 0) {
        const idx_slice = rand_index_creating_latencies[0..rand_idx_count];
        const idx_stats = LatencyStats.compute(idx_slice, allocator);
        try idx_stats.writeCsvWithSamples(index_creating_csv, "random", rand_idx_count, allocator);

        std.debug.print("  Index-creating appends ({d} samples):", .{rand_idx_count});
        idx_stats.print();
    }

    if (rand_norm_count > 0) {
        const norm_slice = rand_normal_latencies[0..rand_norm_count];
        const norm_stats = LatencyStats.compute(norm_slice, allocator);
        try norm_stats.writeCsvWithSamples(normal_append_csv, "random", rand_norm_count, allocator);

        std.debug.print("  Normal appends ({d} samples):", .{rand_norm_count});
        norm_stats.print();
    }

    std.debug.print("\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});
    std.debug.print("Index creation overhead results saved to:\n", .{});
    std.debug.print("  Index-creating appends: {s}\n", .{index_creating_csv_path});
    std.debug.print("  Normal appends: {s}\n", .{normal_append_csv_path});
    std.debug.print("=" ** 80 ++ "\n\n", .{});
}

const LatencyStats = struct {
    avg_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p999_us: f64,

    fn compute(latencies: []u64, allocator: std.mem.Allocator) LatencyStats {
        const avg = avgLatencyHelper(latencies) / 1000.0;
        return .{
            .avg_us = avg,
            .p50_us = @as(f64, @floatFromInt(percentileHelper(latencies, 0.50, allocator))) / 1000.0,
            .p95_us = @as(f64, @floatFromInt(percentileHelper(latencies, 0.95, allocator))) / 1000.0,
            .p99_us = @as(f64, @floatFromInt(percentileHelper(latencies, 0.99, allocator))) / 1000.0,
            .p999_us = @as(f64, @floatFromInt(percentileHelper(latencies, 0.999, allocator))) / 1000.0,
        };
    }

    fn print(self: LatencyStats) void {
        std.debug.print("  avg={d:.2}µs p50={d:.2}µs p95={d:.2}µs p99={d:.2}µs\n", .{ self.avg_us, self.p50_us, self.p95_us, self.p99_us });
    }

    fn writeCsv(self: LatencyStats, file: std.fs.File, record_size_label: []const u8, allocator: std.mem.Allocator) !void {
        const line = try std.fmt.allocPrint(allocator, "{s},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2}\n", .{
            record_size_label,
            self.avg_us,
            self.p50_us,
            self.p95_us,
            self.p99_us,
            self.p999_us,
        });
        defer allocator.free(line);
        try file.writeAll(line);
    }

    fn writeCsvWithSamples(self: LatencyStats, file: std.fs.File, record_size_label: []const u8, num_samples: usize, allocator: std.mem.Allocator) !void {
        const line = try std.fmt.allocPrint(allocator, "{s},{d:.2},{d:.2},{d:.2},{d:.2},{d:.2},{d}\n", .{
            record_size_label,
            self.avg_us,
            self.p50_us,
            self.p95_us,
            self.p99_us,
            self.p999_us,
            num_samples,
        });
        defer allocator.free(line);
        try file.writeAll(line);
    }
};

fn avgLatencyHelper(latencies: []u64) f64 {
    if (latencies.len == 0) return 0.0;
    var sum: u64 = 0;
    for (latencies) |lat| {
        sum += lat;
    }
    return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(latencies.len));
}

fn percentileHelper(latencies: []u64, p: f64, allocator: std.mem.Allocator) u64 {
    if (latencies.len == 0) return 0;

    const sorted = allocator.dupe(u64, latencies) catch return 0;
    defer allocator.free(sorted);

    std.mem.sort(u64, sorted, {}, comptime std.sort.asc(u64));

    const idx = @as(usize, @intFromFloat(@as(f64, @floatFromInt(sorted.len)) * p));
    const clamped = @min(idx, sorted.len - 1);
    return sorted[clamped];
}
