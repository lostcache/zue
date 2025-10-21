const std = @import("std");
const LogIndex = @import("log_index.zig");
const Record = @import("record.zig");

pub const SegmentConfig = struct {
    log_config: Record.OnDiskLogConfig,
    index_config: LogIndex.OnDiskIndexConfig,

    /// Create a SegmentConfig with default values
    pub fn default() SegmentConfig {
        return SegmentConfig{
            .log_config = Record.OnDiskLogConfig{},
            .index_config = LogIndex.OnDiskIndexConfig{},
        };
    }

    /// Create a SegmentConfig with custom log and index file sizes
    pub fn init(log_file_max_bytes: u64, index_file_max_bytes: u64) SegmentConfig {
        return SegmentConfig{
            .log_config = Record.OnDiskLogConfig{
                .log_file_max_size_bytes = log_file_max_bytes,
            },
            .index_config = LogIndex.OnDiskIndexConfig{
                .index_file_max_size_bytes = index_file_max_bytes,
            },
        };
    }

    /// Create a SegmentConfig with custom key/value size limits
    pub fn withLimits(
        log_file_max_bytes: u64,
        index_file_max_bytes: u64,
        key_max_bytes: u64,
        value_max_bytes: u64,
    ) SegmentConfig {
        return SegmentConfig{
            .log_config = Record.OnDiskLogConfig{
                .log_file_max_size_bytes = log_file_max_bytes,
                .key_max_size_bytes = key_max_bytes,
                .value_max_size_bytes = value_max_bytes,
            },
            .index_config = LogIndex.OnDiskIndexConfig{
                .index_file_max_size_bytes = index_file_max_bytes,
            },
        };
    }
};

pub const Segment = struct {
    config: SegmentConfig,
    base_offset: u64,
    next_relative_offset: u32, // Track next relative offset for appends

    // Log file management
    log_file: std.fs.File,
    log_file_name: []const u8,
    log_file_size: u64,

    // Index file management
    index_file: std.fs.File,
    index_file_name: []const u8,
    index_file_size: u64,

    fn getAllocLogFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}.log", .{ base_path, offset });
    }

    fn getAllocIndexFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}.index", .{ base_path, offset });
    }

    pub fn create(config: SegmentConfig, abs_base_path: []const u8, base_offset: u64, gpa_alloc: std.mem.Allocator) !Segment {
        // Create log file
        const log_file_path = try Segment.getAllocLogFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(log_file_path);

        const log_file = try std.fs.createFileAbsolute(log_file_path, .{ .truncate = true, .read = true });
        errdefer log_file.close();

        // Create index file
        const index_file_path = try Segment.getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index_file = try std.fs.createFileAbsolute(index_file_path, .{ .truncate = true, .read = true });
        errdefer index_file.close();

        return Segment{
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = 0,
            .log_file = log_file,
            .log_file_name = log_file_path,
            .log_file_size = 0,
            .index_file = index_file,
            .index_file_name = index_file_path,
            .index_file_size = 0,
        };
    }

    pub fn open(config: SegmentConfig, abs_base_path: []const u8, base_offset: u64, gpa_alloc: std.mem.Allocator) !Segment {
        // Open log file
        const log_file_path = try Segment.getAllocLogFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(log_file_path);

        const log_file = try std.fs.openFileAbsolute(log_file_path, .{ .mode = .read_write });
        errdefer log_file.close();

        const log_stat = try log_file.stat();
        const log_file_size = log_stat.size;

        // Open index file
        const index_file_path = try Segment.getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index_file = try std.fs.openFileAbsolute(index_file_path, .{ .mode = .read_write });
        errdefer index_file.close();

        const index_stat = try index_file.stat();
        const index_file_size = index_stat.size;

        // Calculate next relative offset from index
        const next_relative_offset: u32 = @intCast(LogIndex.OnDiskIndex.getEntryCount(index_file_size));

        return Segment{
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = next_relative_offset,
            .log_file = log_file,
            .log_file_name = log_file_path,
            .log_file_size = log_file_size,
            .index_file = index_file,
            .index_file_name = index_file_path,
            .index_file_size = index_file_size,
        };
    }

    pub fn close(self: *Segment) void {
        self.log_file.close();
        self.index_file.close();
    }

    pub fn delete(self: *Segment, gpa_alloc: std.mem.Allocator) !void {
        self.log_file.close();
        self.index_file.close();
        try std.fs.deleteFileAbsolute(self.log_file_name);
        try std.fs.deleteFileAbsolute(self.index_file_name);
        gpa_alloc.free(self.log_file_name);
        gpa_alloc.free(self.index_file_name);
    }

    pub fn append(self: *Segment, record: Record.Record) !void {
        // Check if log file is full
        const record_size = Record.OnDiskLog.serializedSize(self.config.log_config, record);
        if (self.log_file_size + record_size > self.config.log_config.log_file_max_size_bytes) {
            return error.LogFileFull;
        }

        // Check if index file is full
        if (LogIndex.OnDiskIndex.isFull(self.config.index_config, self.index_file_size)) {
            return error.IndexFileFull;
        }

        // Write record to log file
        const record_pos = self.log_file_size;
        try self.log_file.seekTo(record_pos);
        try Record.OnDiskLog.serialize(self.config.log_config, record, self.log_file);
        self.log_file_size += record_size;

        // Add index entry for this record
        _ = try LogIndex.OnDiskIndex.add(
            self.config.index_config,
            self.index_file,
            &self.index_file_size,
            self.next_relative_offset,
            @intCast(record_pos),
        );

        self.next_relative_offset += 1;
    }

    pub fn read(self: *Segment, offset: u64, allocator: std.mem.Allocator) !Record.Record {
        // Convert absolute offset to relative offset
        if (offset < self.base_offset) {
            return error.OffsetOutOfBounds;
        }
        const relative_offset: u32 = @intCast(offset - self.base_offset);

        // Lookup position in index
        const index_entry = try LogIndex.OnDiskIndex.lookup(
            self.config.index_config,
            self.index_file,
            self.index_file_size,
            relative_offset,
        );

        // Read record from log file
        try self.log_file.seekTo(index_entry.pos);
        return try Record.OnDiskLog.deserialize(self.config.log_config, self.log_file, allocator);
    }

    pub fn isFull(self: *Segment) bool {
        return LogIndex.OnDiskIndex.isFull(self.config.index_config, self.index_file_size);
    }

    pub fn getSize(self: *Segment) u64 {
        return self.log_file_size;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "SegmentConfig: default configuration" {
    const config = SegmentConfig.default();

    // Verify default values
    try std.testing.expectEqual(@as(u64, 1024 * 1024 * 1024), config.log_config.log_file_max_size_bytes);
    try std.testing.expectEqual(@as(u64, 1024 * 1024 * 10), config.index_config.index_file_max_size_bytes);
}

test "SegmentConfig: init with custom sizes" {
    const config = SegmentConfig.init(512 * 1024 * 1024, 5 * 1024 * 1024);

    try std.testing.expectEqual(@as(u64, 512 * 1024 * 1024), config.log_config.log_file_max_size_bytes);
    try std.testing.expectEqual(@as(u64, 5 * 1024 * 1024), config.index_config.index_file_max_size_bytes);
}

test "SegmentConfig: withLimits for custom record sizes" {
    const config = SegmentConfig.withLimits(
        1024 * 1024 * 1024, // 1GB log
        10 * 1024 * 1024,   // 10MB index
        512,                 // 512B max key
        2048,                // 2KB max value
    );

    try std.testing.expectEqual(@as(u64, 512), config.log_config.key_max_size_bytes);
    try std.testing.expectEqual(@as(u64, 2048), config.log_config.value_max_size_bytes);
}

test "Segment: create new segment" {
    const test_dir = "test_segment_create";
    const test_offset: u64 = 1000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    // Use default configuration
    const config = SegmentConfig.default();

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    // Verify initial state
    try std.testing.expectEqual(test_offset, segment.base_offset);
    try std.testing.expectEqual(@as(u64, 0), segment.log_file_size);
    try std.testing.expectEqual(@as(u64, 0), segment.index_file_size);
}

test "Segment: open existing segment" {
    const test_dir = "test_segment_open";
    const test_offset: u64 = 2000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    // Create segment first
    var segment1 = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    const log_name = segment1.log_file_name;
    const index_name = segment1.index_file_name;
    defer std.testing.allocator.free(log_name);
    defer std.testing.allocator.free(index_name);
    segment1.close();

    // Open existing segment
    var segment2 = try Segment.open(config, abs_path, test_offset, std.testing.allocator);
    defer segment2.delete(std.testing.allocator) catch {};

    try std.testing.expectEqual(test_offset, segment2.base_offset);
}

test "Segment: append single record" {
    const test_dir = "test_segment_append";
    const test_offset: u64 = 3000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    const record = Record.Record{ .key = "test-key", .value = "test-value" };
    try segment.append(record);

    // Verify sizes updated
    try std.testing.expect(segment.log_file_size > 0);
    try std.testing.expect(segment.index_file_size > 0);
    try std.testing.expectEqual(LogIndex.INDEX_SIZE_BYTES, segment.index_file_size);
}

test "Segment: append and read multiple records" {
    const test_dir = "test_segment_append_read";
    const test_offset: u64 = 4000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    // Append multiple records
    const record1 = Record.Record{ .key = "key1", .value = "value1" };
    const record2 = Record.Record{ .key = "key2", .value = "value2" };
    const record3 = Record.Record{ .key = null, .value = "value3" };

    try segment.append(record1);
    try segment.append(record2);
    try segment.append(record3);

    // Read back records
    const read1 = try segment.read(test_offset, std.testing.allocator);
    defer std.testing.allocator.free(read1.value);
    defer if (read1.key) |k| std.testing.allocator.free(k);

    try std.testing.expectEqualStrings("key1", read1.key.?);
    try std.testing.expectEqualStrings("value1", read1.value);
}

test "Segment: persistence across close/open" {
    const test_dir = "test_segment_persistence";
    const test_offset: u64 = 5000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var saved_log_size: u64 = 0;
    var saved_index_size: u64 = 0;

    // Create, write, and close
    {
        var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
        defer std.testing.allocator.free(segment.log_file_name);
        defer std.testing.allocator.free(segment.index_file_name);

        const record = Record.Record{ .key = "persist", .value = "test-data" };
        try segment.append(record);

        saved_log_size = segment.log_file_size;
        saved_index_size = segment.index_file_size;

        segment.close();
    }

    // Reopen and verify
    {
        var segment = try Segment.open(config, abs_path, test_offset, std.testing.allocator);
        defer segment.delete(std.testing.allocator) catch {};

        try std.testing.expectEqual(saved_log_size, segment.log_file_size);
        try std.testing.expectEqual(saved_index_size, segment.index_file_size);

        const read_record = try segment.read(test_offset, std.testing.allocator);
        defer std.testing.allocator.free(read_record.value);
        defer if (read_record.key) |k| std.testing.allocator.free(k);

        try std.testing.expectEqualStrings("persist", read_record.key.?);
        try std.testing.expectEqualStrings("test-data", read_record.value);
    }
}

test "Segment: log file full error" {
    const test_dir = "test_segment_log_full";
    const test_offset: u64 = 6000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const test_record = Record.Record{ .key = "k1", .value = "v1" };
    const record_size = Record.OnDiskLog.serializedSize(Record.OnDiskLogConfig{}, test_record);

    // Create config where only 1 record fits
    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{
            .log_file_max_size_bytes = record_size, // Only one record fits
            .key_max_size_bytes = 256,
            .value_max_size_bytes = 65536,
        },
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    // First append should succeed
    const record1 = Record.Record{ .key = "k1", .value = "v1" };
    try segment.append(record1);

    // Second append should fail due to log file full
    const record2 = Record.Record{ .key = "k2", .value = "v2" };
    try std.testing.expectError(error.LogFileFull, segment.append(record2));
}

test "Segment: index file full error" {
    const test_dir = "test_segment_index_full";
    const test_offset: u64 = 7000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    // Create config with very small index file size
    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{
            .index_file_max_size_bytes = LogIndex.INDEX_SIZE_BYTES * 2, // Only 2 entries
            .bytes_per_index = 256,
        },
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    // First two appends should succeed
    const record1 = Record.Record{ .key = "k1", .value = "v1" };
    const record2 = Record.Record{ .key = "k2", .value = "v2" };
    try segment.append(record1);
    try segment.append(record2);

    // Third append should fail due to index file full
    const record3 = Record.Record{ .key = "k3", .value = "v3" };
    try std.testing.expectError(error.IndexFileFull, segment.append(record3));
}

test "Segment: isFull returns correct status" {
    const test_dir = "test_segment_is_full";
    const test_offset: u64 = 8000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{
            .index_file_max_size_bytes = LogIndex.INDEX_SIZE_BYTES * 2,
            .bytes_per_index = 256,
        },
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    // Initially not full
    try std.testing.expect(!segment.isFull());

    // Add records
    const record1 = Record.Record{ .key = "k1", .value = "v1" };
    const record2 = Record.Record{ .key = "k2", .value = "v2" };
    try segment.append(record1);
    try std.testing.expect(!segment.isFull());

    try segment.append(record2);
    try std.testing.expect(segment.isFull());
}

test "Segment: read with invalid offset returns error" {
    const test_dir = "test_segment_invalid_offset";
    const test_offset: u64 = 9000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    const record = Record.Record{ .key = "key", .value = "value" };
    try segment.append(record);

    // Try to read with offset before base_offset
    try std.testing.expectError(error.OffsetOutOfBounds, segment.read(test_offset - 1, std.testing.allocator));
}

test "Segment: getSize returns correct log file size" {
    const test_dir = "test_segment_get_size";
    const test_offset: u64 = 10000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete(std.testing.allocator) catch {};

    try std.testing.expectEqual(@as(u64, 0), segment.getSize());

    const record = Record.Record{ .key = "key", .value = "value" };
    try segment.append(record);

    const expected_size = Record.OnDiskLog.serializedSize(config.log_config, record);
    try std.testing.expectEqual(expected_size, segment.getSize());
}

test "Segment: delete removes both files" {
    const test_dir = "test_segment_delete";
    const test_offset: u64 = 11000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig{
        .log_config = Record.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);

    const log_path = try std.testing.allocator.dupe(u8, segment.log_file_name);
    defer std.testing.allocator.free(log_path);
    const index_path = try std.testing.allocator.dupe(u8, segment.index_file_name);
    defer std.testing.allocator.free(index_path);

    try segment.delete(std.testing.allocator);

    // Verify files are deleted
    const log_err = std.fs.openFileAbsolute(log_path, .{});
    try std.testing.expectError(error.FileNotFound, log_err);

    const index_err = std.fs.openFileAbsolute(index_path, .{});
    try std.testing.expectError(error.FileNotFound, index_err);
}
