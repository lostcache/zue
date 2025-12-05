const std = @import("std");
const LogIndex = @import("log_index.zig");
const OnDiskLog = @import("record.zig");

pub const SegmentConfig = struct {
    log_config: OnDiskLog.OnDiskLogConfig,
    index_config: LogIndex.OnDiskIndexConfig,

    pub fn default() SegmentConfig {
        return SegmentConfig{
            .log_config = OnDiskLog.OnDiskLogConfig{},
            .index_config = LogIndex.OnDiskIndexConfig{},
        };
    }

    pub fn init(log_file_max_bytes: u64, index_file_max_bytes: u64) SegmentConfig {
        return SegmentConfig{
            .log_config = OnDiskLog.OnDiskLogConfig{
                .log_file_max_size_bytes = log_file_max_bytes,
            },
            .index_config = LogIndex.OnDiskIndexConfig{
                .index_file_max_size_bytes = index_file_max_bytes,
            },
        };
    }

    pub fn withLimits(
        log_file_max_bytes: u64,
        index_file_max_bytes: u64,
        key_max_bytes: u64,
        value_max_bytes: u64,
    ) SegmentConfig {
        return SegmentConfig{
            .log_config = OnDiskLog.OnDiskLogConfig{
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
    gpa_alloc: std.mem.Allocator,

    config: SegmentConfig,
    base_offset: u64,
    next_relative_offset: u32,

    log_file: std.fs.File,
    log_file_name: []const u8,
    log_file_size: u64,

    index_file: std.fs.File,
    index_file_name: []const u8,
    index_file_size: u64,

    bytes_since_last_index: u64,

    fn getAllocLogFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}.log", .{ base_path, offset });
    }

    fn getAllocIndexFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}.index", .{ base_path, offset });
    }

    pub fn create(config: SegmentConfig, abs_base_path: []const u8, base_offset: u64, gpa_alloc: std.mem.Allocator) !Segment {
        const log_file_path = try Segment.getAllocLogFilePath(gpa_alloc, abs_base_path, base_offset);
        var log_file_path_allocated = true;
        errdefer {
            if (log_file_path_allocated) {
                gpa_alloc.free(log_file_path);
            }
        }

        var log_file = try OnDiskLog.OnDiskLog.create(log_file_path);
        var log_file_created = true;
        errdefer {
            if (log_file_created) {
                log_file.close();
                std.fs.deleteFileAbsolute(log_file_path) catch {};
            }
        }

        const index_file_path = try Segment.getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index_file = LogIndex.OnDiskIndex.create(index_file_path) catch |err| {
            log_file.close();
            std.fs.deleteFileAbsolute(log_file_path) catch {};
            gpa_alloc.free(log_file_path);
            log_file_created = false;
            log_file_path_allocated = false;
            return err;
        };
        errdefer {
            index_file.close();
            std.fs.deleteFileAbsolute(index_file_path) catch {};
        }

        return Segment{
            .gpa_alloc = gpa_alloc,
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = 0,
            .log_file = log_file,
            .log_file_name = log_file_path,
            .log_file_size = 0,
            .index_file = index_file,
            .index_file_name = index_file_path,
            .index_file_size = 0,
            .bytes_since_last_index = 0,
        };
    }

    pub fn open(config: SegmentConfig, abs_base_path: []const u8, base_offset: u64, gpa_alloc: std.mem.Allocator) !Segment {
        const log_file_path = try Segment.getAllocLogFilePath(gpa_alloc, abs_base_path, base_offset);
        var log_file_path_allocated = true;
        errdefer {
            if (log_file_path_allocated) {
                gpa_alloc.free(log_file_path);
            }
        }

        var log_file = try OnDiskLog.OnDiskLog.open(log_file_path);
        var log_file_opened = true;
        errdefer {
            if (log_file_opened) {
                log_file.close();
            }
        }

        const log_stat = try log_file.stat();
        const log_file_size = log_stat.size;

        const index_file_path = try Segment.getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index_file = LogIndex.OnDiskIndex.open(index_file_path) catch |err| {
            log_file.close();
            gpa_alloc.free(log_file_path);
            log_file_opened = false;
            log_file_path_allocated = false;
            return err;
        };
        errdefer index_file.close();

        const index_stat = try index_file.stat();
        const index_file_size = index_stat.size;

        // Calculate next_relative_offset and bytes_since_last_index
        var next_relative_offset: u32 = 0;
        var bytes_since_last_index: u64 = 0;

        if (index_file_size > 0) {
            // Read the last index entry
            const entry_count = LogIndex.OnDiskIndex.getEntryCount(index_file_size);
            const last_index_pos = (entry_count - 1) * LogIndex.INDEX_SIZE_BYTES;

            try index_file.seekTo(last_index_pos);
            var buf: [LogIndex.INDEX_SIZE_BYTES]u8 = undefined;
            _ = try index_file.readAll(&buf);

            const last_indexed_relative_offset = std.mem.readInt(u32, buf[0..4], .little);
            const last_indexed_pos = std.mem.readInt(u32, buf[4..8], .little);

            // Count records from the last indexed position to EOF
            var arena = std.heap.ArenaAllocator.init(gpa_alloc);
            defer arena.deinit();
            const temp_alloc = arena.allocator();

            try log_file.seekTo(last_indexed_pos);
            var record_count: u32 = 0;
            while (true) {
                const current_pos = try log_file.getPos();
                if (current_pos >= log_file_size) break;

                const rec = OnDiskLog.OnDiskLog.deserialize(config.log_config, log_file, temp_alloc) catch break;
                _ = rec;
                record_count += 1;
            }

            next_relative_offset = last_indexed_relative_offset + record_count;
            bytes_since_last_index = log_file_size - last_indexed_pos;
        } else {
            // No index entries - count all records from beginning
            var arena = std.heap.ArenaAllocator.init(gpa_alloc);
            defer arena.deinit();
            const temp_alloc = arena.allocator();

            try log_file.seekTo(0);
            var record_count: u32 = 0;
            while (true) {
                const current_pos = try log_file.getPos();
                if (current_pos >= log_file_size) break;

                const rec = OnDiskLog.OnDiskLog.deserialize(config.log_config, log_file, temp_alloc) catch break;
                _ = rec;
                record_count += 1;
            }

            next_relative_offset = record_count;
            bytes_since_last_index = log_file_size;
        }

        return Segment{
            .gpa_alloc = gpa_alloc,
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = next_relative_offset,
            .log_file = log_file,
            .log_file_name = log_file_path,
            .log_file_size = log_file_size,
            .index_file = index_file,
            .index_file_name = index_file_path,
            .index_file_size = index_file_size,
            .bytes_since_last_index = bytes_since_last_index,
        };
    }

    pub fn close(self: *Segment) void {
        self.log_file.close();
        self.index_file.close();
        self.gpa_alloc.free(self.log_file_name);
        self.gpa_alloc.free(self.index_file_name);
    }

    pub fn delete(self: *Segment) !void {
        self.log_file.close();
        self.index_file.close();
        try std.fs.deleteFileAbsolute(self.log_file_name);
        try std.fs.deleteFileAbsolute(self.index_file_name);
        self.gpa_alloc.free(self.log_file_name);
        self.gpa_alloc.free(self.index_file_name);
    }

    pub fn append(self: *Segment, record: OnDiskLog.Record) !void {
        const record_size = OnDiskLog.OnDiskLog.serializedSize(self.config.log_config, record);
        if (self.log_file_size + record_size > self.config.log_config.log_file_max_size_bytes) {
            return error.LogFileFull;
        }

        const should_create_index = self.bytes_since_last_index >= self.config.index_config.bytes_per_index or self.next_relative_offset == 0;

        if (should_create_index) {
            if (LogIndex.OnDiskIndex.isFull(self.config.index_config, self.index_file_size)) {
                return error.IndexFileFull;
            }

            _ = try LogIndex.OnDiskIndex.add(
                self.config.index_config,
                self.index_file,
                &self.index_file_size,
                self.next_relative_offset,
                @intCast(self.log_file_size),
            );

            self.bytes_since_last_index = 0;
        }

        const record_pos = self.log_file_size;
        try self.log_file.seekTo(record_pos);
        try OnDiskLog.OnDiskLog.serializeWrite(self.config.log_config, record, self.log_file);
        self.log_file_size += record_size;
        self.bytes_since_last_index += record_size;

        self.next_relative_offset += 1;
    }

    pub fn read(self: *Segment, offset: u64, allocator: std.mem.Allocator) !OnDiskLog.Record {
        if (offset < self.base_offset) {
            return error.OffsetOutOfBounds;
        }
        const relative_offset: u32 = @intCast(offset - self.base_offset);

        // Check if offset is beyond what we've written
        if (relative_offset >= self.next_relative_offset) {
            return error.OffsetOutOfBounds;
        }

        const index_entry = try LogIndex.OnDiskIndex.lookup(
            self.config.index_config,
            self.index_file,
            self.index_file_size,
            relative_offset,
        );

        // Seek to the indexed position
        try self.log_file.seekTo(index_entry.pos);

        // Scan forward from the indexed position to find the exact record
        var current_relative_offset = index_entry.relative_offset;
        while (current_relative_offset < relative_offset) : (current_relative_offset += 1) {
            // Skip this record by deserializing and immediately freeing
            const skip_record = try OnDiskLog.OnDiskLog.deserialize(self.config.log_config, self.log_file, allocator);
            allocator.free(skip_record.value);
            if (skip_record.key) |k| allocator.free(k);
        }

        // Now read the target record
        return try OnDiskLog.OnDiskLog.deserialize(self.config.log_config, self.log_file, allocator);
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
        10 * 1024 * 1024, // 10MB index
        512, // 512B max key
        2048, // 2KB max value
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
    defer segment.delete() catch {};

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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    // Create segment first
    var segment1 = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    segment1.close();

    // Open existing segment
    var segment2 = try Segment.open(config, abs_path, test_offset, std.testing.allocator);
    defer segment2.delete() catch {};

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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    const record = OnDiskLog.Record{ .key = "test-key", .value = "test-value" };
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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    // Append multiple records
    const record1 = OnDiskLog.Record{ .key = "key1", .value = "value1" };
    const record2 = OnDiskLog.Record{ .key = "key2", .value = "value2" };
    const record3 = OnDiskLog.Record{ .key = null, .value = "value3" };

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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var saved_log_size: u64 = 0;
    var saved_index_size: u64 = 0;

    // Create, write, and close
    {
        var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
        defer segment.close();

        const record = OnDiskLog.Record{ .key = "persist", .value = "test-data" };
        try segment.append(record);

        saved_log_size = segment.log_file_size;
        saved_index_size = segment.index_file_size;
    }

    // Reopen and verify
    {
        var segment = try Segment.open(config, abs_path, test_offset, std.testing.allocator);
        defer segment.delete() catch {};

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

    const test_record = OnDiskLog.Record{ .key = "k1", .value = "v1" };
    const record_size = OnDiskLog.OnDiskLog.serializedSize(OnDiskLog.OnDiskLogConfig{}, test_record);

    // Create config where only 1 record fits
    const config = SegmentConfig{
        .log_config = OnDiskLog.OnDiskLogConfig{
            .log_file_max_size_bytes = record_size, // Only one record fits
            .key_max_size_bytes = 256,
            .value_max_size_bytes = 65536,
        },
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    // First append should succeed
    const record1 = OnDiskLog.Record{ .key = "k1", .value = "v1" };
    try segment.append(record1);

    // Second append should fail due to log file full
    const record2 = OnDiskLog.Record{ .key = "k2", .value = "v2" };
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

    // Calculate record size to ensure we trigger index creation
    const test_record = OnDiskLog.Record{ .key = "k1", .value = "v1" };
    const record_size = OnDiskLog.OnDiskLog.serializedSize(OnDiskLog.OnDiskLogConfig{}, test_record);

    // Create config with very small index file size and bytes_per_index
    // Set bytes_per_index smaller than record_size to force index creation on each append
    const config = SegmentConfig{
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{
            .index_file_max_size_bytes = LogIndex.INDEX_SIZE_BYTES * 2, // Only 2 entries
            .bytes_per_index = record_size - 1, // Force index creation on every record
        },
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    // First append creates index at offset 0
    const record1 = OnDiskLog.Record{ .key = "k1", .value = "v1" };
    try segment.append(record1);

    // Second append should create another index (bytes_since_last_index >= bytes_per_index)
    const record2 = OnDiskLog.Record{ .key = "k2", .value = "v2" };
    try segment.append(record2);

    // Third append should fail due to index file full
    const record3 = OnDiskLog.Record{ .key = "k3", .value = "v3" };
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

    // Calculate record size to ensure we trigger index creation
    const test_record = OnDiskLog.Record{ .key = "k1", .value = "v1" };
    const record_size = OnDiskLog.OnDiskLog.serializedSize(OnDiskLog.OnDiskLogConfig{}, test_record);

    const config = SegmentConfig{
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{
            .index_file_max_size_bytes = LogIndex.INDEX_SIZE_BYTES * 2,
            .bytes_per_index = record_size - 1, // Force index creation on every record
        },
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    // Initially not full
    try std.testing.expect(!segment.isFull());

    // Add first record - creates first index
    const record1 = OnDiskLog.Record{ .key = "k1", .value = "v1" };
    try segment.append(record1);
    try std.testing.expect(!segment.isFull());

    // Add second record - creates second index, now full
    const record2 = OnDiskLog.Record{ .key = "k2", .value = "v2" };
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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    const record = OnDiskLog.Record{ .key = "key", .value = "value" };
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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    try std.testing.expectEqual(@as(u64, 0), segment.getSize());

    const record = OnDiskLog.Record{ .key = "key", .value = "value" };
    try segment.append(record);

    const expected_size = OnDiskLog.OnDiskLog.serializedSize(config.log_config, record);
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
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{},
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);

    const log_path = try std.testing.allocator.dupe(u8, segment.log_file_name);
    defer std.testing.allocator.free(log_path);
    const index_path = try std.testing.allocator.dupe(u8, segment.index_file_name);
    defer std.testing.allocator.free(index_path);

    try segment.delete();

    // Verify files are deleted
    const log_err = std.fs.openFileAbsolute(log_path, .{});
    try std.testing.expectError(error.FileNotFound, log_err);

    const index_err = std.fs.openFileAbsolute(index_path, .{});
    try std.testing.expectError(error.FileNotFound, index_err);
}

test "Segment: atomic creation - cleanup on index file failure" {
    const test_dir = "test_segment_atomic_create";
    const test_offset: u64 = 12000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();

    // Create the expected index file path before creating segment
    const expected_index_path = try Segment.getAllocIndexFilePath(std.testing.allocator, abs_path, test_offset);
    defer std.testing.allocator.free(expected_index_path);

    // Create a directory with the index file's name to force creation failure
    try std.fs.makeDirAbsolute(expected_index_path);
    defer std.fs.deleteDirAbsolute(expected_index_path) catch {};

    // Try to create segment - should fail due to index file conflict
    const result = Segment.create(config, abs_path, test_offset, std.testing.allocator);
    try std.testing.expectError(error.IsDir, result);

    // Verify log file was cleaned up (shouldn't exist)
    const log_path = try Segment.getAllocLogFilePath(std.testing.allocator, abs_path, test_offset);
    defer std.testing.allocator.free(log_path);

    const log_check = std.fs.openFileAbsolute(log_path, .{});
    try std.testing.expectError(error.FileNotFound, log_check);
}

test "Segment: atomic open - cleanup on index file failure" {
    const test_dir = "test_segment_atomic_open";
    const test_offset: u64 = 13000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();

    // Create only the log file, but not the index file
    const log_path = try Segment.getAllocLogFilePath(std.testing.allocator, abs_path, test_offset);
    defer std.testing.allocator.free(log_path);

    {
        const log_file = try OnDiskLog.OnDiskLog.create(log_path);
        log_file.close();
    }

    // Try to open segment - should fail because index file doesn't exist
    const result = Segment.open(config, abs_path, test_offset, std.testing.allocator);
    try std.testing.expectError(error.FileNotFound, result);

    // This test verifies that we don't leak the log file handle
    // The log file should still exist on disk (we don't delete it on open failure)
    // but the file handle should be closed
    const log_check = std.fs.openFileAbsolute(log_path, .{});
    if (log_check) |f| {
        f.close();
        // If we can open it, the previous handle was properly closed
    } else |_| {
        return error.TestFailed;
    }
}

test "Segment: sparse index creation based on bytes_per_index" {
    const test_dir = "test_segment_sparse_index";
    const test_offset: u64 = 14000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    // Create a test record and measure its exact size
    const test_record = OnDiskLog.Record{ .key = "key", .value = "value" };
    const record_size = OnDiskLog.OnDiskLog.serializedSize(OnDiskLog.OnDiskLogConfig{}, test_record);

    // Set bytes_per_index to exactly 4 times the record size for predictable behavior
    // With this configuration:
    // - Record 0 (offset 0): Always creates index (first record), bytes_since_last_index = record_size after
    // - Records 1-3: No index created, bytes_since_last_index = 2×, 3×, 4× record_size after each
    // - Record 4 (offset 4): Creates index (4×record_size >= 4×record_size), bytes_since_last_index = record_size after
    // - Records 5-7: No index created, bytes_since_last_index = 2×, 3×, 4× record_size after each
    // - Record 8 (offset 8): Creates index (4×record_size >= 4×record_size), bytes_since_last_index = record_size after
    const config = SegmentConfig{
        .log_config = OnDiskLog.OnDiskLogConfig{},
        .index_config = LogIndex.OnDiskIndexConfig{
            .bytes_per_index = record_size * 4,
        },
    };

    var segment = try Segment.create(config, abs_path, test_offset, std.testing.allocator);
    defer segment.delete() catch {};

    // We'll append exactly 9 records and expect exactly 3 indices at offsets 0, 4, and 8
    const num_records = 9;
    const expected_final_index_count = 3;

    var i: u32 = 0;
    while (i < num_records) : (i += 1) {
        const prev_index_file_size = segment.index_file_size;
        const prev_bytes_since_last_index = segment.bytes_since_last_index;
        const prev_log_file_size = segment.log_file_size;
        const prev_next_relative_offset = segment.next_relative_offset;

        // Append the record
        try segment.append(test_record);

        // Determine if an index should have been created BEFORE writing this record
        const should_have_created_index = (i == 0) or (prev_bytes_since_last_index >= config.index_config.bytes_per_index);

        // Verify index file size and bytes_since_last_index based on whether index was created
        if (should_have_created_index) {
            // Index was created before writing the record, then record was written
            try std.testing.expectEqual(prev_index_file_size + LogIndex.INDEX_SIZE_BYTES, segment.index_file_size);
            try std.testing.expectEqual(record_size, segment.bytes_since_last_index);
        } else {
            // No index was created, only the record was written
            try std.testing.expectEqual(prev_index_file_size, segment.index_file_size);
            try std.testing.expectEqual(prev_bytes_since_last_index + record_size, segment.bytes_since_last_index);
        }

        // Log file size should always increase by exactly record_size
        try std.testing.expectEqual(prev_log_file_size + record_size, segment.log_file_size);

        // next_relative_offset should always increment by 1
        try std.testing.expectEqual(prev_next_relative_offset + 1, segment.next_relative_offset);
    }

    // Final state verification
    try std.testing.expectEqual(num_records * record_size, segment.log_file_size);
    try std.testing.expectEqual(expected_final_index_count * LogIndex.INDEX_SIZE_BYTES, segment.index_file_size);
    try std.testing.expectEqual(@as(u32, num_records), segment.next_relative_offset);

    // Verify we created sparse indices (not one per record)
    try std.testing.expect(expected_final_index_count < num_records);
}
