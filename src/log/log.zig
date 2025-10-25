const std = @import("std");
const record = @import("record.zig");
const log_index = @import("log_index.zig");
const segment = @import("segment.zig");

const Record = record.Record;
const Segment = segment.Segment;
const SegmentConfig = segment.SegmentConfig;

pub const LogConfig = struct {
    segment_config: SegmentConfig,
    initial_offset: u64 = 0,

    pub fn default() LogConfig {
        return LogConfig{
            .segment_config = SegmentConfig.default(),
            .initial_offset = 0,
        };
    }

    pub fn init(log_file_max_bytes: u64, index_file_max_bytes: u64) LogConfig {
        return LogConfig{
            .segment_config = SegmentConfig.init(log_file_max_bytes, index_file_max_bytes),
            .initial_offset = 0,
        };
    }

    pub fn withInitialOffset(segment_config: SegmentConfig, initial_offset: u64) LogConfig {
        return LogConfig{
            .segment_config = segment_config,
            .initial_offset = initial_offset,
        };
    }
};

pub const Log = struct {
    config: LogConfig,
    segments: std.ArrayList(Segment),
    active_segment_index: usize,
    dir_path: []const u8,
    next_offset: u64,
    allocator: std.mem.Allocator,

    pub fn create(config: LogConfig, dir_path: []const u8, allocator: std.mem.Allocator) !Log {
        std.fs.makeDirAbsolute(dir_path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        const stored_dir_path = try allocator.dupe(u8, dir_path);
        errdefer allocator.free(stored_dir_path);

        var segments: std.ArrayList(Segment) = .{};
        errdefer segments.deinit(allocator);

        var initial_segment = try Segment.create(
            config.segment_config,
            stored_dir_path,
            config.initial_offset,
            allocator,
        );
        errdefer initial_segment.delete() catch {};

        try segments.append(allocator, initial_segment);

        return Log{
            .config = config,
            .segments = segments,
            .active_segment_index = 0,
            .dir_path = stored_dir_path,
            .next_offset = config.initial_offset,
            .allocator = allocator,
        };
    }

    pub fn open(config: LogConfig, dir_path: []const u8, allocator: std.mem.Allocator) !Log {
        var dir = try std.fs.openDirAbsolute(dir_path, .{ .iterate = true });
        defer dir.close();

        const stored_dir_path = try allocator.dupe(u8, dir_path);
        errdefer allocator.free(stored_dir_path);

        var segments_list: std.ArrayList(Segment) = .{};
        errdefer {
            for (segments_list.items) |*seg| {
                seg.close();
            }
            segments_list.deinit(allocator);
        }

        var base_offsets: std.ArrayList(u64) = .{};
        defer base_offsets.deinit(allocator);

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".log")) {
                const name_without_ext = entry.name[0 .. entry.name.len - 4];
                const offset = std.fmt.parseInt(u64, name_without_ext, 10) catch continue;
                try base_offsets.append(allocator, offset);
            }
        }

        if (base_offsets.items.len == 0) {
            return error.NoSegmentsFound;
        }

        std.mem.sort(u64, base_offsets.items, {}, comptime std.sort.asc(u64));

        for (base_offsets.items) |offset| {
            const seg = try Segment.open(config.segment_config, stored_dir_path, offset, allocator);
            try segments_list.append(allocator, seg);
        }

        const last_segment = &segments_list.items[segments_list.items.len - 1];
        const next_offset = last_segment.base_offset + last_segment.next_relative_offset;

        return Log{
            .config = config,
            .segments = segments_list,
            .active_segment_index = segments_list.items.len - 1,
            .dir_path = stored_dir_path,
            .next_offset = next_offset,
            .allocator = allocator,
        };
    }

    pub fn close(self: *Log) void {
        for (self.segments.items) |*seg| {
            seg.close();
        }
        self.segments.deinit(self.allocator);
        self.allocator.free(self.dir_path);
    }

    pub fn delete(self: *Log) !void {
        for (self.segments.items) |*seg| {
            try seg.delete();
        }
        self.segments.deinit(self.allocator);

        try std.fs.deleteDirAbsolute(self.dir_path);
        self.allocator.free(self.dir_path);
    }

    pub fn append(self: *Log, rec: Record) !u64 {
        self.segments.items[self.active_segment_index].append(rec) catch |err| {
            if (err == error.LogFileFull or err == error.IndexFileFull) {
                try self.rotateSegment();
                try self.segments.items[self.active_segment_index].append(rec);
            } else {
                return err;
            }
        };

        const offset = self.next_offset;
        self.next_offset += 1;
        return offset;
    }

    pub fn read(self: *Log, offset: u64, allocator: std.mem.Allocator) !Record {
        const seg = self.findSegmentForOffset(offset) orelse return error.OffsetNotFound;
        return try seg.read(offset, allocator);
    }

    pub fn getNextOffset(self: *Log) u64 {
        return self.next_offset;
    }

    pub fn getOldestOffset(self: *Log) u64 {
        if (self.segments.items.len == 0) {
            return self.config.initial_offset;
        }
        return self.segments.items[0].base_offset;
    }

    pub fn getSegmentCount(self: *Log) usize {
        return self.segments.items.len;
    }

    pub fn getTotalSize(self: *Log) u64 {
        var total: u64 = 0;
        for (self.segments.items) |*seg| {
            total += seg.getSize();
        }
        return total;
    }

    pub fn truncate(self: *Log, min_offset: u64) !usize {
        var segments_removed: usize = 0;

        while (self.segments.items.len > 1) {
            const next_segment = &self.segments.items[1];

            if (next_segment.base_offset <= min_offset) {
                var seg = self.segments.orderedRemove(0);
                try seg.delete();
                segments_removed += 1;
            } else {
                break;
            }
        }

        return segments_removed;
    }


    fn rotateSegment(self: *Log) !void {
        const new_base_offset = self.next_offset;
        const new_segment = try Segment.create(
            self.config.segment_config,
            self.dir_path,
            new_base_offset,
            self.allocator,
        );

        try self.segments.append(self.allocator, new_segment);
        self.active_segment_index = self.segments.items.len - 1;
    }

    fn findSegmentForOffset(self: *Log, offset: u64) ?*Segment {
        var left: usize = 0;
        var right: usize = self.segments.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const seg = &self.segments.items[mid];

            if (offset < seg.base_offset) {
                right = mid;
            } else if (mid + 1 < self.segments.items.len and offset >= self.segments.items[mid + 1].base_offset) {
                left = mid + 1;
            } else {
                return seg;
            }
        }

        return null;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "LogConfig: default configuration" {
    const config = LogConfig.default();
    try std.testing.expectEqual(@as(u64, 0), config.initial_offset);
    try std.testing.expectEqual(@as(u64, 1024 * 1024 * 1024), config.segment_config.log_config.log_file_max_size_bytes);
}

test "LogConfig: init with custom sizes" {
    const config = LogConfig.init(512 * 1024 * 1024, 5 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 512 * 1024 * 1024), config.segment_config.log_config.log_file_max_size_bytes);
    try std.testing.expectEqual(@as(u64, 5 * 1024 * 1024), config.segment_config.index_config.index_file_max_size_bytes);
}

test "LogConfig: withInitialOffset" {
    const seg_config = SegmentConfig.default();
    const config = LogConfig.withInitialOffset(seg_config, 1000);
    try std.testing.expectEqual(@as(u64, 1000), config.initial_offset);
}

test "Log: create new log" {
    const test_dir = "test_log_create";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Verify initial state
    try std.testing.expectEqual(@as(usize, 1), log.segments.items.len);
    try std.testing.expectEqual(@as(usize, 0), log.active_segment_index);
    try std.testing.expectEqual(@as(u64, 0), log.next_offset);
}

test "Log: append single record" {
    const test_dir = "test_log_append_single";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    const rec = Record{ .key = "test-key", .value = "test-value" };
    const offset = try log.append(rec);

    try std.testing.expectEqual(@as(u64, 0), offset);
    try std.testing.expectEqual(@as(u64, 1), log.getNextOffset());
}

test "Log: append and read multiple records" {
    const test_dir = "test_log_append_read";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Append multiple records
    const rec1 = Record{ .key = "key1", .value = "value1" };
    const rec2 = Record{ .key = "key2", .value = "value2" };
    const rec3 = Record{ .key = null, .value = "value3" };

    const offset1 = try log.append(rec1);
    const offset2 = try log.append(rec2);
    const offset3 = try log.append(rec3);

    try std.testing.expectEqual(@as(u64, 0), offset1);
    try std.testing.expectEqual(@as(u64, 1), offset2);
    try std.testing.expectEqual(@as(u64, 2), offset3);

    // Read back records
    const read1 = try log.read(offset1, std.testing.allocator);
    defer std.testing.allocator.free(read1.value);
    defer if (read1.key) |k| std.testing.allocator.free(k);

    try std.testing.expectEqualStrings("key1", read1.key.?);
    try std.testing.expectEqualStrings("value1", read1.value);

    const read2 = try log.read(offset2, std.testing.allocator);
    defer std.testing.allocator.free(read2.value);
    defer if (read2.key) |k| std.testing.allocator.free(k);

    try std.testing.expectEqualStrings("key2", read2.key.?);
    try std.testing.expectEqualStrings("value2", read2.value);

    const read3 = try log.read(offset3, std.testing.allocator);
    defer std.testing.allocator.free(read3.value);
    defer if (read3.key) |k| std.testing.allocator.free(k);

    try std.testing.expect(read3.key == null);
    try std.testing.expectEqualStrings("value3", read3.value);
}

test "Log: persistence across close and open" {
    const test_dir = "test_log_persistence";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();

    // Create, write, and close
    {
        var log = try Log.create(config, log_path, std.testing.allocator);
        defer log.close();

        const rec1 = Record{ .key = "persist", .value = "test-data-1" };
        const rec2 = Record{ .key = "persist2", .value = "test-data-2" };

        _ = try log.append(rec1);
        _ = try log.append(rec2);
    }

    // Reopen and verify
    {
        var log = try Log.open(config, log_path, std.testing.allocator);
        defer log.delete() catch {};

        try std.testing.expectEqual(@as(u64, 2), log.getNextOffset());
        try std.testing.expectEqual(@as(usize, 1), log.getSegmentCount());

        const read_rec = try log.read(1, std.testing.allocator);
        defer std.testing.allocator.free(read_rec.value);
        defer if (read_rec.key) |k| std.testing.allocator.free(k);

        try std.testing.expectEqualStrings("persist2", read_rec.key.?);
        try std.testing.expectEqualStrings("test-data-2", read_rec.value);
    }
}

test "Log: segment rotation when full" {
    const test_dir = "test_log_rotation";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    // Create test record and calculate its size
    const test_rec = Record{ .key = "k", .value = "v" };
    const rec_size = record.OnDiskLog.serializedSize(record.OnDiskLogConfig{}, test_rec);

    // Configure log to hold only 2 records per segment
    const config = LogConfig{
        .segment_config = SegmentConfig{
            .log_config = record.OnDiskLogConfig{
                .log_file_max_size_bytes = rec_size * 2,
            },
            .index_config = log_index.OnDiskIndexConfig{},
        },
        .initial_offset = 0,
    };

    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Initially should have 1 segment
    try std.testing.expectEqual(@as(usize, 1), log.getSegmentCount());

    // Append 2 records - should fit in first segment
    _ = try log.append(test_rec);
    _ = try log.append(test_rec);
    try std.testing.expectEqual(@as(usize, 1), log.getSegmentCount());

    // Append 3rd record - should trigger rotation
    _ = try log.append(test_rec);
    try std.testing.expectEqual(@as(usize, 2), log.getSegmentCount());

    // Verify all records are readable
    const read1 = try log.read(0, std.testing.allocator);
    defer std.testing.allocator.free(read1.value);
    defer if (read1.key) |k| std.testing.allocator.free(k);

    const read2 = try log.read(1, std.testing.allocator);
    defer std.testing.allocator.free(read2.value);
    defer if (read2.key) |k| std.testing.allocator.free(k);

    const read3 = try log.read(2, std.testing.allocator);
    defer std.testing.allocator.free(read3.value);
    defer if (read3.key) |k| std.testing.allocator.free(k);
}

test "Log: read from multiple segments" {
    const test_dir = "test_log_multi_segment_read";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    // Create records with unique values
    const rec1 = Record{ .key = "k1", .value = "segment1-record1" };
    const rec2 = Record{ .key = "k2", .value = "segment1-record2" };
    const rec3 = Record{ .key = "k3", .value = "segment2-record1" };

    // Calculate size based on actual records to ensure 2 fit in first segment
    const rec1_size = record.OnDiskLog.serializedSize(record.OnDiskLogConfig{}, rec1);
    const rec2_size = record.OnDiskLog.serializedSize(record.OnDiskLogConfig{}, rec2);

    const config = LogConfig{
        .segment_config = SegmentConfig{
            .log_config = record.OnDiskLogConfig{
                .log_file_max_size_bytes = rec1_size + rec2_size,
            },
            .index_config = log_index.OnDiskIndexConfig{},
        },
        .initial_offset = 100,
    };

    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    _ = try log.append(rec1); // offset 100 - segment 1
    _ = try log.append(rec2); // offset 101 - segment 1
    _ = try log.append(rec3); // offset 102 - segment 2 (rotation)

    try std.testing.expectEqual(@as(usize, 2), log.getSegmentCount());

    // Read from first segment
    const read1 = try log.read(100, std.testing.allocator);
    defer std.testing.allocator.free(read1.value);
    defer if (read1.key) |k| std.testing.allocator.free(k);
    try std.testing.expectEqualStrings("segment1-record1", read1.value);

    // Read from second segment
    const read3 = try log.read(102, std.testing.allocator);
    defer std.testing.allocator.free(read3.value);
    defer if (read3.key) |k| std.testing.allocator.free(k);
    try std.testing.expectEqualStrings("segment2-record1", read3.value);
}

test "Log: truncate old segments" {
    const test_dir = "test_log_truncate";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const test_rec = Record{ .key = "k", .value = "v" };
    const rec_size = record.OnDiskLog.serializedSize(record.OnDiskLogConfig{}, test_rec);

    const config = LogConfig{
        .segment_config = SegmentConfig{
            .log_config = record.OnDiskLogConfig{
                .log_file_max_size_bytes = rec_size * 2,
            },
            .index_config = log_index.OnDiskIndexConfig{},
        },
        .initial_offset = 0,
    };

    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Create 3 segments (2 records each)
    var i: usize = 0;
    while (i < 6) : (i += 1) {
        _ = try log.append(test_rec);
    }

    try std.testing.expectEqual(@as(usize, 3), log.getSegmentCount());

    // Truncate segments before offset 4 (should remove first 2 segments)
    const removed = try log.truncate(4);
    try std.testing.expectEqual(@as(usize, 2), removed);
    try std.testing.expectEqual(@as(usize, 1), log.getSegmentCount());

    // Verify we can still read from remaining segment
    const read = try log.read(4, std.testing.allocator);
    defer std.testing.allocator.free(read.value);
    defer if (read.key) |k| std.testing.allocator.free(k);
    try std.testing.expectEqualStrings("v", read.value);

    // Verify old offsets are not readable
    try std.testing.expectError(error.OffsetNotFound, log.read(0, std.testing.allocator));
}

test "Log: getOldestOffset returns correct value" {
    const test_dir = "test_log_oldest_offset";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig{
        .segment_config = SegmentConfig.default(),
        .initial_offset = 1000,
    };

    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    try std.testing.expectEqual(@as(u64, 1000), log.getOldestOffset());
}

test "Log: getTotalSize returns sum of segment sizes" {
    const test_dir = "test_log_total_size";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    try std.testing.expectEqual(@as(u64, 0), log.getTotalSize());

    const rec = Record{ .key = "key", .value = "value" };
    _ = try log.append(rec);

    const expected_size = record.OnDiskLog.serializedSize(record.OnDiskLogConfig{}, rec);
    try std.testing.expectEqual(expected_size, log.getTotalSize());
}

test "Log: open with non-existent directory returns error" {
    const config = LogConfig.default();
    try std.testing.expectError(error.FileNotFound, Log.open(config, "/nonexistent/path", std.testing.allocator));
}

test "Log: open with empty directory returns error" {
    const test_dir = "test_log_open_empty";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = LogConfig.default();
    try std.testing.expectError(error.NoSegmentsFound, Log.open(config, abs_path, std.testing.allocator));
}

test {
    std.testing.refAllDecls(record);
    std.testing.refAllDecls(log_index);
    std.testing.refAllDecls(segment);
}
