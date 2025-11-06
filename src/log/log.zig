const std = @import("std");
const record = @import("record.zig");
const log_index = @import("log_index.zig");
const segment = @import("segment.zig");
const log_config = @import("log_config.zig");

const Record = record.Record;
const Segment = segment.Segment;
const SegmentConfig = segment.SegmentConfig;

pub const LogConfig = log_config.LogConfig;

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
        return seg.read(offset, allocator) catch |err| {
            // Map OffsetOutOfBounds from segment to OffsetNotFound at log level
            if (err == error.OffsetOutOfBounds) return error.OffsetNotFound;
            return err;
        };
    }

    /// Read a range of records starting from start_offset
    /// Returns a slice of records allocated in the provided allocator
    /// Caller must free each record's key/value AND the returned slice
    /// max_count limits the number of records returned
    pub fn readRange(
        self: *Log,
        start_offset: u64,
        max_count: u32,
        allocator: std.mem.Allocator,
    ) ![]Record {
        if (max_count == 0) {
            return try allocator.alloc(Record, 0);
        }

        // Validate start_offset is within bounds
        if (start_offset < self.getOldestOffset() or start_offset >= self.next_offset) {
            return error.OffsetOutOfBounds;
        }

        // Calculate how many records we can actually read
        const available_records = self.next_offset - start_offset;
        const records_to_read = @min(max_count, available_records);

        var records = try std.ArrayList(Record).initCapacity(allocator, records_to_read);
        errdefer {
            for (records.items) |rec| {
                if (rec.key) |k| allocator.free(k);
                allocator.free(rec.value);
            }
            records.deinit(allocator);
        }

        var current_offset = start_offset;
        var count: u32 = 0;

        while (count < records_to_read) : (count += 1) {
            const rec = self.read(current_offset, allocator) catch |err| {
                // Clean up already read records
                for (records.items) |r| {
                    if (r.key) |k| allocator.free(k);
                    allocator.free(r.value);
                }
                records.deinit(allocator);
                return err;
            };

            try records.append(allocator, rec);
            current_offset += 1;
        }

        return records.toOwnedSlice(allocator);
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

    /// Truncate the log to a specific offset (exclusive)
    /// All entries at offset >= new_next_offset will be removed
    /// This is used to rollback uncommitted writes
    /// WARNING: This is a destructive operation and should only be used for
    /// rolling back uncommitted entries (e.g., when quorum fails)
    pub fn truncateToOffset(self: *Log, new_next_offset: u64) !void {
        if (new_next_offset > self.next_offset) {
            return error.InvalidTruncateOffset;
        }

        if (new_next_offset == self.next_offset) {
            return; // Nothing to truncate
        }

        // For Phase 2 simplification: We only support truncating within the active segment
        const active_segment = &self.segments.items[self.active_segment_index];

        if (new_next_offset < active_segment.base_offset) {
            // Would need to truncate across segments - not supported in Phase 2
            return error.TruncateAcrossSegmentsNotSupported;
        }

        // Calculate the new relative offset for the active segment
        const new_relative_offset = @as(u32, @intCast(new_next_offset - active_segment.base_offset));

        // Update segment's next_relative_offset
        // NOTE: This leaves stale data in the log file, but we don't read past next_relative_offset
        // A full implementation would truncate the actual file, but for Phase 2 this is acceptable
        // since we never read uncommitted data
        active_segment.next_relative_offset = new_relative_offset;

        // Update log's next_offset
        self.next_offset = new_next_offset;
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

test "Log.readRange: read multiple records" {
    const test_dir = "test_log_read_range";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Append 10 records
    var i: u64 = 0;
    while (i < 10) : (i += 1) {
        const key = try std.fmt.allocPrint(std.testing.allocator, "key-{d}", .{i});
        defer std.testing.allocator.free(key);
        const value = try std.fmt.allocPrint(std.testing.allocator, "value-{d}", .{i});
        defer std.testing.allocator.free(value);
        _ = try log.append(Record{ .key = key, .value = value });
    }

    // Read range [2, 6) - should get 5 records
    const records = try log.readRange(2, 5, std.testing.allocator);
    defer {
        for (records) |rec| {
            if (rec.key) |k| std.testing.allocator.free(k);
            std.testing.allocator.free(rec.value);
        }
        std.testing.allocator.free(records);
    }

    try std.testing.expectEqual(@as(usize, 5), records.len);
    try std.testing.expectEqualStrings("key-2", records[0].key.?);
    try std.testing.expectEqualStrings("value-2", records[0].value);
    try std.testing.expectEqualStrings("key-6", records[4].key.?);
    try std.testing.expectEqualStrings("value-6", records[4].value);
}

test "Log.readRange: read all available records when max_count exceeds available" {
    const test_dir = "test_log_read_range_exceed";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Append 3 records
    _ = try log.append(Record{ .key = "k1", .value = "v1" });
    _ = try log.append(Record{ .key = "k2", .value = "v2" });
    _ = try log.append(Record{ .key = "k3", .value = "v3" });

    // Request 100 records starting from offset 1, should only get 2
    const records = try log.readRange(1, 100, std.testing.allocator);
    defer {
        for (records) |rec| {
            if (rec.key) |k| std.testing.allocator.free(k);
            std.testing.allocator.free(rec.value);
        }
        std.testing.allocator.free(records);
    }

    try std.testing.expectEqual(@as(usize, 2), records.len);
    try std.testing.expectEqualStrings("k2", records[0].key.?);
    try std.testing.expectEqualStrings("k3", records[1].key.?);
}

test "Log.readRange: read zero records" {
    const test_dir = "test_log_read_range_zero";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    _ = try log.append(Record{ .key = "k1", .value = "v1" });

    const records = try log.readRange(0, 0, std.testing.allocator);
    defer std.testing.allocator.free(records);

    try std.testing.expectEqual(@as(usize, 0), records.len);
}

test "Log.readRange: offset out of bounds returns error" {
    const test_dir = "test_log_read_range_oob";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    _ = try log.append(Record{ .key = "k1", .value = "v1" });

    // Try to read from offset 10 (out of bounds)
    try std.testing.expectError(error.OffsetOutOfBounds, log.readRange(10, 5, std.testing.allocator));
}

test "Log.readRange: read across multiple segments" {
    const test_dir = "test_log_read_range_multi_segment";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

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

    // Append 5 records (will span 3 segments: 2, 2, 1)
    var i: u64 = 0;
    while (i < 5) : (i += 1) {
        const key = try std.fmt.allocPrint(std.testing.allocator, "k{d}", .{i});
        defer std.testing.allocator.free(key);
        const value = try std.fmt.allocPrint(std.testing.allocator, "v{d}", .{i});
        defer std.testing.allocator.free(value);
        _ = try log.append(Record{ .key = key, .value = value });
    }

    // Read range that spans segments [1, 4) - should cross 2 segments
    const records = try log.readRange(1, 3, std.testing.allocator);
    defer {
        for (records) |rec| {
            if (rec.key) |k| std.testing.allocator.free(k);
            std.testing.allocator.free(rec.value);
        }
        std.testing.allocator.free(records);
    }

    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expectEqualStrings("k1", records[0].key.?);
    try std.testing.expectEqualStrings("k2", records[1].key.?);
    try std.testing.expectEqualStrings("k3", records[2].key.?);
}

test {
    std.testing.refAllDecls(log_config);
    std.testing.refAllDecls(record);
    std.testing.refAllDecls(log_index);
    std.testing.refAllDecls(segment);
}

test "Log: truncateToOffset removes uncommitted entries" {
    const test_dir = "test_log_truncate_to_offset";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    // Append 3 records
    const offset0 = try log.append(Record{ .key = "k0", .value = "v0" });
    const offset1 = try log.append(Record{ .key = "k1", .value = "v1" });
    const offset2 = try log.append(Record{ .key = "k2", .value = "v2" });

    try std.testing.expectEqual(@as(u64, 0), offset0);
    try std.testing.expectEqual(@as(u64, 1), offset1);
    try std.testing.expectEqual(@as(u64, 2), offset2);
    try std.testing.expectEqual(@as(u64, 3), log.getNextOffset());

    // Truncate to offset 1 (removes offsets 1 and 2)
    try log.truncateToOffset(1);

    // Verify next_offset is now 1
    try std.testing.expectEqual(@as(u64, 1), log.getNextOffset());

    // Verify we can still read offset 0
    const rec0 = try log.read(0, std.testing.allocator);
    defer {
        if (rec0.key) |k| std.testing.allocator.free(k);
        std.testing.allocator.free(rec0.value);
    }
    try std.testing.expectEqualStrings("v0", rec0.value);

    // Verify offsets 1 and 2 are no longer accessible
    try std.testing.expectError(error.OffsetNotFound, log.read(1, std.testing.allocator));
    try std.testing.expectError(error.OffsetNotFound, log.read(2, std.testing.allocator));

    // Verify we can append again at offset 1
    const new_offset1 = try log.append(Record{ .key = "k1_new", .value = "v1_new" });
    try std.testing.expectEqual(@as(u64, 1), new_offset1);
}

test "Log: truncateToOffset with invalid offset returns error" {
    const test_dir = "test_log_truncate_invalid";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer std.testing.allocator.free(log_path);

    const config = LogConfig.default();
    var log = try Log.create(config, log_path, std.testing.allocator);
    defer log.delete() catch {};

    _ = try log.append(Record{ .key = "k0", .value = "v0" });

    // Try to truncate to an offset beyond next_offset
    try std.testing.expectError(error.InvalidTruncateOffset, log.truncateToOffset(10));
}
