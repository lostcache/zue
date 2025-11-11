const std = @import("std");
const record = @import("record.zig");
const mmap_index = @import("mmap_index.zig");
const mmap_log = @import("mmap_log.zig");
const segment = @import("segment.zig");

const Record = record.Record;
const SegmentConfig = segment.SegmentConfig;
const MmapIndex = mmap_index.MmapIndex;
const MmapLogReader = mmap_log.MmapLogReader;
const MmapLogWriter = mmap_log.MmapLogWriter;

/// Memory-mapped segment for high-performance IO
/// Uses mmap for both index and log files to minimize syscall overhead
/// Significantly faster than traditional segment implementation for:
/// - Index lookups (binary search through mapped memory)
/// - Log reads (direct memory access)
/// - Log writes (direct memory writes, OS handles flushing)
pub const MmapSegment = struct {
    gpa_alloc: std.mem.Allocator,

    config: SegmentConfig,
    base_offset: u64,
    next_relative_offset: u32,

    log_writer: MmapLogWriter,
    log_reader: MmapLogReader,
    log_file_name: []const u8,
    log_file_size: u64,

    index: MmapIndex,
    index_file_name: []const u8,

    bytes_since_last_index: u64,

    fn getAllocLogFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}.log", .{ base_path, offset });
    }

    fn getAllocIndexFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}.index", .{ base_path, offset });
    }

    /// Create a new memory-mapped segment
    pub fn create(config: SegmentConfig, abs_base_path: []const u8, base_offset: u64, gpa_alloc: std.mem.Allocator) !MmapSegment {
        const log_file_path = try getAllocLogFilePath(gpa_alloc, abs_base_path, base_offset);
        var log_file_path_allocated = true;
        errdefer {
            if (log_file_path_allocated) {
                gpa_alloc.free(log_file_path);
            }
        }

        var log_writer = try MmapLogWriter.create(log_file_path, config.log_config);
        var log_writer_created = true;
        errdefer {
            if (log_writer_created) {
                log_writer.close();
                std.fs.deleteFileAbsolute(log_file_path) catch {};
            }
        }

        var log_reader = try MmapLogReader.open(log_file_path, config.log_config);
        var log_reader_opened = true;
        errdefer {
            if (log_reader_opened) {
                log_reader.close();
            }
        }

        const index_file_path = try getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index = MmapIndex.create(index_file_path, config.index_config) catch |err| {
            log_writer.close();
            log_reader.close();
            std.fs.deleteFileAbsolute(log_file_path) catch {};
            gpa_alloc.free(log_file_path);
            log_writer_created = false;
            log_reader_opened = false;
            log_file_path_allocated = false;
            return err;
        };
        errdefer {
            var idx = index;
            idx.close();
            std.fs.deleteFileAbsolute(index_file_path) catch {};
        }

        return MmapSegment{
            .gpa_alloc = gpa_alloc,
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = 0,
            .log_writer = log_writer,
            .log_reader = log_reader,
            .log_file_name = log_file_path,
            .log_file_size = 0,
            .index = index,
            .index_file_name = index_file_path,
            .bytes_since_last_index = 0,
        };
    }

    /// Open an existing memory-mapped segment
    pub fn open(config: SegmentConfig, abs_base_path: []const u8, base_offset: u64, gpa_alloc: std.mem.Allocator) !MmapSegment {
        const log_file_path = try getAllocLogFilePath(gpa_alloc, abs_base_path, base_offset);
        var log_file_path_allocated = true;
        errdefer {
            if (log_file_path_allocated) {
                gpa_alloc.free(log_file_path);
            }
        }

        // Get log file size
        const log_stat = blk: {
            const file = try std.fs.openFileAbsolute(log_file_path, .{});
            defer file.close();
            break :blk try file.stat();
        };
        const log_file_size = log_stat.size;

        var log_writer = try MmapLogWriter.open(log_file_path, config.log_config, log_file_size);
        var log_writer_opened = true;
        errdefer {
            if (log_writer_opened) {
                log_writer.close();
            }
        }

        var log_reader = try MmapLogReader.open(log_file_path, config.log_config);
        var log_reader_opened = true;
        errdefer {
            if (log_reader_opened) {
                log_reader.close();
            }
        }

        const index_file_path = try getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index = MmapIndex.open(index_file_path, config.index_config) catch |err| {
            log_writer.close();
            log_reader.close();
            gpa_alloc.free(log_file_path);
            log_writer_opened = false;
            log_reader_opened = false;
            log_file_path_allocated = false;
            return err;
        };
        errdefer {
            var idx = index;
            idx.close();
        }

        // Calculate next_relative_offset and bytes_since_last_index
        var next_relative_offset: u32 = 0;
        var bytes_since_last_index: u64 = 0;

        const index_entry_count = index.getEntryCount();
        if (index_entry_count > 0) {
            // Get the last index entry
            const last_index = try index.lookup(std.math.maxInt(u32));
            const last_indexed_pos = last_index.pos;

            // Count records from the last indexed position to EOF
            var arena = std.heap.ArenaAllocator.init(gpa_alloc);
            defer arena.deinit();
            const temp_alloc = arena.allocator();

            var pos: u64 = last_indexed_pos;
            var record_count: u32 = 0;
            while (pos < log_file_size) {
                const rec = log_reader.deserializeAt(pos, temp_alloc) catch break;
                _ = rec;
                record_count += 1;
                pos += try log_reader.recordSizeAt(pos);
            }

            next_relative_offset = last_index.relative_offset + record_count;
            bytes_since_last_index = log_file_size - last_indexed_pos;
        } else {
            // No index entries - count all records from beginning
            var arena = std.heap.ArenaAllocator.init(gpa_alloc);
            defer arena.deinit();
            const temp_alloc = arena.allocator();

            var pos: u64 = 0;
            var record_count: u32 = 0;
            while (pos < log_file_size) {
                const rec = log_reader.deserializeAt(pos, temp_alloc) catch break;
                _ = rec;
                record_count += 1;
                pos += try log_reader.recordSizeAt(pos);
            }

            next_relative_offset = record_count;
            bytes_since_last_index = log_file_size;
        }

        return MmapSegment{
            .gpa_alloc = gpa_alloc,
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = next_relative_offset,
            .log_writer = log_writer,
            .log_reader = log_reader,
            .log_file_name = log_file_path,
            .log_file_size = log_file_size,
            .index = index,
            .index_file_name = index_file_path,
            .bytes_since_last_index = bytes_since_last_index,
        };
    }

    /// Close the segment
    pub fn close(self: *MmapSegment) void {
        self.log_writer.close();
        self.log_reader.close();
        self.index.close();
        self.gpa_alloc.free(self.log_file_name);
        self.gpa_alloc.free(self.index_file_name);
    }

    /// Delete the segment files
    pub fn delete(self: *MmapSegment) !void {
        self.log_writer.close();
        self.log_reader.close();
        self.index.close();
        try std.fs.deleteFileAbsolute(self.log_file_name);
        try std.fs.deleteFileAbsolute(self.index_file_name);
        self.gpa_alloc.free(self.log_file_name);
        self.gpa_alloc.free(self.index_file_name);
    }

    /// Append a record to the segment
    /// Much faster than traditional segment due to direct memory writes
    pub fn append(self: *MmapSegment, rec: Record) !void {
        const record_size = record.OnDiskLog.serializedSize(self.config.log_config, rec);
        if (self.log_file_size + record_size > self.config.log_config.log_file_max_size_bytes) {
            return error.LogFileFull;
        }

        const should_create_index = self.bytes_since_last_index >= self.config.index_config.bytes_per_index or self.next_relative_offset == 0;

        if (should_create_index) {
            if (self.index.isFull()) {
                return error.IndexFileFull;
            }

            _ = try self.index.add(
                self.next_relative_offset,
                @intCast(self.log_file_size),
            );

            self.bytes_since_last_index = 0;
        }

        _ = try self.log_writer.append(rec);
        self.log_file_size += record_size;
        self.bytes_since_last_index += record_size;

        self.next_relative_offset += 1;
    }

    /// Read a record from the segment
    /// Much faster than traditional segment due to direct memory reads
    pub fn read(self: *MmapSegment, offset: u64, allocator: std.mem.Allocator) !Record {
        if (offset < self.base_offset) {
            return error.OffsetOutOfBounds;
        }
        const relative_offset: u32 = @intCast(offset - self.base_offset);

        if (relative_offset >= self.next_relative_offset) {
            return error.OffsetOutOfBounds;
        }

        const index_entry = try self.index.lookup(relative_offset);

        // Start from the indexed position
        var current_relative_offset = index_entry.relative_offset;
        var pos: u64 = index_entry.pos;

        // Scan forward to find the exact record
        while (current_relative_offset < relative_offset) : (current_relative_offset += 1) {
            // Skip this record by just advancing position
            const skip_size = try self.log_reader.recordSizeAt(pos);
            pos += skip_size;
        }

        // Now read the target record
        return try self.log_reader.deserializeAt(pos, allocator);
    }

    /// Check if the segment is full
    pub fn isFull(self: *MmapSegment) bool {
        return self.index.isFull();
    }

    /// Get the size of the log file
    pub fn getSize(self: *MmapSegment) u64 {
        return self.log_file_size;
    }

    /// Sync all changes to disk
    pub fn sync(self: *MmapSegment) !void {
        try self.log_writer.sync();
        try self.index.sync();
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "MmapSegment: create new segment" {
    const test_dir = "test_mmap_segment_create";
    const test_offset: u64 = 1000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();

    var seg = try MmapSegment.create(config, abs_path, test_offset, std.testing.allocator);
    defer seg.delete() catch {};

    try std.testing.expectEqual(test_offset, seg.base_offset);
    try std.testing.expectEqual(@as(u64, 0), seg.log_file_size);
}

test "MmapSegment: append and read single record" {
    const test_dir = "test_mmap_segment_append";
    const test_offset: u64 = 2000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();
    var seg = try MmapSegment.create(config, abs_path, test_offset, std.testing.allocator);
    defer seg.delete() catch {};

    const rec = Record{ .key = "test-key", .value = "test-value" };
    try seg.append(rec);

    try std.testing.expect(seg.log_file_size > 0);

    // Read it back
    const read_rec = try seg.read(test_offset, std.testing.allocator);
    defer std.testing.allocator.free(read_rec.value);
    defer if (read_rec.key) |k| std.testing.allocator.free(k);

    try std.testing.expectEqualStrings("test-key", read_rec.key.?);
    try std.testing.expectEqualStrings("test-value", read_rec.value);
}

test "MmapSegment: append and read multiple records" {
    const test_dir = "test_mmap_segment_multiple";
    const test_offset: u64 = 3000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();
    var seg = try MmapSegment.create(config, abs_path, test_offset, std.testing.allocator);
    defer seg.delete() catch {};

    const rec1 = Record{ .key = "key1", .value = "value1" };
    const rec2 = Record{ .key = "key2", .value = "value2" };
    const rec3 = Record{ .key = null, .value = "value3" };

    try seg.append(rec1);
    try seg.append(rec2);
    try seg.append(rec3);

    // Read them back
    const read1 = try seg.read(test_offset, std.testing.allocator);
    defer std.testing.allocator.free(read1.value);
    defer if (read1.key) |k| std.testing.allocator.free(k);
    try std.testing.expectEqualStrings("key1", read1.key.?);

    const read2 = try seg.read(test_offset + 1, std.testing.allocator);
    defer std.testing.allocator.free(read2.value);
    defer if (read2.key) |k| std.testing.allocator.free(k);
    try std.testing.expectEqualStrings("value2", read2.value);

    const read3 = try seg.read(test_offset + 2, std.testing.allocator);
    defer std.testing.allocator.free(read3.value);
    defer if (read3.key) |k| std.testing.allocator.free(k);
    try std.testing.expectEqual(@as(?[]const u8, null), read3.key);
}

test "MmapSegment: persistence across close/open" {
    const test_dir = "test_mmap_segment_persistence";
    const test_offset: u64 = 4000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();

    // Create, write, and close
    {
        var seg = try MmapSegment.create(config, abs_path, test_offset, std.testing.allocator);
        defer seg.close();

        const rec1 = Record{ .key = "persist", .value = "test-data-1" };
        const rec2 = Record{ .key = "persist2", .value = "test-data-2" };

        try seg.append(rec1);
        try seg.append(rec2);

        try seg.sync();
    }

    // Reopen and verify
    {
        var seg = try MmapSegment.open(config, abs_path, test_offset, std.testing.allocator);
        defer seg.delete() catch {};

        try std.testing.expectEqual(@as(u32, 2), seg.next_relative_offset);

        const read_rec = try seg.read(test_offset + 1, std.testing.allocator);
        defer std.testing.allocator.free(read_rec.value);
        defer if (read_rec.key) |k| std.testing.allocator.free(k);

        try std.testing.expectEqualStrings("persist2", read_rec.key.?);
        try std.testing.expectEqualStrings("test-data-2", read_rec.value);
    }
}

test "MmapSegment: many records (performance test)" {
    const test_dir = "test_mmap_segment_perf";
    const test_offset: u64 = 5000;

    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const config = SegmentConfig.default();
    var seg = try MmapSegment.create(config, abs_path, test_offset, std.testing.allocator);
    defer seg.delete() catch {};

    // Write 1000 records
    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        const key = try std.fmt.allocPrint(std.testing.allocator, "key-{d}", .{i});
        defer std.testing.allocator.free(key);
        const value = try std.fmt.allocPrint(std.testing.allocator, "value-{d}", .{i});
        defer std.testing.allocator.free(value);

        const rec = Record{ .key = key, .value = value };
        try seg.append(rec);
    }

    try seg.sync();

    // Read some random records to verify
    {
        const rec = try seg.read(test_offset + 500, std.testing.allocator);
        defer std.testing.allocator.free(rec.value);
        defer if (rec.key) |k| std.testing.allocator.free(k);

        try std.testing.expectEqualStrings("key-500", rec.key.?);
        try std.testing.expectEqualStrings("value-500", rec.value);
    }
}
