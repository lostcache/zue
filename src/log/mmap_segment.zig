const std = @import("std");
const record = @import("record.zig");
const mmap_index = @import("mmap_index.zig");
const mmap_log = @import("mmap_log.zig");
const segment = @import("segment.zig");

const Record = record.Record;
const SegmentConfig = segment.SegmentConfig;
const MmapIndex = mmap_index.MmapIndex;
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

        var log_writer = try MmapLogWriter.create(log_file_path, config.log_config, gpa_alloc);
        var log_writer_created = true;
        errdefer {
            if (log_writer_created) {
                log_writer.close();
                std.fs.deleteFileAbsolute(log_file_path) catch {};
            }
        }

        const index_file_path = try getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        const index = MmapIndex.create(index_file_path, config.index_config) catch |err| {
            log_writer.close();
            std.fs.deleteFileAbsolute(log_file_path) catch {};
            gpa_alloc.free(log_file_path);
            log_writer_created = false;
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

        // Open the log writer - it will scan to find the actual end of valid data
        var log_writer = try MmapLogWriter.open(log_file_path, config.log_config, gpa_alloc);
        var log_writer_opened = true;
        errdefer {
            if (log_writer_opened) {
                log_writer.close();
            }
        }

        const index_file_path = try getAllocIndexFilePath(gpa_alloc, abs_base_path, base_offset);
        errdefer gpa_alloc.free(index_file_path);

        var index = MmapIndex.open(index_file_path, config.index_config) catch |err| {
            log_writer.close();
            gpa_alloc.free(log_file_path);
            log_writer_opened = false;
            log_file_path_allocated = false;
            return err;
        };
        errdefer {
            index.close();
        }

        // Get the actual data size from the log writer (it scanned the file)
        const actual_data_size = log_writer.getCurrentPos();

        // Get log file size for tracking
        const log_file_size = blk: {
            const file = try std.fs.openFileAbsolute(log_file_path, .{});
            defer file.close();
            const stat = try file.stat();
            break :blk stat.size;
        };

        // Rebuild the index from the log (the log is the source of truth)
        // The index is just a cache for faster lookups
        var next_relative_offset: u32 = 0;
        var bytes_since_last_index: u64 = 0;

        var arena = std.heap.ArenaAllocator.init(gpa_alloc);
        defer arena.deinit();
        const temp_alloc = arena.allocator();

        var pos: u64 = 0;
        while (pos < actual_data_size) {
            // Check if we should create an index entry
            const should_create_index = bytes_since_last_index >= config.index_config.bytes_per_index or next_relative_offset == 0;

            if (should_create_index) {
                if (!index.isFull()) {
                    _ = try index.add(next_relative_offset, @intCast(pos));
                    bytes_since_last_index = 0;
                }
                // If index is full, just continue - index is just a cache
            }

            // Deserialize record to validate and get size
            const rec = deserializeRecordAt(&log_writer, pos, temp_alloc, config.log_config) catch break;
            _ = rec;

            const rec_size = try recordSizeAt(&log_writer, pos, actual_data_size, config.log_config);
            pos += rec_size;
            bytes_since_last_index += rec_size;
            next_relative_offset += 1;

            // Reset arena to avoid unbounded memory growth
            _ = arena.reset(.retain_capacity);
        }

        return MmapSegment{
            .gpa_alloc = gpa_alloc,
            .config = config,
            .base_offset = base_offset,
            .next_relative_offset = next_relative_offset,
            .log_writer = log_writer,
            .log_file_name = log_file_path,
            .log_file_size = log_file_size,
            .index = index,
            .index_file_name = index_file_path,
            .bytes_since_last_index = bytes_since_last_index,
        };
    }

    /// Close the segment
    /// Unmaps files and frees memory, but keeps the files on disk
    pub fn close(self: *MmapSegment) void {
        self.log_writer.close();
        self.index.close();
        self.gpa_alloc.free(self.log_file_name);
        self.gpa_alloc.free(self.index_file_name);
    }

    /// Close the segment and delete the files from disk
    /// This is a destructive operation that permanently removes data
    pub fn closeAndDelete(self: *MmapSegment) !void {
        self.log_writer.close();
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
    ///
    /// Performance characteristics with sparse index:
    /// - Index lookup: O(log N) binary search through index entries
    /// - Forward scan: O(M) where M = records between index entry and target
    ///   - For each skipped record, reads only size fields (not full data)
    ///   - With default bytes_per_index=4KB, typically M < 100 records
    ///   - For denser index (smaller bytes_per_index), M decreases but index grows
    ///
    /// Tradeoff: Sparse index saves disk space at cost of forward scan
    /// For random reads, consider decreasing bytes_per_index to reduce scan distance
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
        // Note: This only reads size fields, not full record data - relatively efficient
        while (current_relative_offset < relative_offset) : (current_relative_offset += 1) {
            // Skip this record by reading its size and advancing position
            const skip_size = try recordSizeAt(&self.log_writer, pos, self.log_file_size, self.config.log_config);
            pos += skip_size;
        }

        // Now read the target record
        return try deserializeRecordAt(&self.log_writer, pos, allocator, self.config.log_config);
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
// Helper Functions - Read from writer's mapping
// ============================================================================

/// Deserialize a record at a specific position from the writer's mapping
fn deserializeRecordAt(writer: *const MmapLogWriter, pos: u64, allocator: std.mem.Allocator, config: record.OnDiskLogConfig) !Record {
    const slice = writer.mmap_file.asConstSlice();

    const actual_size = writer.getCurrentPos();
    if (pos >= actual_size or pos >= slice.len) {
        return error.EndOfStream;
    }

    var stream = std.io.fixedBufferStream(slice[pos..]);
    const reader = stream.reader();

    return try record.OnDiskLog.deserialize(config, reader, allocator);
}

/// Calculate the size of a record at a specific position from the writer's mapping
fn recordSizeAt(writer: *const MmapLogWriter, pos: u64, _: u64, _: record.OnDiskLogConfig) !usize {
    const slice = writer.mmap_file.asConstSlice();
    const actual_size = writer.getCurrentPos();

    if (pos >= actual_size or pos >= slice.len) {
        return error.EndOfStream;
    }

    // Need at least CRC + Timestamp + KeyLen fields (16 bytes)
    const min_header = 4 + 8 + 4; // CRC + Timestamp + KeyLen
    if (pos + min_header > actual_size or pos + min_header > slice.len) {
        return error.IncompleteRecord;
    }

    // Read KeyLen
    const key_len = std.mem.readInt(i32, slice[pos + 12 ..][0..4], .little);
    if (key_len < 0) {
        return error.InvalidRecordSize;
    }

    // ValueLen is after the key
    const value_len_offset = 16 + @as(usize, @intCast(key_len));
    if (pos + value_len_offset + 4 > actual_size or pos + value_len_offset + 4 > slice.len) {
        return error.IncompleteRecord;
    }

    const value_len = std.mem.readInt(i32, slice[pos + value_len_offset ..][0..4], .little);
    if (value_len < 0) {
        return error.InvalidRecordSize;
    }

    // Total size
    const total_size = 16 + @as(usize, @intCast(key_len)) + 4 + @as(usize, @intCast(value_len));
    return total_size;
}

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
    defer seg.closeAndDelete() catch {};

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
    defer seg.closeAndDelete() catch {};

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
    defer seg.closeAndDelete() catch {};

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
        defer seg.closeAndDelete() catch {};

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
    defer seg.closeAndDelete() catch {};

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
