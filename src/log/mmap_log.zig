const std = @import("std");
const mmap = @import("mmap.zig");
const record = @import("record.zig");
const posix = std.posix;

pub const MmapLogReader = struct {
    mmap_file: mmap.MmapFile,
    config: record.OnDiskLogConfig,
    actual_size: u64,

    pub fn open(file_path: []const u8, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) !MmapLogReader {
        const mmap_file = try mmap.MmapFile.openRead(file_path);
        errdefer mmap_file.close();

        const actual_size = scanForValidEnd(mmap_file, config, allocator);

        return MmapLogReader{
            .mmap_file = mmap_file,
            .config = config,
            .actual_size = actual_size,
        };
    }

    fn scanForValidEnd(mmap_file: mmap.MmapFile, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) u64 {
        const slice = mmap_file.asConstSlice();
        var pos: u64 = 0;

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const temp_alloc = arena.allocator();

        while (pos < slice.len) {
            var stream = std.io.fixedBufferStream(slice[pos..]);
            const reader = stream.reader();

            const rec = record.OnDiskLog.deserialize(config, reader, temp_alloc) catch {
                break;
            };

            const rec_size = record.OnDiskLog.serializedSize(config, rec);
            pos += rec_size;

            _ = arena.reset(.retain_capacity);
        }

        return pos;
    }

    pub fn close(self: *MmapLogReader) void {
        self.mmap_file.close();
    }

    pub fn deserializeAt(self: *const MmapLogReader, pos: u64, allocator: std.mem.Allocator) !record.Record {
        const slice = self.mmap_file.asConstSlice();

        if (pos >= self.actual_size) {
            return error.EndOfStream;
        }

        if (pos >= slice.len) {
            return error.EndOfStream;
        }

        var stream = std.io.fixedBufferStream(slice[pos..]);
        const reader = stream.reader();

        return try record.OnDiskLog.deserialize(self.config, reader, allocator);
    }

    pub fn recordSizeAt(self: *const MmapLogReader, pos: u64) !usize {
        const slice = self.mmap_file.asConstSlice();

        if (pos >= self.actual_size) {
            return error.EndOfStream;
        }

        if (pos >= slice.len) {
            return error.EndOfStream;
        }

        // TODO: replace with constants
        const min_header = 4 + 8 + 4; // CRC + Timestamp + KeyLen
        if (pos + min_header > self.actual_size) {
            return error.IncompleteRecord;
        }

        if (pos + min_header > slice.len) {
            return error.IncompleteRecord;
        }

        // TODO: replace with constants
        const key_len = std.mem.readInt(i32, slice[pos + 12 ..][0..4], .little);

        if (key_len < 0) {
            return error.InvalidRecordSize;
        }

        // TODO: replace with constants
        // Value length is at offset: 16 (CRC+TS+KeyLen) + key_len
        const value_len_offset = 16 + @as(usize, @intCast(key_len));

        // TODO: replace with constants
        // Check we can read value length
        if (pos + value_len_offset + 4 > self.actual_size) {
            return error.IncompleteRecord;
        }

        if (pos + value_len_offset + 4 > slice.len) {
            return error.IncompleteRecord;
        }

        const value_len = std.mem.readInt(i32, slice[pos + value_len_offset ..][0..4], .little);

        if (value_len < 0) {
            return error.InvalidRecordSize;
        }

        // TODO: replace with constants
        // Total size: 16 (up to KeyLen) + key_len + 4 (ValueLen) + value_len
        const total_size = 16 + @as(usize, @intCast(key_len)) + 4 + @as(usize, @intCast(value_len));
        return total_size;
    }

    pub fn size(self: *const MmapLogReader) usize {
        return self.actual_size;
    }
};

pub const MmapLogWriter = struct {
    mmap_file: mmap.MmapFile,
    config: record.OnDiskLogConfig,
    current_pos: u64,
    file_path: []const u8,
    allocator: std.mem.Allocator,

    pub fn create(file_path: []const u8, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) !MmapLogWriter {
        const path_copy = try allocator.dupe(u8, file_path);
        errdefer allocator.free(path_copy);

        // TODO: replace with constants
        // Pre-allocate space for initial records (1MB)
        const initial_size = 1024 * 1024;
        const mmap_file = try mmap.MmapFile.create(file_path, initial_size);

        return MmapLogWriter{
            .mmap_file = mmap_file,
            .config = config,
            .current_pos = 0,
            .file_path = path_copy,
            .allocator = allocator,
        };
    }

    pub fn open(file_path: []const u8, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) !MmapLogWriter {
        const path_copy = try allocator.dupe(u8, file_path);
        errdefer allocator.free(path_copy);

        const file_size = blk: {
            const file = try std.fs.openFileAbsolute(file_path, .{});
            defer file.close();
            const stat = try file.stat();
            break :blk stat.size;
        };

        // TODO: replace with constants
        // Open with minimum size
        const min_size = @max(file_size, 1024 * 1024);
        const mmap_file = try mmap.MmapFile.openWrite(file_path, min_size);
        errdefer mmap_file.close();

        const actual_size = scanForValidEndWriter(mmap_file, config, allocator);

        return MmapLogWriter{
            .mmap_file = mmap_file,
            .config = config,
            .current_pos = actual_size,
            .file_path = path_copy,
            .allocator = allocator,
        };
    }

    fn scanForValidEndWriter(mmap_file: mmap.MmapFile, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) u64 {
        const slice = mmap_file.asConstSlice();
        var pos: u64 = 0;

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const temp_alloc = arena.allocator();

        while (pos < slice.len) {
            var stream = std.io.fixedBufferStream(slice[pos..]);
            const reader = stream.reader();

            const rec = record.OnDiskLog.deserialize(config, reader, temp_alloc) catch {
                break;
            };

            const rec_size = record.OnDiskLog.serializedSize(config, rec);
            pos += rec_size;

            _ = arena.reset(.retain_capacity);
        }

        return pos;
    }

    // TODO: handle errors
    pub fn close(self: *MmapLogWriter) void {
        defer self.allocator.free(self.file_path);

        self.mmap_file.sync() catch {};

        self.mmap_file.close();
    }

    pub fn append(self: *MmapLogWriter, rec: record.Record) !usize {
        const rec_size = record.OnDiskLog.serializedSize(self.config, rec);

        if (self.current_pos + rec_size > self.mmap_file.len()) {
            // TODO: replace with constants
            // Extend by 1MB or the record size, whichever is larger
            const extension = @max(1024 * 1024, rec_size);
            const new_size = self.mmap_file.len() + extension;

            if (new_size > self.config.log_file_max_size_bytes) {
                return error.LogFileFull;
            }

            try self.mmap_file.extend(new_size);
        }

        const slice = self.mmap_file.asSlice();
        var stream = std.io.fixedBufferStream(slice[self.current_pos..]);
        const writer = stream.writer();

        try record.OnDiskLog.serializeWrite(self.config, rec, writer);

        const written_pos = self.current_pos;
        self.current_pos += rec_size;

        return written_pos;
    }

    pub fn getCurrentPos(self: *const MmapLogWriter) u64 {
        return self.current_pos;
    }

    pub fn sync(self: *MmapLogWriter) !void {
        try self.mmap_file.sync();
    }

    pub fn syncAsync(self: *MmapLogWriter) !void {
        try self.mmap_file.syncAsync();
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "MmapLogWriter: create and append record" {
    const test_path = "/tmp/test_mmap_log_write.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = record.OnDiskLogConfig{};
    var writer = try MmapLogWriter.create(test_path, config, std.testing.allocator);
    defer writer.close();

    const rec = record.Record{ .key = "test-key", .value = "test-value" };
    const pos = try writer.append(rec);

    try std.testing.expectEqual(@as(usize, 0), pos);
    try std.testing.expect(writer.getCurrentPos() > 0);

    try writer.sync();
}

test "MmapLogWriter: multiple appends" {
    const test_path = "/tmp/test_mmap_log_multi_write.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = record.OnDiskLogConfig{};
    var writer = try MmapLogWriter.create(test_path, config, std.testing.allocator);
    defer writer.close();

    const rec1 = record.Record{ .key = "key1", .value = "value1" };
    const rec2 = record.Record{ .key = "key2", .value = "value2" };
    const rec3 = record.Record{ .key = null, .value = "value3" };

    const pos1 = try writer.append(rec1);
    const pos2 = try writer.append(rec2);
    const pos3 = try writer.append(rec3);

    try std.testing.expectEqual(@as(usize, 0), pos1);
    try std.testing.expect(pos2 > pos1);
    try std.testing.expect(pos3 > pos2);

    try writer.sync();
}

test "MmapLogReader: read records" {
    const test_path = "/tmp/test_mmap_log_read.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = record.OnDiskLogConfig{};

    // Write some records
    var positions: [3]usize = undefined;
    {
        var writer = try MmapLogWriter.create(test_path, config, std.testing.allocator);
        defer writer.close();

        const rec1 = record.Record{ .key = "key1", .value = "value1" };
        const rec2 = record.Record{ .key = "key2", .value = "value2" };
        const rec3 = record.Record{ .key = null, .value = "value3" };

        positions[0] = try writer.append(rec1);
        positions[1] = try writer.append(rec2);
        positions[2] = try writer.append(rec3);

        try writer.sync();
    }

    // Read them back
    {
        var reader = try MmapLogReader.open(test_path, config, std.testing.allocator);
        defer reader.close();

        // Read first record
        const read1 = try reader.deserializeAt(positions[0], std.testing.allocator);
        defer std.testing.allocator.free(read1.value);
        defer if (read1.key) |k| std.testing.allocator.free(k);

        try std.testing.expectEqualStrings("key1", read1.key.?);
        try std.testing.expectEqualStrings("value1", read1.value);

        // Read second record
        const read2 = try reader.deserializeAt(positions[1], std.testing.allocator);
        defer std.testing.allocator.free(read2.value);
        defer if (read2.key) |k| std.testing.allocator.free(k);

        try std.testing.expectEqualStrings("key2", read2.key.?);
        try std.testing.expectEqualStrings("value2", read2.value);

        // Read third record
        const read3 = try reader.deserializeAt(positions[2], std.testing.allocator);
        defer std.testing.allocator.free(read3.value);
        defer if (read3.key) |k| std.testing.allocator.free(k);

        try std.testing.expect(read3.key == null);
        try std.testing.expectEqualStrings("value3", read3.value);
    }
}

test "MmapLogReader: recordSizeAt" {
    const test_path = "/tmp/test_mmap_log_size.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = record.OnDiskLogConfig{};

    var pos: usize = 0;
    {
        var writer = try MmapLogWriter.create(test_path, config, std.testing.allocator);
        defer writer.close();

        const rec = record.Record{ .key = "key", .value = "value" };
        pos = try writer.append(rec);
        try writer.sync();
    }

    {
        var reader = try MmapLogReader.open(test_path, config, std.testing.allocator);
        defer reader.close();

        const size = try reader.recordSizeAt(pos);
        const expected_size = record.OnDiskLog.serializedSize(config, record.Record{ .key = "key", .value = "value" });
        try std.testing.expectEqual(expected_size, size);
    }
}

test "MmapLogWriter: auto-extend on large write" {
    const test_path = "/tmp/test_mmap_log_extend.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = record.OnDiskLogConfig{
        .value_max_size_bytes = 2 * 1024 * 1024, // Allow 2MB values
    };
    var writer = try MmapLogWriter.create(test_path, config, std.testing.allocator);
    defer writer.close();

    // Create a large value that will require extending
    var large_value: [2 * 1024 * 1024]u8 = undefined;
    @memset(&large_value, 'X');

    const rec = record.Record{ .key = "large", .value = &large_value };
    _ = try writer.append(rec);

    try std.testing.expect(writer.getCurrentPos() > 2 * 1024 * 1024);
    try writer.sync();
}

test "MmapLogWriter and Reader: persistence" {
    const test_path = "/tmp/test_mmap_log_persist.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = record.OnDiskLogConfig{};

    // Write
    {
        var writer = try MmapLogWriter.create(test_path, config, std.testing.allocator);
        defer writer.close();

        var i: u32 = 0;
        while (i < 10) : (i += 1) {
            const key = try std.fmt.allocPrint(std.testing.allocator, "key-{d}", .{i});
            defer std.testing.allocator.free(key);
            const value = try std.fmt.allocPrint(std.testing.allocator, "value-{d}", .{i});
            defer std.testing.allocator.free(value);

            const rec = record.Record{ .key = key, .value = value };
            _ = try writer.append(rec);
        }

        try writer.sync();
    }

    // Read and verify
    {
        var reader = try MmapLogReader.open(test_path, config, std.testing.allocator);
        defer reader.close();

        var pos: u64 = 0;
        var count: u32 = 0;
        while (pos < reader.size()) {
            const rec = reader.deserializeAt(pos, std.testing.allocator) catch break;
            defer std.testing.allocator.free(rec.value);
            defer if (rec.key) |k| std.testing.allocator.free(k);

            count += 1;
            pos += try reader.recordSizeAt(pos);
        }

        try std.testing.expectEqual(@as(u32, 10), count);
    }
}
