const std = @import("std");
const mmap = @import("mmap.zig");
const record = @import("record.zig");
const posix = std.posix;

const Record = record.Record;
const OnDiskLogConfig = record.OnDiskLogConfig;

/// Memory-mapped log file for efficient record reading
/// Uses mmap to avoid system calls for each read operation
pub const MmapLogReader = struct {
    mmap_file: mmap.MmapFile,
    config: OnDiskLogConfig,
    actual_size: u64,

    /// Open a log file with memory mapping for reading
    pub fn open(file_path: []const u8, config: OnDiskLogConfig) !MmapLogReader {
        const mmap_file = try mmap.MmapFile.openRead(file_path);

        // Try to read the .size file to get actual data size
        const allocator = std.heap.page_allocator;
        const size_path = std.fmt.allocPrint(allocator, "{s}.size", .{file_path}) catch {
            // If we can't allocate, just use file size
            return MmapLogReader{
                .mmap_file = mmap_file,
                .config = config,
                .actual_size = mmap_file.len(),
            };
        };
        defer allocator.free(size_path);

        const actual_size = blk: {
            const size_file = std.fs.openFileAbsolute(size_path, .{}) catch {
                // No size file, use full file size
                break :blk mmap_file.len();
            };
            defer size_file.close();

            var buf: [32]u8 = undefined;
            const bytes_read = size_file.readAll(&buf) catch break :blk mmap_file.len();

            const size_str = std.mem.trim(u8, buf[0..bytes_read], &std.ascii.whitespace);
            break :blk std.fmt.parseInt(u64, size_str, 10) catch mmap_file.len();
        };

        return MmapLogReader{
            .mmap_file = mmap_file,
            .config = config,
            .actual_size = actual_size,
        };
    }

    /// Close and unmap the log file
    pub fn close(self: *MmapLogReader) void {
        self.mmap_file.close();
    }

    /// Deserialize a record at a specific file position
    /// Reads directly from mapped memory - no syscalls!
    pub fn deserializeAt(self: *const MmapLogReader, pos: u64, allocator: std.mem.Allocator) !Record {
        const slice = self.mmap_file.asConstSlice();

        // Check against actual data size, not file size
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

    /// Calculate the size of a record at a specific position without allocating
    /// Useful for skipping records efficiently
    pub fn recordSizeAt(self: *const MmapLogReader, pos: u64) !usize {
        const slice = self.mmap_file.asConstSlice();

        // Check against actual data size first
        if (pos >= self.actual_size) {
            return error.EndOfStream;
        }

        if (pos >= slice.len) {
            return error.EndOfStream;
        }

        // Need at least CRC + Timestamp + KeyLen fields (16 bytes)
        const min_header = 4 + 8 + 4; // CRC + Timestamp + KeyLen
        if (pos + min_header > self.actual_size) {
            return error.IncompleteRecord;
        }

        if (pos + min_header > slice.len) {
            return error.IncompleteRecord;
        }

        // Read key length (at offset 12)
        const key_len = std.mem.readInt(i32, slice[pos + 12 ..][0..4], .little);

        if (key_len < 0) {
            return error.InvalidRecordSize;
        }

        // Value length is at offset: 16 (CRC+TS+KeyLen) + key_len
        const value_len_offset = 16 + @as(usize, @intCast(key_len));

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

        // Total size: 16 (up to KeyLen) + key_len + 4 (ValueLen) + value_len
        const total_size = 16 + @as(usize, @intCast(key_len)) + 4 + @as(usize, @intCast(value_len));
        return total_size;
    }

    /// Get the actual data size (may be less than file size due to pre-allocation)
    pub fn size(self: *const MmapLogReader) usize {
        return self.actual_size;
    }
};

/// Memory-mapped log file for efficient record writing
/// Writes go directly to mapped memory - OS handles flushing
pub const MmapLogWriter = struct {
    mmap_file: mmap.MmapFile,
    config: OnDiskLogConfig,
    current_pos: u64,
    file_path: []const u8,
    allocator: std.mem.Allocator,

    /// Create a new memory-mapped log file for writing
    pub fn create(file_path: []const u8, config: OnDiskLogConfig) !MmapLogWriter {
        // Duplicate the path so we can use it later for truncation
        const allocator = std.heap.page_allocator;
        const path_copy = try allocator.dupe(u8, file_path);

        // Pre-allocate space for initial records (1MB)
        const initial_size = 1024 * 1024;
        const mmap_file = mmap.MmapFile.create(file_path, initial_size) catch |err| {
            allocator.free(path_copy);
            return err;
        };

        return MmapLogWriter{
            .mmap_file = mmap_file,
            .config = config,
            .current_pos = 0,
            .file_path = path_copy,
            .allocator = allocator,
        };
    }

    /// Open an existing log file for appending
    pub fn open(file_path: []const u8, config: OnDiskLogConfig, current_size: u64) !MmapLogWriter {
        const allocator = std.heap.page_allocator;
        const path_copy = try allocator.dupe(u8, file_path);

        // Open with minimum size equal to current size
        const min_size = if (current_size > 0) current_size else 1024 * 1024;
        const mmap_file = mmap.MmapFile.openWrite(file_path, min_size) catch |err| {
            allocator.free(path_copy);
            return err;
        };

        return MmapLogWriter{
            .mmap_file = mmap_file,
            .config = config,
            .current_pos = current_size,
            .file_path = path_copy,
            .allocator = allocator,
        };
    }

    /// Close and unmap the log file
    /// Note: On some platforms (macOS), files cannot be truncated while mapped.
    /// This means the file may be larger than the actual data written.
    /// Use .size metadata file or track size separately if needed.
    pub fn close(self: *MmapLogWriter) void {
        defer self.allocator.free(self.file_path);

        // Sync all changes to disk
        self.mmap_file.sync() catch {};

        // Write a marker file with the actual size
        // This allows readers to know where the real data ends
        const size_path = std.fmt.allocPrint(self.allocator, "{s}.size", .{self.file_path}) catch {
            self.mmap_file.close();
            return;
        };
        defer self.allocator.free(size_path);

        const size_file = std.fs.createFileAbsolute(size_path, .{ .truncate = true }) catch {
            self.mmap_file.close();
            return;
        };
        defer size_file.close();

        var buf: [32]u8 = undefined;
        const size_str = std.fmt.bufPrint(&buf, "{d}\n", .{self.current_pos}) catch {
            self.mmap_file.close();
            return;
        };
        size_file.writeAll(size_str) catch {};

        // Close (unmap and close file)
        self.mmap_file.close();
    }

    /// Serialize and append a record to the log
    /// Writes directly to mapped memory - very fast!
    pub fn append(self: *MmapLogWriter, rec: Record) !usize {
        const rec_size = record.OnDiskLog.serializedSize(self.config, rec);

        // Check if we need to extend the mapping
        if (self.current_pos + rec_size > self.mmap_file.len()) {
            // Extend by 1MB or the record size, whichever is larger
            const extension = @max(1024 * 1024, rec_size);
            const new_size = self.mmap_file.len() + extension;

            if (new_size > self.config.log_file_max_size_bytes) {
                return error.LogFileFull;
            }

            try self.mmap_file.extend(new_size);
        }

        // Write directly to mapped memory
        const slice = self.mmap_file.asSlice();
        var stream = std.io.fixedBufferStream(slice[self.current_pos..]);
        const writer = stream.writer();

        try record.OnDiskLog.serialize(self.config, rec, writer);

        const written_pos = self.current_pos;
        self.current_pos += rec_size;

        return written_pos;
    }

    /// Get current position (size of written data)
    pub fn getCurrentPos(self: *const MmapLogWriter) u64 {
        return self.current_pos;
    }

    /// Sync changes to disk
    pub fn sync(self: *MmapLogWriter) !void {
        try self.mmap_file.sync();
    }

    /// Async sync changes to disk
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

    const config = OnDiskLogConfig{};
    var writer = try MmapLogWriter.create(test_path, config);
    defer writer.close();

    const rec = Record{ .key = "test-key", .value = "test-value" };
    const pos = try writer.append(rec);

    try std.testing.expectEqual(@as(usize, 0), pos);
    try std.testing.expect(writer.getCurrentPos() > 0);

    try writer.sync();
}

test "MmapLogWriter: multiple appends" {
    const test_path = "/tmp/test_mmap_log_multi_write.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = OnDiskLogConfig{};
    var writer = try MmapLogWriter.create(test_path, config);
    defer writer.close();

    const rec1 = Record{ .key = "key1", .value = "value1" };
    const rec2 = Record{ .key = "key2", .value = "value2" };
    const rec3 = Record{ .key = null, .value = "value3" };

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

    const config = OnDiskLogConfig{};

    // Write some records
    var positions: [3]usize = undefined;
    {
        var writer = try MmapLogWriter.create(test_path, config);
        defer writer.close();

        const rec1 = Record{ .key = "key1", .value = "value1" };
        const rec2 = Record{ .key = "key2", .value = "value2" };
        const rec3 = Record{ .key = null, .value = "value3" };

        positions[0] = try writer.append(rec1);
        positions[1] = try writer.append(rec2);
        positions[2] = try writer.append(rec3);

        try writer.sync();
    }

    // Read them back
    {
        var reader = try MmapLogReader.open(test_path, config);
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
    defer std.fs.deleteFileAbsolute("/tmp/test_mmap_log_size.log.size") catch {};

    const config = OnDiskLogConfig{};

    var pos: usize = 0;
    {
        var writer = try MmapLogWriter.create(test_path, config);
        defer writer.close();

        const rec = Record{ .key = "key", .value = "value" };
        pos = try writer.append(rec);
        try writer.sync();
    }

    {
        var reader = try MmapLogReader.open(test_path, config);
        defer reader.close();

        const size = try reader.recordSizeAt(pos);
        const expected_size = record.OnDiskLog.serializedSize(config, Record{ .key = "key", .value = "value" });
        try std.testing.expectEqual(expected_size, size);
    }
}

test "MmapLogWriter: auto-extend on large write" {
    const test_path = "/tmp/test_mmap_log_extend.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = OnDiskLogConfig{
        .value_max_size_bytes = 2 * 1024 * 1024, // Allow 2MB values
    };
    var writer = try MmapLogWriter.create(test_path, config);
    defer writer.close();

    // Create a large value that will require extending
    var large_value: [2 * 1024 * 1024]u8 = undefined;
    @memset(&large_value, 'X');

    const rec = Record{ .key = "large", .value = &large_value };
    _ = try writer.append(rec);

    try std.testing.expect(writer.getCurrentPos() > 2 * 1024 * 1024);
    try writer.sync();
}

test "MmapLogWriter and Reader: persistence" {
    const test_path = "/tmp/test_mmap_log_persist.log";
    defer std.fs.deleteFileAbsolute(test_path) catch {};
    defer std.fs.deleteFileAbsolute("/tmp/test_mmap_log_persist.log.size") catch {};

    const config = OnDiskLogConfig{};

    // Write
    {
        var writer = try MmapLogWriter.create(test_path, config);
        defer writer.close();

        var i: u32 = 0;
        while (i < 10) : (i += 1) {
            const key = try std.fmt.allocPrint(std.testing.allocator, "key-{d}", .{i});
            defer std.testing.allocator.free(key);
            const value = try std.fmt.allocPrint(std.testing.allocator, "value-{d}", .{i});
            defer std.testing.allocator.free(value);

            const rec = Record{ .key = key, .value = value };
            _ = try writer.append(rec);
        }

        try writer.sync();
    }

    // Read and verify
    {
        var reader = try MmapLogReader.open(test_path, config);
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
