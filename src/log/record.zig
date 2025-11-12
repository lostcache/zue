const std = @import("std");

/// Record
/// | Field        | Data Type    | Size (bytes) |
/// | :----------- | :----------- | :----------- |
/// | CRC32        | `u32`        | 4            |
/// | Timestamp    | `i64`        | 8            |
/// | Key Length   | `i32`        | 4            |
/// | Key          | `[]const u8` | configurable |
/// | Value Length | `i32`        | 4            |
/// | Value        | `[]const u8` | configurable |

const CRC = @sizeOf(u32);
const TIMESTAMP = @sizeOf(i64);
const KEY_LEN_HOLDER = @sizeOf(i32);
const VALUE_LEN_HOLDER = @sizeOf(i32);

pub const RECORD_HEADER_SIZE = CRC + TIMESTAMP + KEY_LEN_HOLDER + VALUE_LEN_HOLDER;

pub const OnDiskLogConfig = struct {
    log_file_max_size_bytes: u64 = 1024 * 1024 * 1024, // 1 GB
    key_max_size_bytes: u64 = 1024, // 1 KB
    key_min_size_bytes: u64 = 0,
    value_max_size_bytes: u64 = 1024 * 64, // 64 KB
    value_min_size_bytes: u64 = 1,
};

pub const Record = struct {
    key: ?[]const u8,
    value: []const u8,
};

pub const OnDiskLog = struct {
    pub fn create(file_path: []const u8) !std.fs.File {
        return try std.fs.createFileAbsolute(file_path, .{ .truncate = true, .read = true });
    }

    pub fn open(file_path: []const u8) !std.fs.File {
        return try std.fs.openFileAbsolute(file_path, .{ .mode = .read_write });
    }

    pub fn serializedSize(config: OnDiskLogConfig, record: Record) usize {
        var size: usize = 0;
        size += RECORD_HEADER_SIZE;
        if (record.key) |k| {
            std.debug.assert(k.len >= config.key_min_size_bytes);
            std.debug.assert(k.len <= config.key_max_size_bytes);

            size += k.len;
        }

        std.debug.assert(record.value.len >= config.value_min_size_bytes);
        std.debug.assert(record.value.len <= config.value_max_size_bytes);

        size += record.value.len;
        return size;
    }

    fn computeCRC(config: OnDiskLogConfig, record: Record, timestamp: i64, key_len: i32, value_len: i32) u32 {
        var crc_hasher = std.hash.Crc32.init();

        crc_hasher.update(std.mem.toBytes(timestamp)[0..]);
        crc_hasher.update(std.mem.toBytes(key_len)[0..]);

        if (record.key) |k| {
            std.debug.assert(k.len >= config.key_min_size_bytes);
            std.debug.assert(k.len <= config.key_max_size_bytes);

            crc_hasher.update(k);
        }

        crc_hasher.update(std.mem.toBytes(value_len)[0..]);

        std.debug.assert(record.value.len >= config.value_min_size_bytes);
        std.debug.assert(record.value.len <= config.value_max_size_bytes);

        crc_hasher.update(record.value);

        return crc_hasher.final();
    }

    fn writeSerialized(config: OnDiskLogConfig, writer: anytype, crc: u32, timestamp: i64, key_len: i32, value_len: i32, record: Record) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, crc, .little);
        try writer.writeAll(&buf);

        var buf8: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf8, timestamp, .little);
        try writer.writeAll(&buf8);

        std.mem.writeInt(i32, &buf, key_len, .little);
        try writer.writeAll(&buf);

        if (record.key) |k| {
            std.debug.assert(k.len >= config.key_min_size_bytes);
            std.debug.assert(k.len <= config.key_max_size_bytes);

            try writer.writeAll(k);
        }

        std.mem.writeInt(i32, &buf, value_len, .little);
        try writer.writeAll(&buf);

        std.debug.assert(record.value.len >= config.value_min_size_bytes);
        std.debug.assert(record.value.len <= config.value_max_size_bytes);

        try writer.writeAll(record.value);
    }

    pub fn serialize(config: OnDiskLogConfig, record: Record, writer: anytype) !void {
        const timestamp: i64 = std.time.milliTimestamp();
        const key_len: i32 = if (record.key) |k| @intCast(k.len) else 0;
        std.debug.assert(key_len >= config.key_min_size_bytes);
        std.debug.assert(key_len <= config.key_max_size_bytes);

        const value_len: i32 = @intCast(record.value.len);
        std.debug.assert(value_len >= config.value_min_size_bytes);
        std.debug.assert(value_len <= config.value_max_size_bytes);

        const crc = computeCRC(config, record, timestamp, key_len, value_len);

        try writeSerialized(config, writer, crc, timestamp, key_len, value_len, record);
    }

    fn verifyCRC(config: OnDiskLogConfig, crc: u32, timestamp: i64, record: Record) error{CRCMismatch}!void {
        const key_len: i32 = if (record.key) |k| @intCast(k.len) else 0;
        std.debug.assert(key_len >= config.key_min_size_bytes);
        std.debug.assert(key_len <= config.key_max_size_bytes);

        const value_len: i32 = @intCast(record.value.len);
        std.debug.assert(value_len >= config.value_min_size_bytes);
        std.debug.assert(value_len <= config.value_max_size_bytes);

        const expected_crc = computeCRC(config, record, timestamp, key_len, value_len);

        if (crc != expected_crc) {
            return error.CRCMismatch;
        }
    }

    pub fn deserialize(config: OnDiskLogConfig, reader: anytype, allocator: std.mem.Allocator) !Record {
        var buf: [4]u8 = undefined;
        _ = try reader.readAll(&buf);
        const crc = std.mem.readInt(u32, &buf, .little);

        var buf8: [8]u8 = undefined;
        _ = try reader.readAll(&buf8);
        const timestamp = std.mem.readInt(i64, &buf8, .little);

        _ = try reader.readAll(&buf);
        const key_len: i32 = std.mem.readInt(i32, &buf, .little);
        std.debug.assert(key_len >= config.key_min_size_bytes);
        std.debug.assert(key_len <= config.key_max_size_bytes);

        var key_buf: ?[]u8 = null;
        if (key_len > 0) {
            key_buf = try allocator.alloc(u8, @intCast(key_len));
            if (key_buf) |kb| {
                _ = try reader.readAll(kb);
            }
        }

        _ = try reader.readAll(&buf);
        const value_len: i32 = std.mem.readInt(i32, &buf, .little);

        // Validate value_len instead of asserting - return error if invalid
        if (value_len < config.value_min_size_bytes or value_len > config.value_max_size_bytes) {
            // Clean up key if allocated
            if (key_buf) |kb| allocator.free(kb);
            return error.InvalidRecordSize;
        }

        const value_buf = try allocator.alloc(u8, @intCast(value_len));
        _ = try reader.readAll(value_buf);

        const record = Record{ .key = key_buf, .value = value_buf };

        try verifyCRC(config, crc, timestamp, record);

        return record;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "OnDiskLog.serializedSize" {
    const config = OnDiskLogConfig{};
    const record = Record{ .key = "lol", .value = "plis" };
    // CRC(4) + Timestamp(8) + KeyLen(4) + Key(3) + ValueLen(4) + Value(4) = 27
    const expected_size: usize = 27;
    try std.testing.expectEqual(expected_size, OnDiskLog.serializedSize(config, record));

    const record_no_key = Record{ .key = null, .value = "plis" };
    // CRC(4) + Timestamp(8) + KeyLen(4) + ValueLen(4) + Value(4) = 24
    const expected_size_no_key: usize = 24;
    try std.testing.expectEqual(expected_size_no_key, OnDiskLog.serializedSize(config, record_no_key));
}

test "OnDiskLog.serialize" {
    const config = OnDiskLogConfig{};
    const t_alloc = std.testing.allocator;
    const record = Record{ .key = "key", .value = "value" };
    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(t_alloc);

    const writer = buffer.writer(t_alloc);
    try OnDiskLog.serialize(config, record, writer);

    // Expected size: CRC(4) + Timestamp(8) + KeyLen(4) + Key(3) + ValueLen(4) + Value(5) = 28
    try std.testing.expectEqual(@as(usize, 28), buffer.items.len);

    var stream = std.io.fixedBufferStream(buffer.items);
    var reader = stream.reader();

    const crc = try reader.readInt(u32, .little);
    const timestamp = try reader.readInt(i64, .little);
    const key_len = try reader.readInt(i32, .little);
    var key_buf: [3]u8 = undefined;
    try reader.readNoEof(key_buf[0..]);
    const value_len = try reader.readInt(i32, .little);
    var value_buf: [5]u8 = undefined;
    try reader.readNoEof(value_buf[0..]);

    // Verify content
    try std.testing.expectEqual(@as(i32, 3), key_len);
    try std.testing.expectEqualStrings("key", &key_buf);
    try std.testing.expectEqual(@as(i32, 5), value_len);
    try std.testing.expectEqualStrings("value", &value_buf);

    // Verify CRC
    var crc_hasher = std.hash.Crc32.init();
    crc_hasher.update(std.mem.toBytes(timestamp)[0..]);
    crc_hasher.update(std.mem.toBytes(key_len)[0..]);
    crc_hasher.update(key_buf[0..]);
    crc_hasher.update(std.mem.toBytes(value_len)[0..]);
    crc_hasher.update(value_buf[0..]);
    const expected_crc = crc_hasher.final();

    try std.testing.expectEqual(expected_crc, crc);
}

test "OnDiskLog.serialize and OnDiskLog.deserialize" {
    const config = OnDiskLogConfig{};
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(allocator);

    {
        // Test with key
        const record_with_key = Record{ .key = "my-key", .value = "my-value" };
        try OnDiskLog.serialize(config, record_with_key, buffer.writer(allocator));

        var stream = std.io.fixedBufferStream(buffer.items);
        const reader = stream.reader();
        const deserialized_with_key = try OnDiskLog.deserialize(config, reader, allocator);

        try std.testing.expect(std.mem.eql(u8, deserialized_with_key.key.?, record_with_key.key.?));
        try std.testing.expect(std.mem.eql(u8, deserialized_with_key.value, record_with_key.value));
    }

    // reset buffer without free, will overwrite data for next test
    buffer.items.len = 0;

    {
        // Test without key
        const record_no_key = Record{ .key = null, .value = "value-only" };
        try OnDiskLog.serialize(config, record_no_key, buffer.writer(allocator));

        var stream_no_key = std.io.fixedBufferStream(buffer.items);
        const reader_no_key = stream_no_key.reader();
        const deserialized_no_key = try OnDiskLog.deserialize(config, reader_no_key, allocator);

        try std.testing.expect(deserialized_no_key.key == null);
        try std.testing.expect(std.mem.eql(u8, deserialized_no_key.value, record_no_key.value));
    }
}

test "OnDiskLog.deserialize with CRC mismatch" {
    const config = OnDiskLogConfig{};
    // Use an ArenaAllocator to contain the memory leak within this test.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(allocator);

    const record = Record{ .key = "key", .value = "value" };
    try OnDiskLog.serialize(config, record, buffer.writer(allocator));

    // Tamper with the buffer. Flip a bit in the value.
    buffer.items[25] ^= 0x01;

    var stream = std.io.fixedBufferStream(buffer.items);
    const reader = stream.reader();
    const err = OnDiskLog.deserialize(config, reader, allocator);

    try std.testing.expectError(error.CRCMismatch, err);
}

test "OnDiskLog.create creates new log file" {
    const test_dir = "test_ondisklog_create";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const file_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/test.log", .{abs_path});
    defer std.testing.allocator.free(file_path);

    // Create file
    const file = try OnDiskLog.create(file_path);
    defer file.close();

    // Verify file exists and is empty
    const stat = try file.stat();
    try std.testing.expectEqual(@as(u64, 0), stat.size);

    // Verify we can write to it
    try file.writeAll("test");
    const stat2 = try file.stat();
    try std.testing.expectEqual(@as(u64, 4), stat2.size);
}

test "OnDiskLog.open opens existing log file" {
    const test_dir = "test_ondisklog_open";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const file_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/test.log", .{abs_path});
    defer std.testing.allocator.free(file_path);

    // Create file and write some data
    {
        const file = try OnDiskLog.create(file_path);
        defer file.close();
        try file.writeAll("existing data");
    }

    // Open existing file
    const file = try OnDiskLog.open(file_path);
    defer file.close();

    // Verify file contains existing data
    const stat = try file.stat();
    try std.testing.expectEqual(@as(u64, 13), stat.size);

    // Verify we can read from it
    var buf: [13]u8 = undefined;
    try file.seekTo(0);
    _ = try file.readAll(&buf);
    try std.testing.expectEqualStrings("existing data", &buf);
}

test "OnDiskLog.open returns error for non-existent file" {
    const test_dir = "test_ondisklog_open_error";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const file_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/nonexistent.log", .{abs_path});
    defer std.testing.allocator.free(file_path);

    // Try to open non-existent file
    try std.testing.expectError(error.FileNotFound, OnDiskLog.open(file_path));
}
