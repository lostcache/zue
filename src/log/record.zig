const std = @import("std");

/// Record
/// | Field        | Data Type    | Size (bytes) |
/// | :----------- | :----------- | :----------- |
/// | CRC32        | `u32`        | 4            |
/// | Timestamp    | `i64`        | 8            |
/// | Key Length   | `i32`        | 4            |
/// | Key          | `[]const u8` | >=0, <=256   |
/// | Value Length | `i32`        | 4            |
/// | Value        | `[]const u8` | >=1, <=65536  |
const KEY_LEN_MAX = 256;
const KEY_LEN_MIN = 0;
const VALUE_LEN_MAX = 65536;
const VALUE_LEN_MIN = 1;

const CRC = @sizeOf(u32);
const TIMESTAMP = @sizeOf(i64);
const KEY_LEN_HOLDER = @sizeOf(i32);
const VALUE_LEN_HOLDER = @sizeOf(i32);

const RECORD_HEADER_SIZE = CRC + TIMESTAMP + KEY_LEN_HOLDER + VALUE_LEN_HOLDER;

pub const Record = struct {
    key: ?[]const u8,
    value: []const u8,
};

pub const OnDisk = struct {
    records_file: std.fs.File,
    records_file_name: []const u8,

    fn getAllocFilePath(gpa_alloc: std.mem.Allocator, base_path: []const u8, offset: u64) ![]u8 {
        return std.fmt.allocPrint(gpa_alloc, "{s}/{d:0>20}", .{ base_path, offset });
    }

    pub fn create(base_path: []const u8, gpa_alloc: std.mem.Allocator, offset: u64) !OnDisk {
        const file_path = try OnDisk.getAllocFilePath(gpa_alloc, base_path, offset);
        errdefer gpa_alloc.free(file_path);

        const file = try std.fs.cwd().createFile(file_path, .{ .truncate = true, .read = true });

        return OnDisk{
            .records_file = file,
            .records_file_name = file_path,
        };
    }

    pub fn open(base_path: []const u8, gpa_alloc: std.mem.Allocator, offset: u64) !OnDisk {
        const file_path = try OnDisk.getAllocFilePath(gpa_alloc, base_path, offset);
        errdefer gpa_alloc.free(file_path);

        const file = try std.fs.cwd().openFile(file_path, .{ .mode = .read_write });

        return OnDisk{
            .records_file = file,
            .records_file_name = file_path,
        };
    }

    pub fn close(self: *OnDisk) void {
        self.records_file.close();
    }

    pub fn delete(self: *OnDisk, gpa_alloc: std.mem.Allocator) !void {
        self.records_file.close();
        try std.fs.cwd().deleteFile(self.records_file_name);
        gpa_alloc.free(self.records_file_name);
    }

    pub fn serializedSize(record: Record) usize {
        var size: usize = 0;
        size += RECORD_HEADER_SIZE;
        if (record.key) |k| {
            std.debug.assert(k.len >= KEY_LEN_MIN);
            std.debug.assert(k.len <= KEY_LEN_MAX);

            size += k.len;
        }

        std.debug.assert(record.value.len >= VALUE_LEN_MIN);
        std.debug.assert(record.value.len <= VALUE_LEN_MAX);

        size += record.value.len;
        return size;
    }

    fn computeCRC(record: Record, timestamp: i64, key_len: i32, value_len: i32) u32 {
        var crc_hasher = std.hash.Crc32.init();

        crc_hasher.update(std.mem.toBytes(timestamp)[0..]);
        crc_hasher.update(std.mem.toBytes(key_len)[0..]);

        if (record.key) |k| {
            std.debug.assert(k.len >= KEY_LEN_MIN);
            std.debug.assert(k.len <= KEY_LEN_MAX);

            crc_hasher.update(k);
        }

        crc_hasher.update(std.mem.toBytes(value_len)[0..]);

        std.debug.assert(record.value.len >= VALUE_LEN_MIN);
        std.debug.assert(record.value.len <= VALUE_LEN_MAX);

        crc_hasher.update(record.value);

        return crc_hasher.final();
    }

    fn writeSerialized(writer: anytype, crc: u32, timestamp: i64, key_len: i32, value_len: i32, record: Record) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, crc, .little);
        try writer.writeAll(&buf);

        var buf8: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf8, timestamp, .little);
        try writer.writeAll(&buf8);

        std.mem.writeInt(i32, &buf, key_len, .little);
        try writer.writeAll(&buf);

        if (record.key) |k| {
            std.debug.assert(k.len >= KEY_LEN_MIN);
            std.debug.assert(k.len <= KEY_LEN_MAX);

            try writer.writeAll(k);
        }

        std.mem.writeInt(i32, &buf, value_len, .little);
        try writer.writeAll(&buf);

        std.debug.assert(record.value.len >= VALUE_LEN_MIN);
        std.debug.assert(record.value.len <= VALUE_LEN_MAX);

        try writer.writeAll(record.value);
    }

    pub fn serialize(record: Record, writer: anytype) !void {
        const timestamp: i64 = std.time.milliTimestamp();
        const key_len: i32 = if (record.key) |k| @intCast(k.len) else 0;
        std.debug.assert(key_len >= KEY_LEN_MIN);
        std.debug.assert(key_len <= KEY_LEN_MAX);

        const value_len: i32 = @intCast(record.value.len);
        std.debug.assert(value_len >= VALUE_LEN_MIN);
        std.debug.assert(value_len <= VALUE_LEN_MAX);

        const crc = computeCRC(record, timestamp, key_len, value_len);

        try writeSerialized(writer, crc, timestamp, key_len, value_len, record);
    }

    fn verifyCRC(crc: u32, timestamp: i64, record: Record) error{CRCMismatch}!void {
        const key_len: i32 = if (record.key) |k| @intCast(k.len) else 0;
        std.debug.assert(key_len >= KEY_LEN_MIN);
        std.debug.assert(key_len <= KEY_LEN_MAX);

        const value_len: i32 = @intCast(record.value.len);
        std.debug.assert(value_len >= VALUE_LEN_MIN);
        std.debug.assert(value_len <= VALUE_LEN_MAX);

        const expected_crc = computeCRC(record, timestamp, key_len, value_len);

        if (crc != expected_crc) {
            return error.CRCMismatch;
        }
    }

    pub fn deserialize(reader: anytype, allocator: std.mem.Allocator) !Record {
        var buf: [4]u8 = undefined;
        _ = try reader.readAll(&buf);
        const crc = std.mem.readInt(u32, &buf, .little);

        var buf8: [8]u8 = undefined;
        _ = try reader.readAll(&buf8);
        const timestamp = std.mem.readInt(i64, &buf8, .little);

        _ = try reader.readAll(&buf);
        const key_len: i32 = std.mem.readInt(i32, &buf, .little);
        std.debug.assert(key_len >= KEY_LEN_MIN);
        std.debug.assert(key_len <= KEY_LEN_MAX);

        var key_buf: ?[]u8 = null;
        if (key_len > 0) {
            key_buf = try allocator.alloc(u8, @intCast(key_len));
            if (key_buf) |kb| {
                _ = try reader.readAll(kb);
            }
        }

        _ = try reader.readAll(&buf);
        const value_len: i32 = std.mem.readInt(i32, &buf, .little);
        std.debug.assert(value_len >= VALUE_LEN_MIN);
        std.debug.assert(value_len <= VALUE_LEN_MAX);

        const value_buf = try allocator.alloc(u8, @intCast(value_len));
        _ = try reader.readAll(value_buf);

        const record = Record{ .key = key_buf, .value = value_buf };

        try verifyCRC(crc, timestamp, record);

        return record;
    }
};

test "OnDisk.serializedSize" {
    const record = Record{ .key = "lol", .value = "plis" };
    // CRC(4) + Timestamp(8) + KeyLen(4) + Key(3) + ValueLen(4) + Value(4) = 27
    const expected_size: usize = 27;
    try std.testing.expectEqual(expected_size, OnDisk.serializedSize(record));

    const record_no_key = Record{ .key = null, .value = "plis" };
    // CRC(4) + Timestamp(8) + KeyLen(4) + ValueLen(4) + Value(4) = 24
    const expected_size_no_key: usize = 24;
    try std.testing.expectEqual(expected_size_no_key, OnDisk.serializedSize(record_no_key));
}

test "OnDisk.serialize" {
    const t_alloc = std.testing.allocator;
    const record = Record{ .key = "key", .value = "value" };
    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(t_alloc);

    const writer = buffer.writer(t_alloc);
    try OnDisk.serialize(record, writer);

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

test "OnDisk.serialize and OnDisk.deserialize" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(allocator);

    {
        // Test with key
        const record_with_key = Record{ .key = "my-key", .value = "my-value" };
        try OnDisk.serialize(record_with_key, buffer.writer(allocator));

        var stream = std.io.fixedBufferStream(buffer.items);
        const reader = stream.reader();
        const deserialized_with_key = try OnDisk.deserialize(reader, allocator);

        try std.testing.expect(std.mem.eql(u8, deserialized_with_key.key.?, record_with_key.key.?));
        try std.testing.expect(std.mem.eql(u8, deserialized_with_key.value, record_with_key.value));
    }

    // reset buffer without free, will overwrite data for next test
    buffer.items.len = 0;

    {
        // Test without key
        const record_no_key = Record{ .key = null, .value = "value-only" };
        try OnDisk.serialize(record_no_key, buffer.writer(allocator));

        var stream_no_key = std.io.fixedBufferStream(buffer.items);
        const reader_no_key = stream_no_key.reader();
        const deserialized_no_key = try OnDisk.deserialize(reader_no_key, allocator);
        // defer allocator.free(deserialized_no_key.value);

        try std.testing.expect(deserialized_no_key.key == null);
        try std.testing.expect(std.mem.eql(u8, deserialized_no_key.value, record_no_key.value));
    }
}

test "OnDisk.deserialize with CRC mismatch" {
    // Use an ArenaAllocator to contain the memory leak within this test.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(allocator);

    const record = Record{ .key = "key", .value = "value" };
    try OnDisk.serialize(record, buffer.writer(allocator));

    // Tamper with the buffer. Flip a bit in the value.
    buffer.items[25] ^= 0x01;

    var stream = std.io.fixedBufferStream(buffer.items);
    const reader = stream.reader();
    const err = OnDisk.deserialize(reader, allocator);

    try std.testing.expectError(error.CRCMismatch, err);
}

test "OnDisk.create and OnDisk.delete" {
    const test_dir = "test_records";
    const test_offset: u64 = 12345;

    // Create test directory
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Test create
    var ondisk = try OnDisk.create(test_dir, std.testing.allocator, test_offset);

    // Verify file was created with correct name (20 chars, zero-padded)
    const expected_name = "test_records/00000000000000012345";
    try std.testing.expectEqualStrings(expected_name, ondisk.records_file_name);

    // Test delete
    try ondisk.delete(std.testing.allocator);

    // Verify file was deleted
    if (std.fs.cwd().openFile(expected_name, .{})) |file| {
        file.close();
        return error.TestExpectedFileToBeDeleted;
    } else |err| {
        if (err != error.FileNotFound) return err;
    }
}

test "OnDisk.open and OnDisk.close" {
    const test_dir = "test_records_open";
    const test_offset: u64 = 67890;

    // Create test directory
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create a file first
    var ondisk1 = try OnDisk.create(test_dir, std.testing.allocator, test_offset);
    defer std.testing.allocator.free(ondisk1.records_file_name);

    // Write some data
    const record = Record{ .key = "test_key", .value = "test_value" };
    try OnDisk.serialize(record, ondisk1.records_file);

    // Close the file
    ondisk1.close();

    // Now open it again
    var ondisk2 = try OnDisk.open(test_dir, std.testing.allocator, test_offset);
    defer std.testing.allocator.free(ondisk2.records_file_name);
    defer ondisk2.close();

    // Verify we can read the data
    try ondisk2.records_file.seekTo(0);
    const deserialized = try OnDisk.deserialize(ondisk2.records_file, std.testing.allocator);
    defer std.testing.allocator.free(deserialized.value);
    defer if (deserialized.key) |k| std.testing.allocator.free(k);

    try std.testing.expectEqualStrings("test_key", deserialized.key.?);
    try std.testing.expectEqualStrings("test_value", deserialized.value);
}
