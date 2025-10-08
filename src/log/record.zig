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

    fn compute_crc(record: Record, timestamp: i64, key_len: i32, value_len: i32) u32 {
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

    fn write_serialized(writer: anytype, crc: u32, timestamp: i64, key_len: i32, value_len: i32, record: Record) !void {
        try writer.writeInt(u32, crc, .little);
        try writer.writeInt(i64, timestamp, .little);
        try writer.writeInt(i32, key_len, .little);

        if (record.key) |k| {
            std.debug.assert(k.len >= KEY_LEN_MIN);
            std.debug.assert(k.len <= KEY_LEN_MAX);

            try writer.writeAll(k);
        }

        try writer.writeInt(i32, value_len, .little);

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

        const crc = compute_crc(record, timestamp, key_len, value_len);

        try write_serialized(writer, crc, timestamp, key_len, value_len, record);
    }

    fn verify_crc(crc: u32, timestamp: i64, record: Record) error{CRCMismatch}!void {
        const key_len: i32 = if (record.key) |k| @intCast(k.len) else 0;
        std.debug.assert(key_len >= KEY_LEN_MIN);
        std.debug.assert(key_len <= KEY_LEN_MAX);

        const value_len: i32 = @intCast(record.value.len);
        std.debug.assert(value_len >= VALUE_LEN_MIN);
        std.debug.assert(value_len <= VALUE_LEN_MAX);

        const expected_crc = compute_crc(record, timestamp, key_len, value_len);

        if (crc != expected_crc) {
            return error.CRCMismatch;
        }
    }

    pub fn deserialize(reader: anytype, allocator: std.mem.Allocator) error{ CRCMismatch, EndOfStream, OutOfMemory }!Record {
        const crc = try reader.readInt(u32, .little);
        const timestamp = try reader.readInt(i64, .little);
        const key_len: i32 = try reader.readInt(i32, .little);
        std.debug.assert(key_len >= KEY_LEN_MIN);
        std.debug.assert(key_len <= KEY_LEN_MAX);

        var key_buf: ?[]u8 = null;
        if (key_len > 0) {
            key_buf = try allocator.alloc(u8, @intCast(key_len));
            if (key_buf) |kb| try reader.readNoEof(kb);
        }

        const value_len: i32 = try reader.readInt(i32, .little);
        std.debug.assert(value_len >= VALUE_LEN_MIN);
        std.debug.assert(value_len <= VALUE_LEN_MAX);

        const value_buf = try allocator.alloc(u8, @intCast(value_len));
        try reader.readNoEof(value_buf);

        const record = Record{ .key = key_buf, .value = value_buf };

        try verify_crc(crc, timestamp, record);

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
        var reader = stream.reader();
        const deserialized_with_key = try OnDisk.deserialize(&reader, allocator);

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
        var reader_no_key = stream_no_key.reader();
        const deserialized_no_key = try OnDisk.deserialize(&reader_no_key, allocator);
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
    var reader = stream.reader();
    const err = OnDisk.deserialize(&reader, allocator);

    try std.testing.expectError(error.CRCMismatch, err);
}
