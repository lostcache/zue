const std = @import("std");

pub const Record = struct {
    key: ?[]const u8,
    value: []const u8,
};

pub const OnDisk = struct {
    /// | Field        | Data Type    | Size (bytes) |
    /// | :----------- | :----------- | :----------- |
    /// | CRC32        | `u32`        | 4            |
    /// | Timestamp    | `i64`        | 8            |
    /// | Key Length   | `i32`        | 4            |
    /// | Key          | `[]const u8` | Variable     |
    /// | Value Length | `i32`        | 4            |
    /// | Value        | `[]const u8` | Variable     |
    pub fn serializedSize(record: Record) usize {
        var size: usize = 0;
        size += @sizeOf(u32); // CRC32
        size += @sizeOf(i64); // Timestamp
        size += @sizeOf(i32); // Key Length
        if (record.key) |k| {
            size += k.len;
        }
        size += @sizeOf(i32); // Value Length
        size += record.value.len; // Value
        return size;
    }

    /// Serializes the record into its on-disk binary format and writes it to the provided writer.
    /// The format is: CRC32 (u32) | Timestamp (i64) | KeyLen (i32) | Key ([]u8) | ValueLen (i32) | Value ([]u8)
    /// All integers are little-endian.
    pub fn serialize(record: Record, writer: anytype) !void {
        const timestamp: i64 = std.time.milliTimestamp();
        const key_len: i32 = if (record.key) |k| @intCast(k.len) else -1;
        const value_len: i32 = @intCast(record.value.len);

        // To calculate the CRC, we first hash all the other fields in little-endian byte order.
        var crc_hasher = std.hash.Crc32.init();
        crc_hasher.update(std.mem.toBytes(timestamp)[0..]);
        crc_hasher.update(std.mem.toBytes(key_len)[0..]);
        if (record.key) |k| {
            crc_hasher.update(k);
        }
        crc_hasher.update(std.mem.toBytes(value_len)[0..]);
        crc_hasher.update(record.value);

        const crc = crc_hasher.final();

        // Now, write all fields to the actual writer, starting with the CRC.
        try writer.writeInt(u32, crc, .little);
        try writer.writeInt(i64, timestamp, .little);
        try writer.writeInt(i32, key_len, .little);
        if (record.key) |k| {
            try writer.writeAll(k);
        }
        try writer.writeInt(i32, value_len, .little);
        try writer.writeAll(record.value);
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
