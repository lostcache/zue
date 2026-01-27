const std = @import("std");
const record = @import("record.zig");
const mmap_log = @import("mmap_log.zig");
const MmapLogReader = mmap_log.MmapLogReader;
const MmapLogWriter = mmap_log.MmapLogWriter;

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
