const std = @import("std");

const INDEX_RELATIVE_OFFSET_HOLDER_BYTES = @sizeOf(u32);
const INDEX_POS_HOLDER_BYTES = @sizeOf(u32);
const INDEX_CRC_HOLDER_BYTES = @sizeOf(u32);
pub const INDEX_SIZE_BYTES = INDEX_RELATIVE_OFFSET_HOLDER_BYTES + INDEX_POS_HOLDER_BYTES + INDEX_CRC_HOLDER_BYTES;

pub const OnDiskIndexConfig = struct {
    index_file_max_size_bytes: u64 = 1024 * 1024 * 10, // 10 MB
    bytes_per_index: u64 = 1024 * 4, // 4 KB
};

pub const Index = struct {
    relative_offset: u32,
    pos: u32,
    crc32: u32,

    pub fn calculateCrc32(self: *const Index) u32 {
        var hasher = std.hash.Crc32.init();

        var buf: [8]u8 = undefined;
        std.mem.writeInt(u32, buf[0..4], self.relative_offset, .little);
        std.mem.writeInt(u32, buf[4..8], self.pos, .little);

        hasher.update(&buf);
        return hasher.final();
    }

    pub fn validate(self: *const Index) bool {
        return self.crc32 == self.calculateCrc32();
    }

    pub fn create(relative_offset: u32, pos: u32) Index {
        // TODO: Add bounds checking
        var index = Index{
            .relative_offset = relative_offset,
            .pos = pos,
            .crc32 = 0,
        };
        index.crc32 = index.calculateCrc32();
        return index;
    }
};

pub const OnDiskIndex = struct {
    pub fn create(file_path: []const u8) !std.fs.File {
        return try std.fs.createFileAbsolute(file_path, .{ .truncate = true, .read = true });
    }

    pub fn open(file_path: []const u8) !std.fs.File {
        return try std.fs.openFileAbsolute(file_path, .{ .mode = .read_write });
    }

    pub fn add(config: OnDiskIndexConfig, file: std.fs.File, current_size: *u64, relative_offset: u32, pos: u32) !Index {
        if (current_size.* + INDEX_SIZE_BYTES > config.index_file_max_size_bytes) {
            return error.IndexFileFull;
        }

        const index = Index.create(relative_offset, pos);

        const emptyBytePos = current_size.*;
        try file.seekTo(emptyBytePos);

        var buf: [INDEX_SIZE_BYTES]u8 = undefined;
        std.mem.writeInt(u32, buf[0..4], index.relative_offset, .little);
        std.mem.writeInt(u32, buf[4..8], index.pos, .little);
        std.mem.writeInt(u32, buf[8..12], index.crc32, .little);

        try file.writeAll(&buf);
        current_size.* += INDEX_SIZE_BYTES;

        return index;
    }

    pub fn lookup(config: OnDiskIndexConfig, file: std.fs.File, current_size: u64, seek_offset: u32) !Index {
        _ = config; // May be used for validation in future

        const no_of_indices = current_size / INDEX_SIZE_BYTES;
        if (no_of_indices == 0) {
            return error.IndexOutOfBounds;
        }

        try file.seekTo(0);
        var first_buf: [INDEX_SIZE_BYTES]u8 = undefined;
        _ = try file.readAll(&first_buf);

        var first_index = Index{
            .relative_offset = std.mem.readInt(u32, first_buf[0..4], .little),
            .pos = std.mem.readInt(u32, first_buf[4..8], .little),
            .crc32 = std.mem.readInt(u32, first_buf[8..12], .little),
        };

        if (!first_index.validate()) {
            return error.IndexEntryCorrupted;
        }

        if (seek_offset < first_index.relative_offset) {
            return error.IndexOutOfBounds;
        }

        var nearest_index: ?Index = null;
        var l: u64 = 0;
        var r: u64 = no_of_indices - 1;
        while (l <= r) {
            const mid = l + (r - l) / 2;
            try file.seekTo(mid * INDEX_SIZE_BYTES);

            var index_buf: [INDEX_SIZE_BYTES]u8 = undefined;
            _ = try file.readAll(&index_buf);

            var index = Index{
                .relative_offset = std.mem.readInt(u32, index_buf[0..4], .little),
                .pos = std.mem.readInt(u32, index_buf[4..8], .little),
                .crc32 = std.mem.readInt(u32, index_buf[8..12], .little),
            };

            if (!index.validate()) {
                return error.IndexEntryCorrupted;
            }

            if (index.relative_offset <= seek_offset) {
                nearest_index = index;
                l = mid + 1;
            } else {
                if (mid == 0) break;
                r = mid - 1;
            }
        }

        std.debug.assert(nearest_index != null);

        return nearest_index.?;
    }

    pub fn isFull(config: OnDiskIndexConfig, current_size: u64) bool {
        return current_size + INDEX_SIZE_BYTES > config.index_file_max_size_bytes;
    }

    pub fn getEntryCount(current_size: u64) u64 {
        return current_size / INDEX_SIZE_BYTES;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "OnDiskIndex: add single entry and verify size" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_add_single.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);

    try std.testing.expectEqual(INDEX_SIZE_BYTES, current_size);
}

test "OnDiskIndex: add multiple entries" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_add_multiple.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);
    _ = try OnDiskIndex.add(config, file, &current_size, 20, 8192);
    _ = try OnDiskIndex.add(config, file, &current_size, 30, 12288);

    try std.testing.expectEqual(INDEX_SIZE_BYTES * 4, current_size);
}

test "OnDiskIndex: add returns IndexFileFull when capacity exceeded" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = INDEX_SIZE_BYTES * 2,
        .bytes_per_index = 256,
    };

    const path = "test_full.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 1, 100);

    try std.testing.expectError(error.IndexFileFull, OnDiskIndex.add(config, file, &current_size, 2, 200));
}

test "OnDiskIndex: lookup finds exact match" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_exact.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);
    _ = try OnDiskIndex.add(config, file, &current_size, 20, 8192);

    const result = try OnDiskIndex.lookup(config, file, current_size, 10);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "OnDiskIndex: lookup finds largest entry less than or equal to target" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_le.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);
    _ = try OnDiskIndex.add(config, file, &current_size, 20, 8192);
    _ = try OnDiskIndex.add(config, file, &current_size, 30, 12288);

    const result = try OnDiskIndex.lookup(config, file, current_size, 25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "OnDiskIndex: lookup returns first entry for offset within first interval" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_first.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);

    const result = try OnDiskIndex.lookup(config, file, current_size, 5);
    try std.testing.expectEqual(@as(u32, 0), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 0), result.pos);
}

test "OnDiskIndex: lookup returns last entry for offset beyond last" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_last.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);
    _ = try OnDiskIndex.add(config, file, &current_size, 20, 8192);

    const result = try OnDiskIndex.lookup(config, file, current_size, 25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "OnDiskIndex: lookup on empty index returns error" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_empty.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    const current_size: u64 = 0;
    try std.testing.expectError(error.IndexOutOfBounds, OnDiskIndex.lookup(config, file, current_size, 0));
}

test "OnDiskIndex: lookup with offset before first entry returns error" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_before.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 100, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 200, 4096);

    // Seeking offset 50 which is before the first entry (100)
    try std.testing.expectError(error.IndexOutOfBounds, OnDiskIndex.lookup(config, file, current_size, 50));
}

test "OnDiskIndex: lookup with offset far beyond last entry returns last entry" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_lookup_beyond.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);

    // Seeking beyond last entry should return the last entry
    const result = try OnDiskIndex.lookup(config, file, current_size, 50);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "OnDiskIndex: binary search performance with many entries" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024 * 100,
        .bytes_per_index = 256,
    };

    const path = "test_binary_search.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;

    // Add 100 entries simulating sparse index (every 4KB)
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        _ = try OnDiskIndex.add(config, file, &current_size, i * 10, i * 4096);
    }

    // Lookup various offsets to verify binary search works
    const result1 = try OnDiskIndex.lookup(config, file, current_size, 0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try OnDiskIndex.lookup(config, file, current_size, 555);
    try std.testing.expectEqual(@as(u32, 550), result2.relative_offset);

    const result3 = try OnDiskIndex.lookup(config, file, current_size, 990);
    try std.testing.expectEqual(@as(u32, 990), result3.relative_offset);
}

test "OnDiskIndex: persistence - write, close, reopen, read" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_persistence.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var current_size: u64 = 0;

    // Create, write, and close
    {
        const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
        defer file.close();

        _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
        _ = try OnDiskIndex.add(config, file, &current_size, 42, 16384);
        _ = try OnDiskIndex.add(config, file, &current_size, 100, 32768);
    }

    // Reopen and verify
    {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();

        const result = try OnDiskIndex.lookup(config, file, current_size, 50);
        try std.testing.expectEqual(@as(u32, 42), result.relative_offset);
        try std.testing.expectEqual(@as(u32, 16384), result.pos);
    }
}

test "OnDiskIndex: edge case - single entry lookup" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_single_entry.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);

    const result1 = try OnDiskIndex.lookup(config, file, current_size, 0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try OnDiskIndex.lookup(config, file, current_size, 5);
    try std.testing.expectEqual(@as(u32, 0), result2.relative_offset);
}

test "OnDiskIndex: edge case - lookup at exact base offset" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_base_offset.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;
    _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
    _ = try OnDiskIndex.add(config, file, &current_size, 10, 4096);

    const result = try OnDiskIndex.lookup(config, file, current_size, 0);
    try std.testing.expectEqual(@as(u32, 0), result.relative_offset);
}

test "OnDiskIndex: serialization correctness - little endian with CRC" {
    const config = OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    const path = "test_serialization.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var current_size: u64 = 0;

    // Add entry with specific values to verify byte order
    const test_relative_offset = 0x12345678;
    const test_pos = 0xABCDEF00;
    const index = try OnDiskIndex.add(config, file, &current_size, test_relative_offset, test_pos);

    // Read raw bytes from file
    try file.seekTo(0);
    var buf: [INDEX_SIZE_BYTES]u8 = undefined;
    _ = try file.readAll(&buf);

    // Verify little-endian encoding
    const relative_offset = std.mem.readInt(u32, buf[0..4], .little);
    const pos = std.mem.readInt(u32, buf[4..8], .little);
    const crc32 = std.mem.readInt(u32, buf[8..12], .little);

    try std.testing.expectEqual(@as(u32, test_relative_offset), relative_offset);
    try std.testing.expectEqual(@as(u32, test_pos), pos);
    try std.testing.expectEqual(index.crc32, crc32);
}

test "OnDiskIndex.create creates new index file" {
    const test_dir = "test_ondiskindex_create";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const file_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/test.index", .{abs_path});
    defer std.testing.allocator.free(file_path);

    // Create file
    const file = try OnDiskIndex.create(file_path);
    defer file.close();

    // Verify file exists and is empty
    const stat = try file.stat();
    try std.testing.expectEqual(@as(u64, 0), stat.size);

    // Verify we can write to it
    var buf: [INDEX_SIZE_BYTES]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], 0, .little);
    std.mem.writeInt(u32, buf[4..8], 100, .little);
    std.mem.writeInt(u32, buf[8..12], 0xDEADBEEF, .little);
    try file.writeAll(&buf);

    const stat2 = try file.stat();
    try std.testing.expectEqual(INDEX_SIZE_BYTES, stat2.size);
}

test "OnDiskIndex.open opens existing index file" {
    const test_dir = "test_ondiskindex_open";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const file_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/test.index", .{abs_path});
    defer std.testing.allocator.free(file_path);

    // Create file and write some index entries
    {
        const file = try OnDiskIndex.create(file_path);
        defer file.close();

        const config = OnDiskIndexConfig{};
        var current_size: u64 = 0;
        _ = try OnDiskIndex.add(config, file, &current_size, 0, 0);
        _ = try OnDiskIndex.add(config, file, &current_size, 10, 1024);
    }

    // Open existing file
    const file = try OnDiskIndex.open(file_path);
    defer file.close();

    // Verify file contains existing entries (2 entries)
    const stat = try file.stat();
    try std.testing.expectEqual(INDEX_SIZE_BYTES * 2, stat.size);

    // Verify we can read from it
    const config = OnDiskIndexConfig{};
    const index = try OnDiskIndex.lookup(config, file, stat.size, 10);
    try std.testing.expectEqual(@as(u32, 10), index.relative_offset);
    try std.testing.expectEqual(@as(u32, 1024), index.pos);
}

test "OnDiskIndex.open returns error for non-existent file" {
    const test_dir = "test_ondiskindex_open_error";
    std.fs.cwd().makeDir(test_dir) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(std.testing.allocator, test_dir);
    defer std.testing.allocator.free(abs_path);

    const file_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/nonexistent.index", .{abs_path});
    defer std.testing.allocator.free(file_path);

    // Try to open non-existent file
    try std.testing.expectError(error.FileNotFound, OnDiskIndex.open(file_path));
}
