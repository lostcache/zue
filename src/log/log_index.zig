const std = @import("std");

const Index = struct {
    relative_offset: u32,
    pos: u32,
};

const INDEX_RELATIVE_OFFSET_HOLDER_BYTES = @sizeOf(u32);
const INDEX_POS_HOLDER_BYTES = @sizeOf(u32);
const INDEX_SIZE_BYTES = INDEX_RELATIVE_OFFSET_HOLDER_BYTES + INDEX_POS_HOLDER_BYTES;

const IndexFile = struct {
    file: std.fs.File,
    base_offset: u64,
    capacity: u64,
    mmap: ?[]align(std.heap.page_size_min) u8,
    size: u64,

    pub fn create(
        path: []const u8,
        base_offset: u64,
        max_size_bytes: u64,
    ) !IndexFile {
        if (path.len <= 0) {
            return error.InvalidPath;
        }

        const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });

        return IndexFile{
            .file = file,
            .base_offset = base_offset,
            .capacity = max_size_bytes,
            .mmap = null,
            .size = 0,
        };
    }

    pub fn open(
        path: []const u8,
        base_offset: u64,
        max_size_bytes: u64,
    ) !IndexFile {
        if (path.len <= 0) {
            return error.InvalidPath;
        }

        const file: std.fs.File = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        const file_size = try file.getEndPos();

        std.debug.assert(file_size % INDEX_SIZE_BYTES == 0);

        return IndexFile{
            .file = file,
            .base_offset = base_offset,
            .capacity = max_size_bytes,
            .size = file_size,
            .mmap = null,
        };
    }

    pub fn close(self: *IndexFile) void {
        self.file.close();
    }

    pub fn add(self: *IndexFile, index: Index) !Index {
        if (self.size + INDEX_SIZE_BYTES > self.capacity) {
            return error.IndexFileFull;
        }

        const emptyBytePos = self.size;
        try self.file.seekTo(emptyBytePos);

        var buf: [INDEX_SIZE_BYTES]u8 = undefined;
        std.mem.writeInt(u32, buf[0..INDEX_RELATIVE_OFFSET_HOLDER_BYTES], index.relative_offset, .little);
        std.mem.writeInt(u32, buf[INDEX_RELATIVE_OFFSET_HOLDER_BYTES..INDEX_SIZE_BYTES], index.pos, .little);

        try self.file.writeAll(&buf);
        self.size += INDEX_SIZE_BYTES;

        return index;
    }

    pub fn lookup(self: *IndexFile, seek_offset: u32) !Index {
        const no_of_indices = self.size / INDEX_SIZE_BYTES;
        if (no_of_indices == 0) {
            return error.IndexOutOfBounds;
        }

        try self.file.seekTo(0);
        var first_buf: [INDEX_SIZE_BYTES]u8 = undefined;
        _ = try self.file.readAll(&first_buf);
        const first_relative_offset = std.mem.readInt(u32, first_buf[0..INDEX_RELATIVE_OFFSET_HOLDER_BYTES], .little);

        if (seek_offset < first_relative_offset) {
            return error.IndexOutOfBounds;
        }

        var nearest_index: ?Index = null;
        var l: u64 = 0;
        var r: u64 = no_of_indices - 1;
        while (l <= r) {
            const mid = l + (r - l) / 2;
            try self.file.seekTo(mid * INDEX_SIZE_BYTES);

            var index_buf: [INDEX_SIZE_BYTES]u8 = undefined;
            _ = try self.file.readAll(&index_buf);

            const relative_offset = std.mem.readInt(u32, index_buf[0..INDEX_RELATIVE_OFFSET_HOLDER_BYTES], .little);
            const pos = std.mem.readInt(u32, index_buf[INDEX_RELATIVE_OFFSET_HOLDER_BYTES..INDEX_SIZE_BYTES], .little);

            if (relative_offset <= seek_offset) {
                nearest_index = Index{ .relative_offset = relative_offset, .pos = pos };
                l = mid + 1;
            } else {
                if (mid == 0) break;
                r = mid - 1;
            }
        }

        std.debug.assert(nearest_index != null);

        return nearest_index.?;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "IndexFile: create new index file" {
    const path = "test_create.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    try std.testing.expectEqual(@as(u64, 1000), index_file.base_offset);
    try std.testing.expectEqual(@as(u64, 0), index_file.size);
    try std.testing.expectEqual(@as(u64, 1024), index_file.capacity);
}

test "IndexFile: create with invalid path" {
    try std.testing.expectError(error.InvalidPath, IndexFile.create("", 0, 1024));
}

test "IndexFile: create with existing file fails" {
    const path = "test_exclusive.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const index1 = try IndexFile.create(path, 0, 1024);
    defer index1.file.close();

    try std.testing.expectError(error.PathAlreadyExists, IndexFile.create(path, 0, 1024));
}

test "IndexFile: add single entry and verify size" {
    const path = "test_add_single.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    const index = Index{ .relative_offset = 0, .pos = 0 };
    _ = try index_file.add(index);

    try std.testing.expectEqual(@as(u64, INDEX_SIZE_BYTES), index_file.size);
}

test "IndexFile: add multiple entries" {
    const path = "test_add_multiple.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });
    _ = try index_file.add(Index{ .relative_offset = 20, .pos = 8192 });
    _ = try index_file.add(Index{ .relative_offset = 30, .pos = 12288 });

    try std.testing.expectEqual(@as(u64, 4 * INDEX_SIZE_BYTES), index_file.size);
}

test "IndexFile: add returns IndexFileFull when capacity exceeded" {
    const path = "test_full.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 0, INDEX_SIZE_BYTES * 2);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 1, .pos = 100 });

    try std.testing.expectError(error.IndexFileFull, index_file.add(Index{ .relative_offset = 2, .pos = 200 }));
}

test "IndexFile: lookup finds exact match" {
    const path = "test_lookup_exact.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });
    _ = try index_file.add(Index{ .relative_offset = 20, .pos = 8192 });

    const result = try index_file.lookup(10);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "IndexFile: lookup finds largest entry less than or equal to target" {
    const path = "test_lookup_le.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });
    _ = try index_file.add(Index{ .relative_offset = 20, .pos = 8192 });
    _ = try index_file.add(Index{ .relative_offset = 30, .pos = 12288 });

    const result = try index_file.lookup(25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "IndexFile: lookup returns first entry for offset within first interval" {
    const path = "test_lookup_first.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });

    const result = try index_file.lookup(5);
    try std.testing.expectEqual(@as(u32, 0), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 0), result.pos);
}

test "IndexFile: lookup returns last entry for offset beyond last" {
    const path = "test_lookup_last.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });
    _ = try index_file.add(Index{ .relative_offset = 20, .pos = 8192 });

    const result = try index_file.lookup(25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "IndexFile: lookup on empty index returns error" {
    const path = "test_lookup_empty.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    try std.testing.expectError(error.IndexOutOfBounds, index_file.lookup(0));
}

test "IndexFile: lookup with offset before first entry returns error" {
    const path = "test_lookup_before.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 100, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 200, .pos = 4096 });

    // Seeking offset 50 which is before the first entry (100)
    try std.testing.expectError(error.IndexOutOfBounds, index_file.lookup(50));
}

test "IndexFile: lookup with offset far beyond last entry returns last entry" {
    const path = "test_lookup_beyond.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });

    // Seeking beyond last entry should return the last entry
    const result = try index_file.lookup(50);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "IndexFile: open existing index file" {
    const path = "test_open.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    // Create and populate an index
    {
        var index_file = try IndexFile.create(path, 2000, 1024);
        defer index_file.file.close();

        _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
        _ = try index_file.add(Index{ .relative_offset = 5, .pos = 2048 });
        _ = try index_file.add(Index{ .relative_offset = 15, .pos = 6144 });
    }

    // Open the existing index
    var reopened = try IndexFile.open(path, 2000, 1024);
    defer reopened.file.close();

    try std.testing.expectEqual(@as(u64, 2000), reopened.base_offset);
    try std.testing.expectEqual(@as(u64, 3 * INDEX_SIZE_BYTES), reopened.size);

    // Verify we can lookup entries
    const result = try reopened.lookup(10);
    try std.testing.expectEqual(@as(u32, 5), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 2048), result.pos);
}

test "IndexFile: open with invalid path" {
    try std.testing.expectError(error.InvalidPath, IndexFile.open("", 0, 1024));
}

test "IndexFile: binary search performance with many entries" {
    const path = "test_binary_search.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 0, 1024 * 1024);
    defer index_file.file.close();

    // Add 100 entries simulating sparse index (every 4KB)
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        _ = try index_file.add(Index{
            .relative_offset = i * 10,
            .pos = i * 4096,
        });
    }

    try std.testing.expectEqual(@as(u64, 100 * INDEX_SIZE_BYTES), index_file.size);

    // Lookup various offsets to verify binary search works
    const result1 = try index_file.lookup(0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try index_file.lookup(555);
    try std.testing.expectEqual(@as(u32, 550), result2.relative_offset);

    const result3 = try index_file.lookup(990);
    try std.testing.expectEqual(@as(u32, 990), result3.relative_offset);
}

test "IndexFile: persistence - write, close, reopen, read" {
    const path = "test_persistence.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const base_offset: u64 = 5000;

    // Create, write, and close
    {
        var index_file = try IndexFile.create(path, base_offset, 1024);
        _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
        _ = try index_file.add(Index{ .relative_offset = 42, .pos = 16384 });
        _ = try index_file.add(Index{ .relative_offset = 100, .pos = 32768 });
        index_file.close();
    }

    // Reopen and verify
    {
        var index_file = try IndexFile.open(path, base_offset, 1024);
        defer index_file.close();

        const result = try index_file.lookup(50);
        try std.testing.expectEqual(@as(u32, 42), result.relative_offset);
        try std.testing.expectEqual(@as(u32, 16384), result.pos);
    }
}

test "IndexFile: edge case - single entry lookup" {
    const path = "test_single_entry.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });

    const result1 = try index_file.lookup(0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try index_file.lookup(5);
    try std.testing.expectEqual(@as(u32, 0), result2.relative_offset);
}

test "IndexFile: edge case - lookup at exact base offset" {
    const path = "test_base_offset.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 1000, 1024);
    defer index_file.file.close();

    _ = try index_file.add(Index{ .relative_offset = 0, .pos = 0 });
    _ = try index_file.add(Index{ .relative_offset = 10, .pos = 4096 });

    const result = try index_file.lookup(0);
    try std.testing.expectEqual(@as(u32, 0), result.relative_offset);
}

test "IndexFile: serialization correctness - little endian" {
    const path = "test_serialization.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    var index_file = try IndexFile.create(path, 0, 1024);
    defer index_file.file.close();

    // Add entry with specific values to verify byte order
    const test_relative_offset = 0x12345678;
    const test_pos = 0xABCDEF00;
    _ = try index_file.add(Index{
        .relative_offset = test_relative_offset,
        .pos = test_pos,
    });

    // Read raw bytes from file
    try index_file.file.seekTo(0);
    var buf: [INDEX_SIZE_BYTES]u8 = undefined;
    _ = try index_file.file.readAll(&buf);

    // Verify little-endian encoding
    const relative_offset = std.mem.readInt(u32, buf[0..4], .little);
    const pos = std.mem.readInt(u32, buf[4..8], .little);

    try std.testing.expectEqual(@as(u32, test_relative_offset), relative_offset);
    try std.testing.expectEqual(@as(u32, test_pos), pos);
}
