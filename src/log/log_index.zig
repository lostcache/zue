const std = @import("std");

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

const INDEX_RELATIVE_OFFSET_HOLDER_BYTES = @sizeOf(u32);
const INDEX_POS_HOLDER_BYTES = @sizeOf(u32);
const INDEX_CRC_HOLDER_BYTES = @sizeOf(u32);
pub const INDEX_SIZE_BYTES = INDEX_RELATIVE_OFFSET_HOLDER_BYTES + INDEX_POS_HOLDER_BYTES + INDEX_CRC_HOLDER_BYTES;

pub const Indices = struct {
    base_offset: u64,
    capacity: u64,
    mmap: ?[]align(std.heap.page_size_min) u8,
    size: u64,

    pub fn init(
        base_offset: u64,
        max_size_bytes: u64,
    ) Indices {
        // TODO: Add bounds checking
        return Indices{
            .base_offset = base_offset,
            .capacity = max_size_bytes,
            .mmap = null,
            .size = 0,
        };
    }

    pub fn initFromFile(
        base_offset: u64,
        max_size_bytes: u64,
        current_size: u64,
    ) Indices {
        std.debug.assert(current_size % INDEX_SIZE_BYTES == 0);

        return Indices{
            .base_offset = base_offset,
            .capacity = max_size_bytes,
            .size = current_size,
            .mmap = null,
        };
    }

    pub fn add(self: *Indices, file: std.fs.File, relative_offset: u32, pos: u32) !Index {
        if (self.size + INDEX_SIZE_BYTES > self.capacity) {
            return error.IndexFileFull;
        }

        const index = Index.create(relative_offset, pos);

        const emptyBytePos = self.size;
        try file.seekTo(emptyBytePos);

        var buf: [INDEX_SIZE_BYTES]u8 = undefined;
        std.mem.writeInt(u32, buf[0..4], index.relative_offset, .little);
        std.mem.writeInt(u32, buf[4..8], index.pos, .little);
        std.mem.writeInt(u32, buf[8..12], index.crc32, .little);

        try file.writeAll(&buf);
        self.size += INDEX_SIZE_BYTES;

        return index;
    }

    pub fn lookup(self: *Indices, file: std.fs.File, seek_offset: u32) !Index {
        const no_of_indices = self.size / INDEX_SIZE_BYTES;
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

    pub fn isFull(self: *Indices) bool {
        return self.size + INDEX_SIZE_BYTES > self.capacity;
    }

    pub fn getEntryCount(self: *Indices) u64 {
        return self.size / INDEX_SIZE_BYTES;
    }

    pub fn getLastEntry(self: *Indices, file: std.fs.File) !?Index {
        if (self.size == 0) return null;

        const last_pos = self.size - INDEX_SIZE_BYTES;
        try file.seekTo(last_pos);

        var buf: [INDEX_SIZE_BYTES]u8 = undefined;
        _ = try file.readAll(&buf);

        var index = Index{
            .relative_offset = std.mem.readInt(u32, buf[0..4], .little),
            .pos = std.mem.readInt(u32, buf[4..8], .little),
            .crc32 = std.mem.readInt(u32, buf[8..12], .little),
        };

        if (!index.validate()) {
            return error.IndexEntryCorrupted;
        }

        return index;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "Indices: create new index" {
    const indices = Indices.init(1000, 1024);

    try std.testing.expectEqual(@as(u64, 1000), indices.base_offset);
    try std.testing.expectEqual(@as(u64, 0), indices.size);
    try std.testing.expectEqual(@as(u64, 1024), indices.capacity);
}

test "Indices: open existing index" {
    const indices = Indices.initFromFile(2000, 1024, 24); // 2 entries * 12 bytes

    try std.testing.expectEqual(@as(u64, 2000), indices.base_offset);
    try std.testing.expectEqual(@as(u64, 24), indices.size);
    try std.testing.expectEqual(@as(u64, 1024), indices.capacity);
}

test "Indices: add single entry and verify size" {
    const path = "test_add_single.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);

    try std.testing.expectEqual(@as(u64, INDEX_SIZE_BYTES), indices.size);
}

test "Indices: add multiple entries" {
    const path = "test_add_multiple.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);
    _ = try indices.add(file, 20, 8192);
    _ = try indices.add(file, 30, 12288);

    try std.testing.expectEqual(@as(u64, 4 * INDEX_SIZE_BYTES), indices.size);
}

test "Indices: add returns IndexFileFull when capacity exceeded" {
    const path = "test_full.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(0, INDEX_SIZE_BYTES * 2);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 1, 100);

    try std.testing.expectError(error.IndexFileFull, indices.add(file, 2, 200));
}

test "Indices: lookup finds exact match" {
    const path = "test_lookup_exact.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);
    _ = try indices.add(file, 20, 8192);

    const result = try indices.lookup(file, 10);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "Indices: lookup finds largest entry less than or equal to target" {
    const path = "test_lookup_le.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);
    _ = try indices.add(file, 20, 8192);
    _ = try indices.add(file, 30, 12288);

    const result = try indices.lookup(file, 25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "Indices: lookup returns first entry for offset within first interval" {
    const path = "test_lookup_first.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);

    const result = try indices.lookup(file, 5);
    try std.testing.expectEqual(@as(u32, 0), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 0), result.pos);
}

test "Indices: lookup returns last entry for offset beyond last" {
    const path = "test_lookup_last.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);
    _ = try indices.add(file, 20, 8192);

    const result = try indices.lookup(file, 25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "Indices: lookup on empty index returns error" {
    const path = "test_lookup_empty.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    try std.testing.expectError(error.IndexOutOfBounds, indices.lookup(file, 0));
}

test "Indices: lookup with offset before first entry returns error" {
    const path = "test_lookup_before.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 100, 0);
    _ = try indices.add(file, 200, 4096);

    // Seeking offset 50 which is before the first entry (100)
    try std.testing.expectError(error.IndexOutOfBounds, indices.lookup(file, 50));
}

test "Indices: lookup with offset far beyond last entry returns last entry" {
    const path = "test_lookup_beyond.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);

    // Seeking beyond last entry should return the last entry
    const result = try indices.lookup(file, 50);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "Indices: open existing index file" {
    const path = "test_open.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    // Create and populate an index
    {
        const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
        defer file.close();

        var indices = Indices.init(2000, 1024);

        _ = try indices.add(file, 0, 0);
        _ = try indices.add(file, 5, 2048);
        _ = try indices.add(file, 15, 6144);
    }

    // Open the existing index
    const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();

    const file_stat = try file.stat();
    var indices = Indices.initFromFile(2000, 1024, file_stat.size);

    try std.testing.expectEqual(@as(u64, 2000), indices.base_offset);
    try std.testing.expectEqual(@as(u64, 3 * INDEX_SIZE_BYTES), indices.size);

    // Verify we can lookup entries
    const result = try indices.lookup(file, 10);
    try std.testing.expectEqual(@as(u32, 5), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 2048), result.pos);
}

test "Indices: open with invalid path" {
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().openFile("", .{ .mode = .read_write }));
}

test "Indices: binary search performance with many entries" {
    const path = "test_binary_search.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(0, 1024 * 1024);

    // Add 100 entries simulating sparse index (every 4KB)
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        _ = try indices.add(file, i * 10, i * 4096);
    }

    try std.testing.expectEqual(@as(u64, 100 * INDEX_SIZE_BYTES), indices.size);

    // Lookup various offsets to verify binary search works
    const result1 = try indices.lookup(file, 0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try indices.lookup(file, 555);
    try std.testing.expectEqual(@as(u32, 550), result2.relative_offset);

    const result3 = try indices.lookup(file, 990);
    try std.testing.expectEqual(@as(u32, 990), result3.relative_offset);
}

test "Indices: persistence - write, close, reopen, read" {
    const path = "test_persistence.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const base_offset: u64 = 5000;

    // Create, write, and close
    {
        const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
        defer file.close();

        var indices = Indices.init(base_offset, 1024);
        _ = try indices.add(file, 0, 0);
        _ = try indices.add(file, 42, 16384);
        _ = try indices.add(file, 100, 32768);
    }

    // Reopen and verify
    {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();

        const file_stat = try file.stat();
        var indices = Indices.initFromFile(base_offset, 1024, file_stat.size);

        const result = try indices.lookup(file, 50);
        try std.testing.expectEqual(@as(u32, 42), result.relative_offset);
        try std.testing.expectEqual(@as(u32, 16384), result.pos);
    }
}

test "Indices: edge case - single entry lookup" {
    const path = "test_single_entry.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);

    const result1 = try indices.lookup(file, 0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try indices.lookup(file, 5);
    try std.testing.expectEqual(@as(u32, 0), result2.relative_offset);
}

test "Indices: edge case - lookup at exact base offset" {
    const path = "test_base_offset.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(1000, 1024);

    _ = try indices.add(file, 0, 0);
    _ = try indices.add(file, 10, 4096);

    const result = try indices.lookup(file, 0);
    try std.testing.expectEqual(@as(u32, 0), result.relative_offset);
}

test "Indices: serialization correctness - little endian with CRC" {
    const path = "test_serialization.index";
    defer std.fs.cwd().deleteFile(path) catch {};

    const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
    defer file.close();

    var indices = Indices.init(0, 1024);

    // Add entry with specific values to verify byte order
    const test_relative_offset = 0x12345678;
    const test_pos = 0xABCDEF00;
    const index = try indices.add(file, test_relative_offset, test_pos);

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
