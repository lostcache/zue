const std = @import("std");
const mmap = @import("mmap.zig");
const log_index = @import("log_index.zig");

pub const MmapIndex = struct {
    mmap_file: mmap.MmapFile,
    config: log_index.OnDiskIndexConfig,
    current_size: u64,

    pub fn create(file_path: []const u8, config: log_index.OnDiskIndexConfig) !MmapIndex {
        // TODO: use constant values or add to configurables
        const initial_size = log_index.INDEX_SIZE_BYTES * 16; // Start with space for 16 entries
        const mmap_file = try mmap.MmapFile.create(file_path, initial_size);

        return MmapIndex{
            .mmap_file = mmap_file,
            .config = config,
            .current_size = 0,
        };
    }

    pub fn open(file_path: []const u8, config: log_index.OnDiskIndexConfig) !MmapIndex {
        const file = try std.fs.openFileAbsolute(file_path, .{ .mode = .read_write });
        const stat = try file.stat();
        const file_size = stat.size;
        file.close();

        const mmap_file = try mmap.MmapFile.openWrite(file_path, @max(file_size, log_index.INDEX_SIZE_BYTES * 16));

        return MmapIndex{
            .mmap_file = mmap_file,
            .config = config,
            .current_size = 0,
        };
    }

    pub fn close(self: *MmapIndex) void {
        self.mmap_file.close();
    }

    pub fn add(self: *MmapIndex, relative_offset: u32, pos: u32) !log_index.Index {
        if (self.current_size + log_index.INDEX_SIZE_BYTES > self.config.index_file_max_size_bytes) {
            return error.IndexFileFull;
        }

        const index = log_index.Index.create(relative_offset, pos);

        if (self.current_size + log_index.INDEX_SIZE_BYTES > self.mmap_file.len()) {
            // TODO: use constant values or add to configurables
            const new_size = self.mmap_file.len() + (log_index.INDEX_SIZE_BYTES * 16); // Grow by 16 entries
            const capped_size = @min(new_size, self.config.index_file_max_size_bytes);
            try self.mmap_file.extend(capped_size);
        }

        const slice = self.mmap_file.asSlice();
        const offset = self.current_size;

        std.mem.writeInt(u32, slice[offset..][0..4], index.relative_offset, .little);
        std.mem.writeInt(u32, slice[offset + 4 ..][0..4], index.pos, .little);
        std.mem.writeInt(u32, slice[offset + 8 ..][0..4], index.crc32, .little);

        self.current_size += log_index.INDEX_SIZE_BYTES;

        return index;
    }

    pub fn lookup(self: *const MmapIndex, seek_offset: u32) !log_index.Index {
        const no_of_indices = self.current_size / log_index.INDEX_SIZE_BYTES;
        if (no_of_indices == 0) {
            return error.IndexOutOfBounds;
        }

        const slice = self.mmap_file.asConstSlice();

        const first_index = self.readIndexAt(slice, 0);
        if (!first_index.validate()) {
            return error.IndexEntryCorrupted;
        }
        if (seek_offset < first_index.relative_offset) {
            return error.IndexOutOfBounds;
        }

        var nearest_index: ?log_index.Index = null;
        var l: u64 = 0;
        var r: u64 = no_of_indices - 1;

        while (l <= r) {
            const mid = l + (r - l) / 2;
            const index = self.readIndexAt(slice, mid);

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

    fn readIndexAt(_: *const MmapIndex, slice: []const u8, index_pos: u64) log_index.Index {
        const offset = index_pos * log_index.INDEX_SIZE_BYTES;
        return log_index.Index{
            .relative_offset = std.mem.readInt(u32, slice[offset..][0..4], .little),
            .pos = std.mem.readInt(u32, slice[offset + 4 ..][0..4], .little),
            .crc32 = std.mem.readInt(u32, slice[offset + 8 ..][0..4], .little),
        };
    }

    pub fn isFull(self: *const MmapIndex) bool {
        return self.current_size + log_index.INDEX_SIZE_BYTES > self.config.index_file_max_size_bytes;
    }

    pub fn getEntryCount(self: *const MmapIndex) u64 {
        return self.current_size / log_index.INDEX_SIZE_BYTES;
    }

    pub fn sync(self: *MmapIndex) !void {
        try self.mmap_file.sync();
    }

    pub fn syncAsync(self: *MmapIndex) !void {
        try self.mmap_file.syncAsync();
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "MmapIndex: create and add entries" {
    const test_path = "/tmp/test_mmap_index_create.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    var index = try MmapIndex.create(test_path, config);
    defer index.close();

    const entry1 = try index.add(0, 0);
    try std.testing.expectEqual(@as(u32, 0), entry1.relative_offset);
    try std.testing.expectEqual(@as(u32, 0), entry1.pos);
    try std.testing.expect(entry1.validate());

    const entry2 = try index.add(10, 4096);
    try std.testing.expectEqual(@as(u32, 10), entry2.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), entry2.pos);

    try std.testing.expectEqual(@as(u64, 2), index.getEntryCount());
}

test "MmapIndex: lookup exact match" {
    const test_path = "/tmp/test_mmap_index_lookup.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    var index = try MmapIndex.create(test_path, config);
    defer index.close();

    _ = try index.add(0, 0);
    _ = try index.add(10, 4096);
    _ = try index.add(20, 8192);

    const result = try index.lookup(10);
    try std.testing.expectEqual(@as(u32, 10), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 4096), result.pos);
}

test "MmapIndex: lookup nearest (less than or equal)" {
    const test_path = "/tmp/test_mmap_index_lookup_le.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    var index = try MmapIndex.create(test_path, config);
    defer index.close();

    _ = try index.add(0, 0);
    _ = try index.add(10, 4096);
    _ = try index.add(20, 8192);
    _ = try index.add(30, 12288);

    const result = try index.lookup(25);
    try std.testing.expectEqual(@as(u32, 20), result.relative_offset);
    try std.testing.expectEqual(@as(u32, 8192), result.pos);
}

test "MmapIndex: index treated as cache (rebuilds on open)" {
    const test_path = "/tmp/test_mmap_index_persistence.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024,
        .bytes_per_index = 256,
    };

    // Create and write
    {
        var index = try MmapIndex.create(test_path, config);
        defer index.close();

        _ = try index.add(0, 0);
        _ = try index.add(42, 16384);
        _ = try index.add(100, 32768);

        try index.sync();
    }

    // Reopen - index starts empty as it's treated as a cache
    // In practice, MmapSegment will rebuild it from the log
    {
        var index = try MmapIndex.open(test_path, config);
        defer index.close();

        // Index starts empty - it's just a cache, not the source of truth
        try std.testing.expectEqual(@as(u64, 0), index.getEntryCount());

        // Can still add new entries after opening
        _ = try index.add(0, 0);
        _ = try index.add(50, 20000);

        const result = try index.lookup(50);
        try std.testing.expectEqual(@as(u32, 50), result.relative_offset);
        try std.testing.expectEqual(@as(u32, 20000), result.pos);
    }
}

test "MmapIndex: index file full" {
    const test_path = "/tmp/test_mmap_index_full.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{
        .index_file_max_size_bytes = log_index.INDEX_SIZE_BYTES * 2,
        .bytes_per_index = 256,
    };

    var index = try MmapIndex.create(test_path, config);
    defer index.close();

    _ = try index.add(0, 0);
    try std.testing.expect(!index.isFull());

    _ = try index.add(1, 100);
    try std.testing.expect(index.isFull());

    try std.testing.expectError(error.IndexFileFull, index.add(2, 200));
}

test "MmapIndex: empty index lookup returns error" {
    const test_path = "/tmp/test_mmap_index_empty.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{};
    var index = try MmapIndex.create(test_path, config);
    defer index.close();

    try std.testing.expectError(error.IndexOutOfBounds, index.lookup(0));
}

test "MmapIndex: many entries (stress test)" {
    const test_path = "/tmp/test_mmap_index_stress.index";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    const config = log_index.OnDiskIndexConfig{
        .index_file_max_size_bytes = 1024 * 100,
        .bytes_per_index = 256,
    };

    var index = try MmapIndex.create(test_path, config);
    defer index.close();

    // Add 100 entries
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        _ = try index.add(i * 10, i * 4096);
    }

    try std.testing.expectEqual(@as(u64, 100), index.getEntryCount());

    // Verify lookups work correctly
    const result1 = try index.lookup(0);
    try std.testing.expectEqual(@as(u32, 0), result1.relative_offset);

    const result2 = try index.lookup(555);
    try std.testing.expectEqual(@as(u32, 550), result2.relative_offset);

    const result3 = try index.lookup(990);
    try std.testing.expectEqual(@as(u32, 990), result3.relative_offset);
}
