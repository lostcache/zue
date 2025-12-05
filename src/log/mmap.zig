const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

pub const MmapFile = struct {
    file: std.fs.File,
    mapped_memory: []align(std.heap.page_size_min) u8,
    writable: bool,

    pub fn openRead(file_path: []const u8) !MmapFile {
        const file = try std.fs.openFileAbsolute(file_path, .{ .mode = .read_only });
        errdefer file.close();

        const stat = try file.stat();
        const file_size = stat.size;

        if (file_size == 0) {
            // Empty file - return with empty mapping
            return MmapFile{
                .file = file,
                .mapped_memory = &[_]u8{},
                .writable = false,
            };
        }

        const mapped_memory = try posix.mmap(
            null,
            file_size,
            posix.PROT.READ,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        return MmapFile{
            .file = file,
            .mapped_memory = mapped_memory,
            .writable = false,
        };
    }

    pub fn openWrite(file_path: []const u8, min_size: usize) !MmapFile {
        const file = try std.fs.openFileAbsolute(file_path, .{ .mode = .read_write });
        errdefer file.close();

        const stat = try file.stat();
        var file_size = stat.size;

        // Extend file if necessary
        if (file_size < min_size) {
            try file.setEndPos(min_size);
            file_size = min_size;
        }

        if (file_size == 0) {
            return MmapFile{
                .file = file,
                .mapped_memory = &[_]u8{},
                .writable = true,
            };
        }

        const mapped_memory = try posix.mmap(
            null,
            file_size,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        return MmapFile{
            .file = file,
            .mapped_memory = mapped_memory,
            .writable = true,
        };
    }

    pub fn create(file_path: []const u8, initial_size: usize) !MmapFile {
        const file = try std.fs.createFileAbsolute(file_path, .{ .truncate = true, .read = true });
        errdefer file.close();

        if (initial_size == 0) {
            return MmapFile{
                .file = file,
                .mapped_memory = &[_]u8{},
                .writable = true,
            };
        }

        try file.setEndPos(initial_size);

        const mapped_memory = try posix.mmap(
            null,
            initial_size,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        return MmapFile{
            .file = file,
            .mapped_memory = mapped_memory,
            .writable = true,
        };
    }

    pub fn asSlice(self: *const MmapFile) []u8 {
        return @constCast(self.mapped_memory);
    }

    pub fn asConstSlice(self: *const MmapFile) []const u8 {
        return self.mapped_memory;
    }

    pub fn len(self: *const MmapFile) usize {
        return self.mapped_memory.len;
    }

    pub fn sync(self: *MmapFile) !void {
        if (!self.writable or self.mapped_memory.len == 0) return;
        try posix.msync(self.mapped_memory, posix.MSF.SYNC);
    }

    pub fn syncAsync(self: *MmapFile) !void {
        if (!self.writable or self.mapped_memory.len == 0) return;
        try posix.msync(self.mapped_memory, posix.MSF.ASYNC);
    }

    pub fn close(self: *MmapFile) void {
        if (self.mapped_memory.len > 0) {
            posix.munmap(self.mapped_memory);
        }
        self.file.close();
    }

    /// Extend the mapping to a new size
    /// Platform-specific implementation:
    /// - Linux: Uses mremap() which can efficiently extend the mapping in-place
    ///   or move it if necessary, avoiding full unmap/remap cycle
    /// - macOS/Other: Falls back to unmap/remap which has higher performance penalty
    ///   due to lack of mremap support
    pub fn extend(self: *MmapFile, new_size: usize) !void {
        if (new_size <= self.mapped_memory.len) return;
        if (!self.writable) return error.NotWritable;

        try self.file.setEndPos(new_size);

        if (builtin.os.tag == .linux) {
            const old_size = self.mapped_memory.len;

            if (old_size == 0) {
                // Empty mapping - need to create new mapping
                self.mapped_memory = try posix.mmap(
                    null,
                    new_size,
                    posix.PROT.READ | posix.PROT.WRITE,
                    .{ .TYPE = .SHARED },
                    self.file.handle,
                    0,
                );
            } else {
                const linux = std.os.linux;

                const new_addr = linux.mremap(
                    self.mapped_memory.ptr,
                    old_size,
                    new_size,
                    .{ .MAYMOVE = true },
                    null,
                );

                const err_int = @as(isize, @bitCast(new_addr));
                if (err_int < 0 and err_int > -4096) {
                    const err = @as(linux.E, @enumFromInt(-err_int));
                    return posix.unexpectedErrno(err);
                }

                self.mapped_memory = @as([*]align(std.heap.page_size_min) u8, @ptrFromInt(new_addr))[0..new_size];
            }
        } else {
            // macOS and other platforms: Fall back to unmap/remap
            // This is less efficient as it requires unmapping the entire region,
            // potentially losing TLB entries and requiring page table updates
            if (self.mapped_memory.len > 0) {
                posix.munmap(self.mapped_memory);
            }

            self.mapped_memory = try posix.mmap(
                null,
                new_size,
                posix.PROT.READ | posix.PROT.WRITE,
                .{ .TYPE = .SHARED },
                self.file.handle,
                0,
            );
        }
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "MmapFile: create and write" {
    const test_path = "/tmp/test_mmap_create.dat";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    var mmap_file = try MmapFile.create(test_path, 4096);
    defer mmap_file.close();

    const slice = mmap_file.asSlice();
    try std.testing.expectEqual(@as(usize, 4096), slice.len);

    // Write some data
    @memcpy(slice[0..5], "Hello");
    try mmap_file.sync();
}

test "MmapFile: create empty file" {
    const test_path = "/tmp/test_mmap_empty.dat";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    var mmap_file = try MmapFile.create(test_path, 0);
    defer mmap_file.close();

    const slice = mmap_file.asSlice();
    try std.testing.expectEqual(@as(usize, 0), slice.len);
}

test "MmapFile: openRead" {
    const test_path = "/tmp/test_mmap_read.dat";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    // Create file with some data
    {
        const file = try std.fs.createFileAbsolute(test_path, .{});
        defer file.close();
        try file.writeAll("Test data");
    }

    // Open with mmap for reading
    var mmap_file = try MmapFile.openRead(test_path);
    defer mmap_file.close();

    const slice = mmap_file.asConstSlice();
    try std.testing.expectEqualStrings("Test data", slice);
}

test "MmapFile: openWrite and modify" {
    const test_path = "/tmp/test_mmap_write.dat";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    // Create file with some data
    {
        const file = try std.fs.createFileAbsolute(test_path, .{});
        defer file.close();
        try file.writeAll("Original");
    }

    // Open with mmap for writing
    var mmap_file = try MmapFile.openWrite(test_path, 8);
    defer mmap_file.close();

    const slice = mmap_file.asSlice();
    @memcpy(slice[0..8], "Modified");
    try mmap_file.sync();

    // Verify changes persisted
    {
        const file = try std.fs.openFileAbsolute(test_path, .{});
        defer file.close();
        var buf: [8]u8 = undefined;
        _ = try file.readAll(&buf);
        try std.testing.expectEqualStrings("Modified", &buf);
    }
}

test "MmapFile: extend mapping" {
    const test_path = "/tmp/test_mmap_extend.dat";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    var mmap_file = try MmapFile.create(test_path, 1024);
    defer mmap_file.close();

    try std.testing.expectEqual(@as(usize, 1024), mmap_file.len());

    try mmap_file.extend(2048);
    try std.testing.expectEqual(@as(usize, 2048), mmap_file.len());

    // Write to extended region
    const slice = mmap_file.asSlice();
    slice[1500] = 42;
    try mmap_file.sync();
}

test "MmapFile: read from offset" {
    const test_path = "/tmp/test_mmap_offset.dat";
    defer std.fs.deleteFileAbsolute(test_path) catch {};

    // Create file with known data
    {
        const file = try std.fs.createFileAbsolute(test_path, .{});
        defer file.close();
        try file.writeAll("0123456789ABCDEF");
    }

    var mmap_file = try MmapFile.openRead(test_path);
    defer mmap_file.close();

    const slice = mmap_file.asConstSlice();
    try std.testing.expectEqualStrings("456", slice[4..7]);
    try std.testing.expectEqualStrings("ABCDEF", slice[10..16]);
}
