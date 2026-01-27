const std = @import("std");
const mmap = @import("mmap.zig");
const record = @import("record.zig");

pub const MmapLogWriter = struct {
    mmap_file: mmap.MmapFile,
    config: record.OnDiskLogConfig,
    current_pos: u64,
    file_path: []const u8,
    allocator: std.mem.Allocator,

    pub fn create(file_path: []const u8, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) !MmapLogWriter {
        const path_copy = try allocator.dupe(u8, file_path);
        errdefer allocator.free(path_copy);

        const initial_size = 1024 * 1024;
        const mmap_file = try mmap.MmapFile.create(file_path, initial_size);

        return MmapLogWriter{
            .mmap_file = mmap_file,
            .config = config,
            .current_pos = 0,
            .file_path = path_copy,
            .allocator = allocator,
        };
    }

    pub fn open(file_path: []const u8, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) !MmapLogWriter {
        const path_copy = try allocator.dupe(u8, file_path);
        errdefer allocator.free(path_copy);

        const file_size = blk: {
            const file = try std.fs.openFileAbsolute(file_path, .{});
            defer file.close();
            const stat = try file.stat();
            break :blk stat.size;
        };

        const min_size = @max(file_size, 1024 * 1024);
        const mmap_file = try mmap.MmapFile.openWrite(file_path, min_size);
        errdefer mmap_file.close();

        const actual_size = scanForValidEndWriter(mmap_file, config, allocator);

        return MmapLogWriter{
            .mmap_file = mmap_file,
            .config = config,
            .current_pos = actual_size,
            .file_path = path_copy,
            .allocator = allocator,
        };
    }

    fn scanForValidEndWriter(mmap_file: mmap.MmapFile, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) u64 {
        const slice = mmap_file.asConstSlice();
        var pos: u64 = 0;

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const temp_alloc = arena.allocator();

        while (pos < slice.len) {
            var stream = std.io.fixedBufferStream(slice[pos..]);
            const reader = stream.reader();

            const rec = record.OnDiskLog.deserialize(config, reader, temp_alloc) catch {
                break;
            };

            const rec_size = record.OnDiskLog.serializedSize(config, rec);
            pos += rec_size;

            _ = arena.reset(.retain_capacity);
        }

        return pos;
    }

    pub fn close(self: *MmapLogWriter) void {
        defer self.allocator.free(self.file_path);
        self.mmap_file.sync() catch {};
        self.mmap_file.close();
    }

    pub fn append(self: *MmapLogWriter, rec: record.Record) !usize {
        const rec_size = record.OnDiskLog.serializedSize(self.config, rec);

        if (self.current_pos + rec_size > self.mmap_file.len()) {
            const extension = @max(1024 * 1024, rec_size);
            const new_size = self.mmap_file.len() + extension;

            if (new_size > self.config.log_file_max_size_bytes) {
                return error.LogFileFull;
            }

            try self.mmap_file.extend(new_size);
        }

        const slice = self.mmap_file.asSlice();
        var stream = std.io.fixedBufferStream(slice[self.current_pos..]);
        const writer = stream.writer();

        try record.OnDiskLog.serializeWrite(self.config, rec, writer);

        const written_pos = self.current_pos;
        self.current_pos += rec_size;

        return written_pos;
    }

    pub fn getCurrentPos(self: *const MmapLogWriter) u64 {
        return self.current_pos;
    }

    pub fn sync(self: *MmapLogWriter) !void {
        try self.mmap_file.sync();
    }

    pub fn syncAsync(self: *MmapLogWriter) !void {
        try self.mmap_file.syncAsync();
    }
};
