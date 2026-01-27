const std = @import("std");
const mmap = @import("mmap.zig");
const record = @import("record.zig");

pub const MmapLogReader = struct {
    mmap_file: mmap.MmapFile,
    config: record.OnDiskLogConfig,
    actual_size: u64,

    pub fn open(file_path: []const u8, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) !MmapLogReader {
        const mmap_file = try mmap.MmapFile.openRead(file_path);
        errdefer mmap_file.close();

        const actual_size = scanForValidEnd(mmap_file, config, allocator);

        return MmapLogReader{
            .mmap_file = mmap_file,
            .config = config,
            .actual_size = actual_size,
        };
    }

    fn scanForValidEnd(mmap_file: mmap.MmapFile, config: record.OnDiskLogConfig, allocator: std.mem.Allocator) u64 {
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

    pub fn close(self: *MmapLogReader) void {
        self.mmap_file.close();
    }

    pub fn deserializeAt(self: *const MmapLogReader, pos: u64, allocator: std.mem.Allocator) !record.Record {
        const slice = self.mmap_file.asConstSlice();

        if (pos >= self.actual_size) {
            return error.EndOfStream;
        }

        if (pos >= slice.len) {
            return error.EndOfStream;
        }

        var stream = std.io.fixedBufferStream(slice[pos..]);
        const reader = stream.reader();

        return try record.OnDiskLog.deserialize(self.config, reader, allocator);
    }

    pub fn recordSizeAt(self: *const MmapLogReader, pos: u64) !usize {
        const slice = self.mmap_file.asConstSlice();

        if (pos >= self.actual_size) {
            return error.EndOfStream;
        }

        if (pos >= slice.len) {
            return error.EndOfStream;
        }

        const min_header = 16; // CRC(4) + Timestamp(8) + KeyLen(4)
        if (pos + min_header > self.actual_size) {
            return error.IncompleteRecord;
        }

        if (pos + min_header > slice.len) {
            return error.IncompleteRecord;
        }

        const key_len = std.mem.readInt(i32, slice[pos + 12 ..][0..4], .little);

        if (key_len < 0) {
            return error.InvalidRecordSize;
        }

        const value_len_offset = 16 + @as(usize, @intCast(key_len));

        if (pos + value_len_offset + 4 > self.actual_size) {
            return error.IncompleteRecord;
        }

        if (pos + value_len_offset + 4 > slice.len) {
            return error.IncompleteRecord;
        }

        const value_len = std.mem.readInt(i32, slice[pos + value_len_offset ..][0..4], .little);

        if (value_len < 0) {
            return error.InvalidRecordSize;
        }

        const total_size = 16 + @as(usize, @intCast(key_len)) + 4 + @as(usize, @intCast(value_len));
        return total_size;
    }

    pub fn size(self: *const MmapLogReader) usize {
        return self.actual_size;
    }
};
