const std = @import("std");
const transport = @import("transport.zig");

test "readCompleteMessage reads message correctly" {
    const allocator = std.testing.allocator;

    var fds: [2]std.posix.fd_t = undefined;
    try std.posix.pipe(&fds);
    defer {
        std.posix.close(fds[0]);
        std.posix.close(fds[1]);
    }

    const read_fd = fds[0];
    const write_fd = fds[1];

    // Create a message: [length=5][hello]
    const message = "hello";
    const length: u32 = @intCast(message.len);
    
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, length, .little);

    // Write length prefix
    _ = try std.posix.write(write_fd, &len_bytes);
    // Write body
    _ = try std.posix.write(write_fd, message);

    var buffer: [1024]u8 = undefined;
    const body = try transport.readCompleteMessage(read_fd, &buffer);

    try std.testing.expectEqualStrings(message, body);
}

test "readCompleteMessage handles partial writes" {
    const allocator = std.testing.allocator;

    var fds: [2]std.posix.fd_t = undefined;
    try std.posix.pipe(&fds);
    defer {
        std.posix.close(fds[0]);
        std.posix.close(fds[1]);
    }

    const read_fd = fds[0];
    const write_fd = fds[1];

    const message = "partial";
    const length: u32 = @intCast(message.len);
    
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, length, .little);

    // Spawn thread to write slowly
    const ThreadContext = struct {
        fd: std.posix.fd_t,
        len_bytes: [4]u8,
        message: []const u8,
    };

    const writer_thread = struct {
        fn run(ctx: ThreadContext) void {
            std.time.sleep(10 * std.time.ns_per_ms);
            _ = std.posix.write(ctx.fd, ctx.len_bytes[0..2]) catch {};
            std.time.sleep(10 * std.time.ns_per_ms);
            _ = std.posix.write(ctx.fd, ctx.len_bytes[2..4]) catch {};
            std.time.sleep(10 * std.time.ns_per_ms);
            _ = std.posix.write(ctx.fd, ctx.message[0..3]) catch {};
            std.time.sleep(10 * std.time.ns_per_ms);
            _ = std.posix.write(ctx.fd, ctx.message[3..]) catch {};
        }
    };

    const ctx = ThreadContext{ .fd = write_fd, .len_bytes = len_bytes, .message = message };
    const thread = try std.Thread.spawn(.{}, writer_thread.run, .{ctx});
    defer thread.join();

    var buffer: [1024]u8 = undefined;
    const body = try transport.readCompleteMessage(read_fd, &buffer);

    try std.testing.expectEqualStrings(message, body);
}
