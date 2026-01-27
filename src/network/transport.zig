const std = @import("std");
const serialization = @import("serialization.zig");

/// Read a complete message from a socket, handling partial reads
/// Returns a slice into the provided buffer containing the message body (without length prefix)
pub fn readCompleteMessage(socket_handle: std.posix.socket_t, buffer: []u8) ![]u8 {
    var len_bytes: [4]u8 = undefined;
    var total_read: usize = 0;
    while (total_read < 4) {
        const n = try std.posix.read(socket_handle, len_bytes[total_read..]);
        if (n == 0) return error.EndOfStream;
        total_read += n;
    }

    const message_len = std.mem.readInt(u32, &len_bytes, .little);

    if (message_len > buffer.len) return error.MessageTooLarge;
    if (message_len > serialization.MAX_MESSAGE_SIZE) return serialization.ProtocolError.MessageTooLarge;

    total_read = 0;
    while (total_read < message_len) {
        const n = try std.posix.read(socket_handle, buffer[total_read..message_len]);
        if (n == 0) return error.UnexpectedEndOfStream;
        total_read += n;
    }

    return buffer[0..message_len];
}
