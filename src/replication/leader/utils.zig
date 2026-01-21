const std = @import("std");

/// Helper: Check if a socket has data ready to read (non-blocking)
pub fn isSocketReady(stream: std.net.Stream, timeout_ms: i32) bool {
    var poll_fds = [_]std.posix.pollfd{.{
        .fd = stream.handle,
        .events = std.posix.POLL.IN,
        .revents = 0,
    }};

    const ready = std.posix.poll(&poll_fds, timeout_ms) catch return false;
    return ready > 0 and (poll_fds[0].revents & std.posix.POLL.IN) != 0;
}
