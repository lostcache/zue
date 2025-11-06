const std = @import("std");

pub const EventType = enum {
    listener,
    client,
};

pub const SocketInfo = struct {
    fd: std.posix.fd_t,
    event_type: EventType,
};

pub const EventHandlers = struct {
    onListenReady: *const fn (ctx: *anyopaque) anyerror!void,
    onClientReady: *const fn (ctx: *anyopaque, fd: std.posix.fd_t) anyerror!void,
    onTimer: *const fn (ctx: *anyopaque) anyerror!void,
    context: *anyopaque,
};

pub const EventLoop = struct {
    poll_fds: std.ArrayList(std.posix.pollfd),
    socket_info: std.ArrayList(SocketInfo),
    allocator: std.mem.Allocator,
    last_timer_ms: i64,
    timer_interval_ms: i64,

    pub fn init(allocator: std.mem.Allocator, timer_interval_ms: i64) EventLoop {
        return EventLoop{
            .poll_fds = .{},
            .socket_info = .{},
            .allocator = allocator,
            .last_timer_ms = std.time.milliTimestamp(),
            .timer_interval_ms = timer_interval_ms,
        };
    }

    pub fn deinit(self: *EventLoop) void {
        self.poll_fds.deinit(self.allocator);
        self.socket_info.deinit(self.allocator);
    }

    pub fn registerSocket(self: *EventLoop, fd: std.posix.fd_t, event_type: EventType) !void {
        try self.poll_fds.append(self.allocator, .{
            .fd = fd,
            .events = std.posix.POLL.IN,
            .revents = 0,
        });

        try self.socket_info.append(self.allocator, .{
            .fd = fd,
            .event_type = event_type,
        });
    }

    pub fn unregisterSocket(self: *EventLoop, fd: std.posix.fd_t) void {
        var i: usize = 0;
        while (i < self.poll_fds.items.len) : (i += 1) {
            if (self.poll_fds.items[i].fd == fd) {
                _ = self.poll_fds.orderedRemove(i);
                _ = self.socket_info.orderedRemove(i);
                return;
            }
        }
    }

    pub fn run(self: *EventLoop, handlers: EventHandlers, timeout_ms: i32) !void {
        const ready = try std.posix.poll(self.poll_fds.items, timeout_ms);

        const now = std.time.milliTimestamp();
        if (now - self.last_timer_ms >= self.timer_interval_ms) {
            try handlers.onTimer(handlers.context);
            self.last_timer_ms = now;
        }

        if (ready == 0) {
            return;
        }

        var fds_to_remove: std.ArrayList(std.posix.fd_t) = .{};
        defer fds_to_remove.deinit(self.allocator);

        for (self.poll_fds.items, self.socket_info.items) |*pfd, info| {
            if (pfd.revents & std.posix.POLL.IN != 0) {
                switch (info.event_type) {
                    .listener => try handlers.onListenReady(handlers.context),
                    .client => {
                        // If handler errors, mark this FD for removal
                        handlers.onClientReady(handlers.context, info.fd) catch {
                            try fds_to_remove.append(self.allocator, info.fd);
                        };
                    },
                }
                pfd.revents = 0;
            }

            if (pfd.revents & std.posix.POLL.HUP != 0 or pfd.revents & std.posix.POLL.ERR != 0) {
                try fds_to_remove.append(self.allocator, info.fd);
                pfd.revents = 0;
            }
        }

        for (fds_to_remove.items) |fd| {
            self.unregisterSocket(fd);
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "EventLoop.init creates empty event loop" {
    const allocator = std.testing.allocator;
    var loop = EventLoop.init(allocator, 2000);
    defer loop.deinit();

    try std.testing.expectEqual(@as(usize, 0), loop.poll_fds.items.len);
    try std.testing.expectEqual(@as(usize, 0), loop.socket_info.items.len);
    try std.testing.expectEqual(@as(i64, 2000), loop.timer_interval_ms);
}

test "EventLoop.registerSocket adds socket to poll list" {
    const allocator = std.testing.allocator;
    var loop = EventLoop.init(allocator, 2000);
    defer loop.deinit();

    try loop.registerSocket(42, .listener);
    try std.testing.expectEqual(@as(usize, 1), loop.poll_fds.items.len);
    try std.testing.expectEqual(@as(std.posix.fd_t, 42), loop.poll_fds.items[0].fd);
    try std.testing.expectEqual(std.posix.POLL.IN, loop.poll_fds.items[0].events);
    try std.testing.expectEqual(EventType.listener, loop.socket_info.items[0].event_type);
}

test "EventLoop.registerSocket can add multiple sockets" {
    const allocator = std.testing.allocator;
    var loop = EventLoop.init(allocator, 2000);
    defer loop.deinit();

    try loop.registerSocket(10, .listener);
    try loop.registerSocket(20, .client);
    try loop.registerSocket(30, .client);

    try std.testing.expectEqual(@as(usize, 3), loop.poll_fds.items.len);
    try std.testing.expectEqual(@as(std.posix.fd_t, 10), loop.poll_fds.items[0].fd);
    try std.testing.expectEqual(@as(std.posix.fd_t, 20), loop.poll_fds.items[1].fd);
    try std.testing.expectEqual(@as(std.posix.fd_t, 30), loop.poll_fds.items[2].fd);
}

test "EventLoop.unregisterSocket removes socket from poll list" {
    const allocator = std.testing.allocator;
    var loop = EventLoop.init(allocator, 2000);
    defer loop.deinit();

    try loop.registerSocket(10, .listener);
    try loop.registerSocket(20, .client);
    try loop.registerSocket(30, .client);

    loop.unregisterSocket(20);

    try std.testing.expectEqual(@as(usize, 2), loop.poll_fds.items.len);
    try std.testing.expectEqual(@as(std.posix.fd_t, 10), loop.poll_fds.items[0].fd);
    try std.testing.expectEqual(@as(std.posix.fd_t, 30), loop.poll_fds.items[1].fd);
}

test "EventLoop.unregisterSocket handles non-existent socket gracefully" {
    const allocator = std.testing.allocator;
    var loop = EventLoop.init(allocator, 2000);
    defer loop.deinit();

    try loop.registerSocket(10, .listener);
    loop.unregisterSocket(999);  // Non-existent fd

    try std.testing.expectEqual(@as(usize, 1), loop.poll_fds.items.len);
    try std.testing.expectEqual(@as(std.posix.fd_t, 10), loop.poll_fds.items[0].fd);
}
