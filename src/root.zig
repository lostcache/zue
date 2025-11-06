const std = @import("std");
const log = @import("log/log.zig");
const protocol = @import("network/protocol.zig");
const uncommitted = @import("replication/uncommitted.zig");
const follower_tracker = @import("replication/follower_tracker.zig");
const follower = @import("replication/follower.zig");
const leader = @import("replication/leader.zig");
const config = @import("config.zig");
const event_loop = @import("event_loop.zig");

test {
    std.testing.refAllDecls(log);
    std.testing.refAllDecls(protocol);
    std.testing.refAllDecls(uncommitted);
    std.testing.refAllDecls(follower_tracker);
    std.testing.refAllDecls(follower);
    std.testing.refAllDecls(leader);
    std.testing.refAllDecls(config);
    std.testing.refAllDecls(event_loop);
}
