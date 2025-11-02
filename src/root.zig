const std = @import("std");
const log = @import("log/log.zig");
const protocol = @import("network/protocol.zig");

test {
    std.testing.refAllDecls(log);
    std.testing.refAllDecls(protocol);
}
