const std = @import("std");
const log = @import("log/log.zig");

test {
    std.testing.refAllDecls(log);
}
