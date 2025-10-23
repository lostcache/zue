const std = @import("std");
const record = @import("record.zig");
const log_index = @import("log_index.zig");
const segment = @import("segment.zig");

test {
    std.testing.refAllDecls(record);
    std.testing.refAllDecls(log_index);
    std.testing.refAllDecls(segment);
}
