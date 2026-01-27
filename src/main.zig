const std = @import("std");
const Server = @import("server.zig").Server;
const ServerConfig = @import("server.zig").ServerConfig;
const Log = @import("log.zig").Log;
const AppConfig = @import("config_loader.zig").AppConfig;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var app_config = try AppConfig.parseArgs(allocator);
    defer app_config.deinit();

    // Try to open existing log, create new one only if it doesn't exist
    var log = Log.open(app_config.log_config, app_config.log_dir, allocator) catch |err| blk: {
        if (err == error.NoSegmentsFound or err == error.FileNotFound) {
            std.debug.print("No existing log found, creating new log at {s}\n", .{app_config.log_dir});
            break :blk try Log.create(app_config.log_config, app_config.log_dir, allocator);
        }
        return err;
    };
    defer log.close();

    std.debug.print("Log opened: next_offset={}, segment_count={}\n", .{ log.getNextOffset(), log.getSegmentCount() });

    const server_config = ServerConfig{
        .port = app_config.port,
    };

    // Initialize server
    var server = if (app_config.cluster_config) |*cluster_config| blk: {
        const node_id = app_config.node_id.?;
        std.debug.print("Starting in cluster mode: node_id={d}\n", .{node_id});
        break :blk try Server.initWithCluster(server_config, &log, cluster_config, node_id, allocator);
    } else blk: {
        std.debug.print("Starting in standalone mode\n", .{});
        break :blk Server.init(server_config, &log, allocator);
    };
    defer server.stop();

    std.debug.print("Zue server starting on port {d} with log directory {s}...\n", .{ app_config.port, app_config.log_dir });
    try server.start();
}

test {
    std.testing.refAllDecls(@import("log/main.zig"));
    std.testing.refAllDecls(@import("log/mmap_log_test.zig"));
    std.testing.refAllDecls(@import("network/protocol.zig"));
    std.testing.refAllDecls(@import("network/protocol_test.zig"));
    std.testing.refAllDecls(@import("replication/follower_tracker.zig"));
    std.testing.refAllDecls(@import("replication/follower.zig"));
    std.testing.refAllDecls(@import("replication/leader.zig"));
    std.testing.refAllDecls(@import("config.zig"));
    std.testing.refAllDecls(@import("event_loop.zig"));
    std.testing.refAllDecls(@import("server.zig"));
}