const std = @import("std");
const config = @import("config.zig");
const LogConfig = @import("log/log_config.zig").LogConfig;
const SegmentConfig = @import("log/segment.zig").SegmentConfig;

pub const AppConfig = struct {
    port: u16,
    log_dir: []const u8,
    cluster_config: ?config.ClusterConfig,
    node_id: ?u32,
    log_config: LogConfig,
    allocator: std.mem.Allocator,

    pub fn parseArgs(allocator: std.mem.Allocator) !AppConfig {
        const args = try std.process.argsAlloc(allocator);
        defer std.process.argsFree(allocator, args);

        var port: u16 = 9000;
        if (args.len > 1) {
            port = std.fmt.parseInt(u16, args[1], 10) catch 9000;
        }

        const log_dir = if (args.len > 2) try allocator.dupe(u8, args[2]) else try allocator.dupe(u8, "/tmp/zue_server_test");
        errdefer allocator.free(log_dir);

        var cluster_config: ?config.ClusterConfig = null;
        var node_id: ?u32 = null;

        if (args.len > 4) {
            const config_path = args[3];
            node_id = try std.fmt.parseInt(u32, args[4], 10);
            cluster_config = try config.parseFile(config_path, allocator);
        }

        return AppConfig{
            .port = port,
            .log_dir = log_dir,
            .cluster_config = cluster_config,
            .node_id = node_id,
            .log_config = LogConfig{
                .segment_config = SegmentConfig.default(),
            },
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *AppConfig) void {
        self.allocator.free(self.log_dir);
        if (self.cluster_config) |*cfg| {
            cfg.deinit();
        }
    }
};
