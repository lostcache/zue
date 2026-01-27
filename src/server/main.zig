const std = @import("std");
const Log = @import("../log/log.zig").Log;
const LogConfig = @import("../log/log_config.zig").LogConfig;
const SegmentConfig = @import("../log/segment.zig").SegmentConfig;
const Protocol = @import("../network/protocol.zig");
const Record = @import("../log/record.zig").Record;
const EventLoop = @import("../event_loop.zig").EventLoop;
const EventHandlers = @import("../event_loop.zig").EventHandlers;
const config_module = @import("../config.zig");
const ClusterConfig = config_module.ClusterConfig;
const NodeRole = config_module.NodeRole;
const Follower = @import("../replication/follower.zig").Follower;
const Leader = @import("../replication/leader.zig").Leader;
const request_handlers = @import("request_handlers.zig");
const event_handlers = @import("event_handlers.zig");

pub const ServerConfig = struct {
    port: u16 = 9000,
    address: []const u8 = "0.0.0.0",
    backlog: u31 = 128,
    keepalive: bool = true,
    nodelay: bool = true,
};

pub const ClientState = struct {
    fd: std.posix.fd_t,
    read_buffer: [65536]u8,
};

pub const Server = struct {
    config: ServerConfig,
    log: *Log,
    allocator: std.mem.Allocator,
    listener: ?std.net.Server,
    running: bool,
    event_loop: EventLoop,
    clients: std.AutoHashMap(std.posix.fd_t, *ClientState),

    // Replication fields
    cluster_config: ?*ClusterConfig,
    role: NodeRole,
    follower: ?*Follower,
    leader: ?*Leader,

    pub fn init(config: ServerConfig, log: *Log, allocator: std.mem.Allocator) Server {
        return Server{
            .config = config,
            .log = log,
            .allocator = allocator,
            .listener = null,
            .running = false,
            .event_loop = EventLoop.init(allocator, 2000),
            .clients = std.AutoHashMap(std.posix.fd_t, *ClientState).init(allocator),
            .cluster_config = null,
            .role = .leader, // Default to leader for standalone mode
            .follower = null,
            .leader = null,
        };
    }

    /// Initialize server with cluster configuration
    pub fn initWithCluster(
        config: ServerConfig,
        log: *Log,
        cluster_config: *ClusterConfig,
        node_id: u32,
        allocator: std.mem.Allocator,
    ) !Server {
        var server = init(config, log, allocator);
        server.cluster_config = cluster_config;

        // Find this node's role in the cluster
        for (cluster_config.nodes) |node| {
            if (node.id == node_id) {
                server.role = node.role;

                // If follower, initialize Follower struct
                if (node.role == .follower) {
                    const leader_node = cluster_config.getLeader() orelse return error.NoLeaderConfigured;
                    const follower = try allocator.create(Follower);
                    follower.* = try Follower.init(log, leader_node.address, leader_node.port, allocator);
                    server.follower = follower;
                }

                // If leader, initialize Leader struct
                if (node.role == .leader) {
                    const leader = try allocator.create(Leader);
                    leader.* = try Leader.init(allocator, log, cluster_config.*);
                    server.leader = leader;
                }

                break;
            }
        }

        return server;
    }

    pub fn start(self: *Server) !void {
        const address = try std.net.Address.parseIp(self.config.address, self.config.port);

        self.listener = try address.listen(.{
            .reuse_address = true,
            .kernel_backlog = self.config.backlog,
        });

        self.running = true;

        std.debug.print("Zue server listening on {s}:{d}\n", .{ self.config.address, self.config.port });

        // If leader, connect to all followers
        if (self.leader) |leader| {
            std.debug.print("[LEADER] Connecting to followers...\n", .{});
            leader.connectToFollowers();
            std.debug.print("[LEADER] Connected to {d} followers\n", .{leader.followers.len});
        }

        try self.event_loop.registerSocket(self.listener.?.stream.handle, .listener);

        const handlers = EventHandlers{
            .onListenReady = event_handlers.onListenReady,
            .onClientReady = event_handlers.onClientReady,
            .onTimer = event_handlers.onTimer,
            .context = self,
        };

        while (self.running) {
            try self.event_loop.run(handlers, 100);
        }
    }

    pub fn stop(self: *Server) void {
        self.running = false;

        var it = self.clients.iterator();
        while (it.next()) |entry| {
            std.posix.close(entry.key_ptr.*);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.clients.deinit();

        self.event_loop.deinit();

        if (self.listener) |*listener| {
            listener.deinit();
            self.listener = null;
        }

        // Clean up follower if present
        if (self.follower) |follower| {
            follower.deinit();
            self.allocator.destroy(follower);
        }

        // Clean up leader if present
        if (self.leader) |leader| {
            leader.deinit();
            self.allocator.destroy(leader);
        }
    }

    pub fn sendErrorResponseDirect(self: *Server, socket_handle: std.posix.socket_t, code: Protocol.ErrorCode, message: []const u8) !void {
        const response = Protocol.Message{
            .error_response = Protocol.ErrorResponse{
                .code = code,
                .message = message,
            },
        };
        // WORKAROUND: Serialize to buffer then write with posix.write
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        try Protocol.serializeMessage(msg_stream.writer(), response, self.allocator);
        _ = try std.posix.write(socket_handle, msg_stream.getWritten());
    }
};
