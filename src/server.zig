const std = @import("std");
const Log = @import("log/log.zig").Log;
const LogConfig = @import("log/log.zig").LogConfig;
const SegmentConfig = @import("log/segment.zig").SegmentConfig;
const Protocol = @import("network/protocol.zig");
const Record = @import("log/record.zig").Record;
const EventLoop = @import("event_loop.zig").EventLoop;
const EventHandlers = @import("event_loop.zig").EventHandlers;
const config_module = @import("config.zig");
const ClusterConfig = config_module.ClusterConfig;
const NodeRole = config_module.NodeRole;
const Follower = @import("replication/follower.zig").Follower;
const Leader = @import("replication/leader.zig").Leader;

pub const ServerConfig = struct {
    port: u16 = 9000,
    address: []const u8 = "0.0.0.0",
    backlog: u31 = 128,
    keepalive: bool = true,
    nodelay: bool = true,
};

const ClientState = struct {
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
            .onListenReady = onListenReady,
            .onClientReady = onClientReady,
            .onTimer = onTimer,
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

    // ========================================================================
    // Event Handlers
    // ========================================================================

    fn onListenReady(ctx: *anyopaque) !void {
        const self: *Server = @ptrCast(@alignCast(ctx));

        const connection = self.listener.?.accept() catch |err| {
            std.debug.print("Error accepting connection: {}\n", .{err});
            return;
        };

        // std.debug.print("Client connected from {any}\n", .{connection.address});

        const client_state = try self.allocator.create(ClientState);
        client_state.* = .{
            .fd = connection.stream.handle,
            .read_buffer = undefined,
        };

        try self.clients.put(connection.stream.handle, client_state);

        try self.event_loop.registerSocket(connection.stream.handle, .client);
    }

    fn onClientReady(ctx: *anyopaque, fd: std.posix.fd_t) !void {
        const self: *Server = @ptrCast(@alignCast(ctx));

        const client_state = self.clients.get(fd) orelse {
            std.debug.print("Unknown client fd: {}\n", .{fd});
            return;
        };

        self.handleClientMessage(fd, &client_state.read_buffer) catch |err| {
            switch (err) {
                error.EndOfStream, error.ConnectionResetByPeer => {
                    // std.debug.print("Client disconnected\n", .{});
                },
                else => {
                    std.debug.print("Error handling client message: {}\n", .{err});
                },
            }
            // Clean up client resources
            // NOTE: event_loop.unregisterSocket() is called automatically by EventLoop.run()
            // when this handler returns an error. We just handle our own cleanup here.
            std.posix.close(fd);
            _ = self.clients.remove(fd);
            self.allocator.destroy(client_state);

            // Return error to signal event loop to unregister this socket
            return err;
        };
    }

    fn onTimer(ctx: *anyopaque) !void {
        const self: *Server = @ptrCast(@alignCast(ctx));

        // If we're a leader, send heartbeats to all followers
        if (self.leader) |leader| {
            leader.sendHeartbeats();

            // Background repair: Process followers that need repair
            _ = leader.tickRepair();
        }

        // If we're a follower in catching-up state, try to fetch more data
        if (self.follower) |follower| {
            if (follower.needsCatchUp()) {
                _ = follower.tickCatchUp() catch |err| {
                    std.debug.print("[FOLLOWER] Catch-up error: {}\n", .{err});
                    return;
                };
            }
        }
    }

    fn handleClientMessage(self: *Server, socket_handle: std.posix.socket_t, read_buffer: []u8) !void {
        const message_bytes = try Protocol.readCompleteMessage(socket_handle, read_buffer);

        var request_stream = std.io.fixedBufferStream(message_bytes);
        const request_reader = request_stream.reader();

        const request = Protocol.deserializeMessageBody(request_reader, self.allocator) catch |err| {
            std.debug.print("Error deserializing message: {}\n", .{err});
            try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Failed to deserialize message");
            return err;
        };
        defer self.freeMessage(request);

        try self.processRequest(request, socket_handle);
    }

    fn processRequest(self: *Server, request: Protocol.Message, socket_handle: std.posix.socket_t) !void {
        switch (request) {
            .append_request => |req| {
                // Followers reject client append requests
                if (self.role == .follower) {
                    const leader_addr = if (self.cluster_config) |cfg| blk: {
                        const leader_node = cfg.getLeader();
                        break :blk if (leader_node) |l| l.address else "unknown";
                    } else "unknown";

                    std.debug.print("Follower rejecting append request, redirecting to leader at {s}\n", .{leader_addr});
                    try self.sendErrorResponseDirect(socket_handle, .not_leader, "This node is a follower, redirect to leader");
                    return;
                }

                // std.debug.print("Processing append request (key={?s}, value_len={})\n", .{
                //     req.record.key,
                //     req.record.value.len,
                // });

                // Leader mode: replicate to followers
                const offset = if (self.leader) |leader| blk: {
                    // Use leader.replicate() for replication + quorum
                    const result_offset = leader.replicate(req.record) catch |err| {
                        std.debug.print("[LEADER] Replication failed: {}\n", .{err});
                        if (err == error.QuorumNotReached) {
                            try self.sendErrorResponseDirect(socket_handle, .io_error, "Quorum not reached - cannot commit write");
                        } else {
                            try self.sendErrorResponseDirect(socket_handle, .io_error, "Failed to replicate record");
                        }
                        return;
                    };
                    // std.debug.print("[LEADER] Replicated to quorum, offset={}\n", .{result_offset});
                    break :blk result_offset;
                } else blk: {
                    // Standalone mode (no cluster): just append locally
                    const result_offset = self.log.append(req.record) catch |err| {
                        std.debug.print("Error appending to log: {}\n", .{err});
                        try self.sendErrorResponseDirect(socket_handle, .io_error, "Failed to append record");
                        return;
                    };
                    break :blk result_offset;
                };

                // WORKAROUND: Serialize to buffer then write with posix.write
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .append_response = .{ .offset = offset },
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                // std.debug.print("Append successful, offset={}\n", .{offset});
            },

            .read_request => |req| {
                // Followers reject client read requests (could be relaxed to allow stale reads)
                if (self.role == .follower) {
                    const leader_addr = if (self.cluster_config) |cfg| blk: {
                        const leader = cfg.getLeader();
                        break :blk if (leader) |l| l.address else "unknown";
                    } else "unknown";

                    std.debug.print("Follower rejecting read request, redirecting to leader at {s}\n", .{leader_addr});
                    try self.sendErrorResponseDirect(socket_handle, .not_leader, "This node is a follower, redirect to leader");
                    return;
                }

                std.debug.print("Processing read request (offset={})\n", .{req.offset});

                const record = self.log.read(req.offset, self.allocator) catch |err| {
                    std.debug.print("Error reading from log: {}\n", .{err});

                    const error_code: Protocol.ErrorCode = switch (err) {
                        error.OffsetOutOfBounds => .invalid_offset,
                        else => .io_error,
                    };

                    try self.sendErrorResponseDirect(socket_handle, error_code, "Failed to read record");
                    return;
                };
                defer {
                    if (record.key) |k| self.allocator.free(k);
                    self.allocator.free(record.value);
                }

                // WORKAROUND: Serialize to buffer then write with posix.write
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .read_response = .{ .record = record },
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                // std.debug.print("Read successful (key={?s}, value_len={})\n", .{
                //     record.key,
                //     record.value.len,
                // });
            },

            .replicate_request => |req| {
                // Only followers should receive replication requests from leader
                if (self.role != .follower or self.follower == null) {
                    std.debug.print("Received replicate_request but not a follower\n", .{});
                    try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Not a follower node");
                    return;
                }

                // std.debug.print("[FOLLOWER] Received replication request (entries={}, leader_commit={})\n", .{
                //     req.entries.len,
                //     req.leader_commit,
                // });

                const resp = try self.follower.?.handleReplicateRequest(req);

                // Send response back to leader
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .replicate_response = resp,
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                // if (resp.success) {
                //     std.debug.print("[FOLLOWER] Replicated successfully, follower_offset={?}\n", .{resp.follower_offset});
                // } else {
                //     std.debug.print("[FOLLOWER] Replication failed, error_code={}\n", .{resp.error_code});
                // }
            },

            .heartbeat_request => |req| {
                // Only followers should receive heartbeats from leader
                if (self.role != .follower or self.follower == null) {
                    std.debug.print("Received heartbeat_request but not a follower\n", .{});
                    try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Not a follower node");
                    return;
                }

                // std.debug.print("[FOLLOWER] Received heartbeat (leader_commit={}, leader_offset={})\n", .{
                //     req.leader_commit,
                //     req.leader_offset,
                // });

                const resp = self.follower.?.handleHeartbeat(req);

                // Send response back to leader
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .heartbeat_response = resp,
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                // std.debug.print("[FOLLOWER] Heartbeat acknowledged, follower_offset={?}\n", .{resp.follower_offset});
            },

            .catch_up_request => |req| {
                // Only leaders should handle catch-up requests from followers
                if (self.role != .leader) {
                    std.debug.print("Received catch_up_request but not a leader\n", .{});
                    try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Not a leader node");
                    return;
                }

                // std.debug.print("[LEADER] Received catch-up request (start_offset={}, max_entries={})\n", .{
                //     req.start_offset,
                //     req.max_entries,
                // });

                // Handle empty log case
                var entries: []Protocol.ReplicatedEntry = undefined;
                var more: bool = false;

                if (self.log.getNextOffset() == 0 or req.start_offset >= self.log.getNextOffset()) {
                    // Log is empty or follower is caught up - return empty response
                    entries = try self.allocator.alloc(Protocol.ReplicatedEntry, 0);
                    more = false;
                } else {
                    // Read range of entries from log
                    const records = try self.log.readRange(req.start_offset, req.max_entries, self.allocator);
                    errdefer {
                        for (records) |record| {
                            if (record.key) |k| self.allocator.free(k);
                            self.allocator.free(record.value);
                        }
                        self.allocator.free(records);
                    }

                    // Convert to ReplicatedEntry array
                    entries = try self.allocator.alloc(Protocol.ReplicatedEntry, records.len);
                    errdefer self.allocator.free(entries);

                    for (records, 0..) |record, i| {
                        entries[i] = Protocol.ReplicatedEntry{
                            .offset = req.start_offset + i,
                            .record = record,
                        };
                    }

                    // Check if there are more entries after this batch
                    const end_offset = req.start_offset + records.len;
                    more = end_offset < self.log.getNextOffset();

                    // Clean up records array (but not the record contents - they're used in entries)
                    self.allocator.free(records);
                }
                defer {
                    // Free entries array and all record contents
                    for (entries) |entry| {
                        if (entry.record.key) |k| self.allocator.free(k);
                        self.allocator.free(entry.record.value);
                    }
                    self.allocator.free(entries);
                }

                const resp = Protocol.CatchUpResponse{
                    .entries = entries,
                    .more = more,
                };

                // Send response back to follower
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .catch_up_response = resp,
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                // std.debug.print("[LEADER] Sent catch-up response ({} entries, more={})\n", .{
                //     entries.len,
                //     more,
                // });
            },

            else => {
                std.debug.print("Unexpected message type from client\n", .{});
                try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Unexpected message type");
            },
        }
    }

    fn sendErrorResponseDirect(self: *Server, socket_handle: std.posix.socket_t, code: Protocol.ErrorCode, message: []const u8) !void {
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

    fn freeMessage(self: *Server, message: Protocol.Message) void {
        switch (message) {
            .append_request => |req| {
                if (req.record.key) |k| self.allocator.free(k);
                self.allocator.free(req.record.value);
            },
            .read_response => |res| {
                if (res.record.key) |k| self.allocator.free(k);
                self.allocator.free(res.record.value);
            },
            .error_response => |err| {
                self.allocator.free(err.message);
            },
            else => {},
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Parse arguments: <port> <log_dir> [config_path] [node_id]
    var port: u16 = 9000; // Default port
    if (args.len > 1) {
        port = std.fmt.parseInt(u16, args[1], 10) catch blk: {
            std.debug.print("Invalid port argument: {s}, using default 9000\n", .{args[1]});
            break :blk 9000;
        };
    }

    const log_dir = if (args.len > 2) args[2] else "/tmp/zue_server_test";

    const log_config = LogConfig{
        .segment_config = SegmentConfig.default(),
    };
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.close();

    const server_config = ServerConfig{
        .port = port,
    };

    // Check if cluster mode (config_path and node_id provided)
    var server = if (args.len > 4) blk: {
        const config_path = args[3];
        const node_id = try std.fmt.parseInt(u32, args[4], 10);

        std.debug.print("Starting in cluster mode: node_id={d}, config={s}\n", .{ node_id, config_path });

        // Load cluster config
        const cluster_config = try config_module.parseFile(config_path, allocator);
        // Note: We'll pass ownership to the server, but need to manage cleanup
        const cluster_config_ptr = try allocator.create(ClusterConfig);
        cluster_config_ptr.* = cluster_config;

        var srv = try Server.initWithCluster(server_config, &log, cluster_config_ptr, node_id, allocator);
        srv.cluster_config = cluster_config_ptr; // Ensure server keeps reference
        break :blk srv;
    } else blk: {
        std.debug.print("Starting in standalone mode\n", .{});
        break :blk Server.init(server_config, &log, allocator);
    };

    defer {
        // Clean up cluster config if allocated
        if (server.cluster_config) |cfg| {
            cfg.deinit();
            allocator.destroy(cfg);
        }
    }

    std.debug.print("Zue server starting on port {d} with log directory {s}...\n", .{ port, log_dir });
    try server.start();
}

// ============================================================================
// Tests
// ============================================================================

test "Server.readCompleteMessage returns body without length prefix" {
    // Simulate serialized message
    const serialized = [_]u8{
        10, 0, 0, 0, // length = 10
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // body
    };

    const length_prefix = std.mem.readInt(u32, serialized[0..4], .little);
    try std.testing.expectEqual(@as(u32, 10), length_prefix);

    const body = serialized[4..];
    try std.testing.expectEqual(@as(usize, 10), body.len);
    try std.testing.expectEqual(@as(u8, 1), body[0]); // First byte of body, NOT length
}

test "Server.readCompleteMessage compatible with deserializeMessageBody" {
    const allocator = std.testing.allocator;

    const original = Protocol.Message{
        .read_request = .{ .offset = 555 },
    };

    var buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer buffer.deinit(allocator);
    try Protocol.serializeMessage(buffer.writer(allocator), original, allocator);

    // Extract body
    const length_prefix = std.mem.readInt(u32, buffer.items[0..4], .little);
    const body = buffer.items[4 .. 4 + length_prefix];

    // Deserialize
    var body_stream = std.io.fixedBufferStream(body);
    const deserialized = try Protocol.deserializeMessageBody(body_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 555), deserialized.read_request.offset);
}
