const std = @import("std");
const Server = @import("main.zig").Server;
const Protocol = @import("../network/protocol.zig");

pub fn processRequest(self: *Server, request: Protocol.Message, socket_handle: std.posix.socket_t) !void {
    switch (request) {
        .append_request => |req| try handleAppend(self, req, socket_handle),
        .read_request => |req| try handleRead(self, req, socket_handle),
        .replicate_request => |req| try handleReplicate(self, req, socket_handle),
        .heartbeat_request => |req| try handleHeartbeat(self, req, socket_handle),
        else => {
            std.debug.print("Unexpected message type from client\n", .{});
            try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Unexpected message type");
        },
    }
}

pub fn handleClientMessage(self: *Server, socket_handle: std.posix.socket_t, read_buffer: []u8) !void {
    const message_bytes = try Protocol.readCompleteMessage(socket_handle, read_buffer);

    var request_stream = std.io.fixedBufferStream(message_bytes);
    const request_reader = request_stream.reader();

    const request = Protocol.deserializeMessageBody(request_reader, self.allocator) catch |err| {
        std.debug.print("Error deserializing message: {}\n", .{err});
        try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Failed to deserialize message");
        return err;
    };
    defer freeMessage(self, request);

    try processRequest(self, request, socket_handle);
}

pub fn freeMessage(self: *Server, message: Protocol.Message) void {
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

fn handleAppend(self: *Server, req: Protocol.AppendRequest, socket_handle: std.posix.socket_t) !void {
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
}

fn handleRead(self: *Server, req: Protocol.ReadRequest, socket_handle: std.posix.socket_t) !void {
    std.debug.print("Processing read request (offset={})\n", .{req.offset});

    const record = self.log.read(req.offset, self.allocator) catch |err| {
        std.debug.print("Error reading from log: {}\n", .{err});

        const error_code: Protocol.ErrorCode = switch (err) {
            error.OffsetNotFound => .invalid_offset,
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
}

fn handleReplicate(self: *Server, req: Protocol.ReplicateRequest, socket_handle: std.posix.socket_t) !void {
    // Only followers should receive replication requests from leader
    if (self.role != .follower or self.follower == null) {
        std.debug.print("Received replicate_request but not a follower\n", .{});
        try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Not a follower node");
        return;
    }

    std.debug.print("[FOLLOWER] Received replication request (entries={}, leader_commit={})\n", .{
        req.entries.len,
        req.leader_commit,
    });

    const resp = try self.follower.?.handleReplicateRequest(req);

    // Send response back to leader
    var msg_buffer: [65536]u8 = undefined;
    var msg_stream = std.io.fixedBufferStream(&msg_buffer);
    try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
        .replicate_response = resp,
    }, self.allocator);
    _ = try std.posix.write(socket_handle, msg_stream.getWritten());
}

fn handleHeartbeat(self: *Server, req: Protocol.HeartbeatRequest, socket_handle: std.posix.socket_t) !void {
    // Only followers should receive heartbeats from leader
    if (self.role != .follower or self.follower == null) {
        std.debug.print("Received heartbeat_request but not a follower\n", .{});
        try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Not a follower node");
        return;
    }

    const resp = self.follower.?.handleHeartbeat(req);

    // Send response back to leader
    var msg_buffer: [65536]u8 = undefined;
    var msg_stream = std.io.fixedBufferStream(&msg_buffer);
    try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
        .heartbeat_response = resp,
    }, self.allocator);
    _ = try std.posix.write(socket_handle, msg_stream.getWritten());
}
