const std = @import("std");
const protocol = @import("../../network/protocol.zig");
const Allocator = std.mem.Allocator;

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;

/// Follower state machine for async repair process
pub const FollowerState = enum {
    replicating, // Normal replication, follower is in-sync or catching up
    repairing_stream_sent, // Sent batch of catch-up entries, awaiting response
};

/// State for a single follower connection
pub const FollowerConnection = struct {
    id: u32,
    address: []const u8,
    port: u16,
    stream: ?std.net.Stream,
    last_offset: u64, // Follower's last known offset (DEPRECATED - use match_index)
    last_heartbeat_ms: i64,
    in_sync: bool, // Is follower within acceptable lag?
    state: FollowerState, // Current state in repair state machine
    allocator: Allocator,

    // Raft-like replication state
    match_index: ?u64, // Highest offset confirmed replicated on this follower (null = no data yet)
    next_index: u64, // Next offset to send to this follower

    pub fn init(
        allocator: Allocator,
        id: u32,
        address: []const u8,
        port: u16,
        initial_offset: ?u64,
    ) !FollowerConnection {
        const stored_address = try allocator.dupe(u8, address);

        // For Raft-like replication:
        // - match_index: Highest offset we KNOW is replicated (null for empty log)
        // - next_index: Next offset to send (0 for empty, initial_offset + 1 otherwise)
        const match_idx = initial_offset;
        const next_idx = if (initial_offset) |offset| offset + 1 else 0;

        return FollowerConnection{
            .id = id,
            .address = stored_address,
            .port = port,
            .stream = null,
            .last_offset = initial_offset orelse 0, // DEPRECATED - kept for compatibility
            .last_heartbeat_ms = std.time.milliTimestamp(),
            .in_sync = true, // Start optimistic
            .state = .replicating, // Start in normal replication state
            .allocator = allocator,
            .match_index = match_idx,
            .next_index = next_idx,
        };
    }

    pub fn deinit(self: *FollowerConnection) void {
        self.allocator.free(self.address);
        if (self.stream) |stream| {
            stream.close();
        }
    }

    /// Connect to follower
    pub fn connect(self: *FollowerConnection) !void {
        if (self.stream != null) return; // Already connected

        const address = try std.net.Address.parseIp(self.address, self.port);
        const stream = try std.net.tcpConnectToAddress(address);

        // Set receive timeout to 5 seconds to prevent blocking forever
        const timeout = std.c.timeval{
            .sec = 5,
            .usec = 0,
        };
        try std.posix.setsockopt(
            stream.handle,
            std.posix.SOL.SOCKET,
            std.posix.SO.RCVTIMEO,
            std.mem.asBytes(&timeout),
        );

        self.stream = stream;
    }

    /// Disconnect from follower
    pub fn disconnect(self: *FollowerConnection) void {
        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }
    }

    /// Send replicate request to follower
    pub fn sendReplicateRequest(
        self: *FollowerConnection,
        req: ReplicateRequest,
        allocator: Allocator,
    ) !ReplicateResponse {
        if (self.stream == null) {
            return error.NotConnected;
        }

        // Serialize request
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .replicate_request = req,
        };

        try protocol.serializeMessage(msg_writer, request, allocator);
        const msg_bytes = msg_stream.getWritten();

        // Write request to socket
        _ = try std.posix.write(self.stream.?.handle, msg_bytes);

        // Read response
        var response_buffer: [65536]u8 = undefined;
        const message_bytes = try protocol.readCompleteMessage(self.stream.?.handle, &response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, allocator);

        return switch (response) {
            .replicate_response => |resp| resp,
            else => error.UnexpectedResponse,
        };
    }

    /// Send heartbeat to follower
    pub fn sendHeartbeat(
        self: *FollowerConnection,
        req: HeartbeatRequest,
        allocator: Allocator,
    ) !HeartbeatResponse {
        if (self.stream == null) {
            return error.NotConnected;
        }

        // Serialize request
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .heartbeat_request = req,
        };

        try protocol.serializeMessage(msg_writer, request, allocator);
        const msg_bytes = msg_stream.getWritten();

        // Write request to socket
        _ = try std.posix.write(self.stream.?.handle, msg_bytes);

        // Read response
        var response_buffer: [65536]u8 = undefined;
        const message_bytes = try protocol.readCompleteMessage(self.stream.?.handle, &response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, allocator);

        return switch (response) {
            .heartbeat_response => |resp| resp,
            else => error.UnexpectedResponse,
        };
    }

    /// Send replication request without waiting for response (non-blocking)
    pub fn sendReplicateRequestNonBlocking(
        self: *FollowerConnection,
        req: ReplicateRequest,
        allocator: Allocator,
    ) !void {
        if (self.stream == null) {
            return error.NotConnected;
        }

        // Serialize request
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .replicate_request = req,
        };

        try protocol.serializeMessage(msg_writer, request, allocator);
        const msg_bytes = msg_stream.getWritten();

        // Non-blocking write
        _ = try std.posix.write(self.stream.?.handle, msg_bytes);
    }

    /// Receive replication response (blocking read, but only when data is ready)
    pub fn receiveReplicateResponse(
        self: *FollowerConnection,
        allocator: Allocator,
    ) !ReplicateResponse {
        if (self.stream == null) {
            return error.NotConnected;
        }

        var response_buffer: [65536]u8 = undefined;
        const message_bytes = try protocol.readCompleteMessage(self.stream.?.handle, &response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, allocator);

        return switch (response) {
            .replicate_response => |resp| resp,
            else => error.UnexpectedResponse,
        };
    }
};
