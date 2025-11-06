const std = @import("std");
const Log = @import("../log/log.zig").Log;
const Record = @import("../log/record.zig").Record;
const protocol = @import("../network/protocol.zig");

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;
const CatchUpRequest = protocol.CatchUpRequest;
const CatchUpResponse = protocol.CatchUpResponse;
const ErrorCode = protocol.ErrorCode;

const Allocator = std.mem.Allocator;

/// Follower states for gap recovery
pub const FollowerState = enum {
    normal, // In sync, accepting replication
    catching_up, // Detected gap, requesting batch catch-up
};

pub const Follower = struct {
    log: *Log,
    allocator: Allocator,
    leader_address: []const u8,
    leader_port: u16,
    leader_stream: ?std.net.Stream, // Connection to leader for catch-up
    last_heartbeat_ms: i64,
    commit_index: u64, // Learned from leader
    state: FollowerState, // State machine for recovery

    /// Initialize follower with leader connection info
    pub fn init(
        log: *Log,
        leader_address: []const u8,
        leader_port: u16,
        allocator: Allocator,
    ) !Follower {
        const stored_address = try allocator.dupe(u8, leader_address);

        return Follower{
            .log = log,
            .allocator = allocator,
            .leader_address = stored_address,
            .leader_port = leader_port,
            .leader_stream = null,
            .last_heartbeat_ms = std.time.milliTimestamp(),
            .commit_index = 0,
            .state = .normal,
        };
    }

    /// Clean up resources
    pub fn deinit(self: *Follower) void {
        self.allocator.free(self.leader_address);
        if (self.leader_stream) |stream| {
            stream.close();
        }
    }

    /// Handle replication request from leader
    /// CRITICAL: Validates offset to detect gaps
    pub fn handleReplicateRequest(
        self: *Follower,
        req: ReplicateRequest,
    ) !ReplicateResponse {
        // Handle batch of entries (streaming replication)
        var last_offset: ?u64 = if (self.log.next_offset > 0) self.log.next_offset - 1 else null;

        for (req.entries) |entry| {
            // CRITICAL: Validate entry.offset == self.log.next_offset
            const expected_offset = self.log.next_offset;

            if (entry.offset != expected_offset) {
                // GAP DETECTED: Enter catching-up state
                self.state = .catching_up;

                // Return error to leader so it knows we're out of sync
                return ReplicateResponse{
                    .success = false,
                    .follower_offset = last_offset,
                    .error_code = .offset_mismatch,
                };
            }

            // Normal path: append and track offset
            last_offset = try self.log.append(entry.record);
        }

        // Update commit index from leader
        self.commit_index = req.leader_commit;

        return ReplicateResponse{
            .success = true,
            .follower_offset = last_offset,
            .error_code = @enumFromInt(0), // .none - using raw value for compatibility
        };
    }

    /// Handle heartbeat from leader
    pub fn handleHeartbeat(
        self: *Follower,
        req: HeartbeatRequest,
    ) HeartbeatResponse {
        self.last_heartbeat_ms = std.time.milliTimestamp();
        self.commit_index = req.leader_commit; // Advance commit index

        // If follower is behind leader, enter catching_up state
        // req.leader_offset is now the leader's next_offset (not last offset)
        // This makes comparison unambiguous: follower is behind if next_offset < leader_next_offset
        // Empty logs: 0 < 0 = false (both in sync)
        // Follower behind: 0 < 1 = true (needs catch-up)
        // Caught up: 1 < 1 = false (in sync)
        if (self.log.next_offset < req.leader_offset) {
            // std.debug.print("[FOLLOWER] Behind leader (leader_next={}, follower_next={}), entering catch-up mode\n", .{
            //     req.leader_offset,
            //     self.log.next_offset,
            // });
            self.state = .catching_up;
        }

        return HeartbeatResponse{
            .follower_offset = if (self.log.next_offset > 0) self.log.next_offset - 1 else null,
        };
    }

    /// INCREMENTAL CATCH-UP: Fetch ONE batch per call (non-blocking)
    /// Called repeatedly by server main loop until caught up
    /// Returns true if more batches remain
    pub fn tickCatchUp(self: *Follower) !bool {
        if (self.state != .catching_up) return false;

        // Ensure we have a connection to the leader
        if (self.leader_stream == null) {
            try self.connectToLeader();
        }

        // Fetch ONE batch
        const req = CatchUpRequest{
            .start_offset = self.log.next_offset,
            .max_entries = 1000,
        };

        const resp = try self.sendCatchUpRequest(req);
        defer self.freeCatchUpResponse(resp);

        // Apply batch
        for (resp.entries) |entry| {
            _ = try self.log.append(entry.record);
        }

        // Check if done
        if (!resp.more) {
            self.state = .normal; // Caught up!
            self.disconnectFromLeader();
            return false;
        }

        return true; // More batches remain
    }

    /// Check if follower is healthy (received recent heartbeat)
    pub fn isHealthy(self: *const Follower, timeout_ms: u64) bool {
        const now_ms = std.time.milliTimestamp();
        const elapsed: u64 = @intCast(now_ms - self.last_heartbeat_ms);
        return elapsed < timeout_ms;
    }

    /// Returns true if follower needs catch-up
    pub fn needsCatchUp(self: *const Follower) bool {
        return self.state == .catching_up;
    }

    // ========================================================================
    // Private helper methods for catch-up
    // ========================================================================

    fn connectToLeader(self: *Follower) !void {
        const address = try std.net.Address.parseIp(self.leader_address, self.leader_port);
        const stream = try std.net.tcpConnectToAddress(address);
        self.leader_stream = stream;
    }

    fn disconnectFromLeader(self: *Follower) void {
        if (self.leader_stream) |stream| {
            stream.close();
            self.leader_stream = null;
        }
    }

    fn sendCatchUpRequest(self: *Follower, req: CatchUpRequest) !CatchUpResponse {
        if (self.leader_stream == null) {
            return error.NotConnected;
        }

        // Serialize request
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .catch_up_request = req,
        };

        try protocol.serializeMessage(msg_writer, request, self.allocator);
        const msg_bytes = msg_stream.getWritten();

        // Write request to socket
        _ = try std.posix.write(self.leader_stream.?.handle, msg_bytes);

        // Read response
        var response_buffer: [1048576]u8 = undefined; // 1MB buffer for batch responses
        const message_bytes = try protocol.readCompleteMessage(self.leader_stream.?.handle, &response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, self.allocator);

        return switch (response) {
            .catch_up_response => |res| res,
            .error_response => error.ServerError,
            else => error.UnexpectedResponse,
        };
    }

    fn freeCatchUpResponse(self: *Follower, resp: CatchUpResponse) void {
        for (resp.entries) |entry| {
            if (entry.record.key) |k| self.allocator.free(k);
            self.allocator.free(entry.record.value);
        }
        self.allocator.free(resp.entries);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "Follower: init and deinit" {
    const allocator = std.testing.allocator;

    const log_config = @import("../log/log_config.zig").LogConfig.default();

    const test_dir = "test_follower_init";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer allocator.free(log_path);

    var log = try Log.create(log_config, log_path, allocator);
    defer log.delete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    try std.testing.expectEqual(FollowerState.normal, follower.state);
    try std.testing.expectEqual(@as(u64, 0), follower.commit_index);
    try std.testing.expectEqualStrings("127.0.0.1", follower.leader_address);
}

test "Follower: handleReplicateRequest - normal append" {
    const allocator = std.testing.allocator;

    const log_config = @import("../log/log_config.zig").LogConfig.default();

    const test_dir = "test_follower_replicate";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer allocator.free(log_path);

    var log = try Log.create(log_config, log_path, allocator);
    defer log.delete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    // Append first record
    const record1 = Record{
        .key = null,
        .value = "test value 1",
    };

    var entries1 = try allocator.alloc(protocol.ReplicatedEntry, 1);
    defer allocator.free(entries1);
    entries1[0] = protocol.ReplicatedEntry{ .offset = 0, .record = record1 };

    const req1 = ReplicateRequest{
        .entries = entries1,
        .leader_commit = 0,
    };

    const resp1 = try follower.handleReplicateRequest(req1);

    try std.testing.expect(resp1.success);
    try std.testing.expectEqual(@as(u64, 0), resp1.follower_offset);

    // Append second record
    const record2 = Record{
        .key = null,
        .value = "test value 2",
    };

    var entries2 = try allocator.alloc(protocol.ReplicatedEntry, 1);
    defer allocator.free(entries2);
    entries2[0] = protocol.ReplicatedEntry{ .offset = 1, .record = record2 };

    const req2 = ReplicateRequest{
        .entries = entries2,
        .leader_commit = 1,
    };

    const resp2 = try follower.handleReplicateRequest(req2);

    try std.testing.expect(resp2.success);
    try std.testing.expectEqual(@as(u64, 1), resp2.follower_offset);
    try std.testing.expectEqual(@as(u64, 1), follower.commit_index);
}

test "Follower: handleReplicateRequest - gap detection" {
    const allocator = std.testing.allocator;

    const log_config = @import("../log/log_config.zig").LogConfig.default();

    const test_dir = "test_follower_gap";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer allocator.free(log_path);

    var log = try Log.create(log_config, log_path, allocator);
    defer log.delete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    // Follower is at offset 0, leader sends offset 5 (gap!)
    const record = Record{
        .key = null,
        .value = "test value",
    };

    var entries = try allocator.alloc(protocol.ReplicatedEntry, 1);
    defer allocator.free(entries);
    entries[0] = protocol.ReplicatedEntry{ .offset = 5, .record = record };

    const req = ReplicateRequest{
        .entries = entries,
        .leader_commit = 5,
    };

    const resp = try follower.handleReplicateRequest(req);

    try std.testing.expect(!resp.success);
    try std.testing.expectEqual(ErrorCode.offset_mismatch, resp.error_code);
    try std.testing.expectEqual(FollowerState.catching_up, follower.state);
}

test "Follower: handleHeartbeat" {
    const allocator = std.testing.allocator;

    const log_config = @import("../log/log_config.zig").LogConfig.default();

    const test_dir = "test_follower_heartbeat";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer allocator.free(log_path);

    var log = try Log.create(log_config, log_path, allocator);
    defer log.delete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    const initial_heartbeat = follower.last_heartbeat_ms;

    // Wait a bit
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Send heartbeat with leader_offset = 100 (leader has 100 entries)
    // Follower has 0 entries, so should enter catching_up state
    const req = HeartbeatRequest{
        .leader_commit = 42,
        .leader_offset = 100,
    };

    const resp = follower.handleHeartbeat(req);

    try std.testing.expect(follower.last_heartbeat_ms > initial_heartbeat);
    try std.testing.expectEqual(@as(u64, 42), follower.commit_index);
    // Empty log returns null
    try std.testing.expectEqual(@as(?u64, null), resp.follower_offset);
    // Should enter catching_up state because follower is behind
    try std.testing.expectEqual(FollowerState.catching_up, follower.state);
}

test "Follower: isHealthy" {
    const allocator = std.testing.allocator;

    const log_config = @import("../log/log_config.zig").LogConfig.default();

    const test_dir = "test_follower_healthy";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer allocator.free(log_path);

    var log = try Log.create(log_config, log_path, allocator);
    defer log.delete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    // Should be healthy immediately
    try std.testing.expect(follower.isHealthy(1000));

    // Wait 50ms
    std.Thread.sleep(50 * std.time.ns_per_ms);

    // Should be unhealthy with very short timeout
    try std.testing.expect(!follower.isHealthy(10));

    // But healthy with longer timeout
    try std.testing.expect(follower.isHealthy(1000));
}

test "Follower: needsCatchUp" {
    const allocator = std.testing.allocator;

    const log_config = @import("../log/log_config.zig").LogConfig.default();

    const test_dir = "test_follower_catchup_state";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(abs_path);

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ abs_path, test_dir });
    defer allocator.free(log_path);

    var log = try Log.create(log_config, log_path, allocator);
    defer log.delete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    // Should not need catch-up initially
    try std.testing.expect(!follower.needsCatchUp());

    // Trigger gap detection
    const record = Record{
        .key = null,
        .value = "test",
    };

    var entries = try allocator.alloc(protocol.ReplicatedEntry, 1);
    defer allocator.free(entries);
    entries[0] = protocol.ReplicatedEntry{ .offset = 10, .record = record };

    const req = ReplicateRequest{
        .entries = entries,
        .leader_commit = 10,
    };

    _ = try follower.handleReplicateRequest(req);

    // Now should need catch-up
    try std.testing.expect(follower.needsCatchUp());
}
