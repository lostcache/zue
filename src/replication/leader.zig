/// Leader Implementation for Phase 2 Replication
///
/// Manages replication to followers with:
/// - SEQUENTIAL (non-parallel) replication to followers
/// - Quorum-based commit logic
/// - In-Sync Replica (ISR) tracking
/// - Heartbeat management
/// - Uncommitted write buffer
const std = @import("std");
const Log = @import("../log/log.zig").Log;
const LogConfig = @import("../log/log_config.zig").LogConfig;
const Record = @import("../log/record.zig").Record;
const protocol = @import("../network/protocol.zig");
const UncommittedBuffer = @import("uncommitted.zig").UncommittedBuffer;
const ClusterConfig = @import("../config.zig").ClusterConfig;
const NodeConfig = @import("../config.zig").NodeConfig;

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;
const Allocator = std.mem.Allocator;

/// State for a single follower connection
pub const FollowerConnection = struct {
    id: u32,
    address: []const u8,
    port: u16,
    stream: ?std.net.Stream,
    last_offset: u64, // Follower's last known offset
    last_heartbeat_ms: i64,
    in_sync: bool, // Is follower within acceptable lag?
    allocator: Allocator,

    pub fn init(
        allocator: Allocator,
        id: u32,
        address: []const u8,
        port: u16,
        initial_offset: u64,
    ) !FollowerConnection {
        const stored_address = try allocator.dupe(u8, address);

        return FollowerConnection{
            .id = id,
            .address = stored_address,
            .port = port,
            .stream = null,
            .last_offset = initial_offset,
            .last_heartbeat_ms = std.time.milliTimestamp(),
            .in_sync = true, // Start optimistic
            .allocator = allocator,
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
};

/// Leader manages replication to followers
pub const Leader = struct {
    log: *Log,
    allocator: Allocator,
    followers: []FollowerConnection,
    quorum_size: u32,
    replication_timeout_ms: u64,
    max_lag_entries: u64,
    commit_index: u64,
    uncommitted: UncommittedBuffer,

    /// Initialize leader with cluster configuration
    pub fn init(
        allocator: Allocator,
        log: *Log,
        config: ClusterConfig,
    ) !Leader {
        // Count followers (all nodes except leader)
        var follower_count: usize = 0;
        for (config.nodes) |node| {
            if (node.role == .follower) {
                follower_count += 1;
            }
        }

        // Allocate followers array
        const followers = try allocator.alloc(FollowerConnection, follower_count);
        errdefer allocator.free(followers);

        // Initialize follower connections
        // Note: last_offset represents the last offset successfully written on the follower
        // If log is empty, use maxInt as sentinel meaning "no data yet"
        // When replicating, last_offset + 1 wraps to 0 for the first write
        var idx: usize = 0;
        const initial_offset = if (log.next_offset > 0)
            log.next_offset - 1
        else
            std.math.maxInt(u64); // Sentinel for empty log

        for (config.nodes) |node| {
            if (node.role == .follower) {
                followers[idx] = try FollowerConnection.init(
                    allocator,
                    node.id,
                    node.address,
                    node.port,
                    initial_offset,
                );
                idx += 1;
            }
        }

        return Leader{
            .log = log,
            .allocator = allocator,
            .followers = followers,
            .quorum_size = config.replication.quorum_size,
            .replication_timeout_ms = config.replication.timeout_ms,
            .max_lag_entries = config.replication.max_lag_entries,
            .commit_index = initial_offset,
            .uncommitted = UncommittedBuffer.init(allocator),
        };
    }

    pub fn deinit(self: *Leader) void {
        // Clean up follower connections
        for (self.followers) |*follower| {
            follower.deinit();
        }
        self.allocator.free(self.followers);

        // Clean up uncommitted buffer
        self.uncommitted.deinit();
    }

    /// Connect to all followers
    /// If a follower connection fails, mark it as out-of-sync but continue
    pub fn connectToFollowers(self: *Leader) void {
        for (self.followers) |*follower| {
            follower.connect() catch |err| {
                std.debug.print("WARNING: Failed to connect to follower {d} at {s}:{d}: {}\n", .{
                    follower.id,
                    follower.address,
                    follower.port,
                    err,
                });
                follower.in_sync = false;
            };
        }
    }

    /// Replicate record to followers (BLOCKING, SEQUENTIAL)
    /// Returns the committed offset on success
    pub fn replicate(self: *Leader, record: Record) !u64 {
        // 1. Append to local log
        const offset = try self.log.append(record);

        // 2. Add to uncommitted buffer
        const now_ms = std.time.milliTimestamp();
        try self.uncommitted.add(offset, record, now_ms);

        // 3. Send to followers SEQUENTIALLY (no pipelining)
        var acks: u32 = 1; // Leader counts as first ack
        for (self.followers) |*follower| {
            if (!follower.in_sync) continue; // Skip out-of-sync followers

            // Calculate next offset for follower
            // If last_offset is maxInt (sentinel for empty), wrapping add gives 0
            const next_offset = follower.last_offset +% 1;

            const req = ReplicateRequest{
                .offset = next_offset, // Expected offset on follower
                .record = record,
                .leader_commit = self.commit_index,
            };

            // Send replication request
            const resp = follower.sendReplicateRequest(req, self.allocator) catch |err| {
                // Connection error or timeout
                std.debug.print("WARNING: Failed to replicate to follower {d}: {}\n", .{ follower.id, err });
                follower.in_sync = false;
                continue;
            };

            if (resp.success) {
                acks += 1;
                // follower_offset is the offset that was just written
                follower.last_offset = resp.follower_offset;
            } else {
                // Gap detected or other replication error
                std.debug.print("WARNING: Follower {d} rejected replication (error: {}), marking out-of-sync\n", .{
                    follower.id,
                    resp.error_code,
                });
                follower.in_sync = false;
            }
        }

        // 4. Check quorum
        if (acks >= self.quorum_size) {
            // SUCCESS: Quorum reached, commit the write
            self.commit_index = offset;
            try self.uncommitted.remove(offset);
            return offset;
        } else {
            // FAILURE: Quorum not reached - MUST ROLLBACK THE LOG
            // Remove from uncommitted buffer
            try self.uncommitted.remove(offset);

            // CRITICAL: Rollback the log to remove the uncommitted entry
            // This prevents log corruption where uncommitted entries persist
            try self.log.truncateToOffset(offset);

            return error.QuorumNotReached;
        }
    }

    /// Send heartbeats to all followers (called periodically)
    /// Updates follower ISR status based on their responses
    pub fn sendHeartbeats(self: *Leader) void {
        const leader_offset = if (self.log.next_offset > 0) self.log.next_offset - 1 else 0;

        for (self.followers) |*follower| {
            // Try to connect if not already connected
            if (follower.stream == null) {
                follower.connect() catch |err| {
                    std.debug.print("WARNING: Failed to connect to follower {d}: {}\n", .{ follower.id, err });
                    follower.in_sync = false;
                    follower.last_heartbeat_ms = 0;
                    continue;
                };
            }

            const req = HeartbeatRequest{
                .leader_commit = self.commit_index,
                .leader_offset = leader_offset,
            };

            const resp = follower.sendHeartbeat(req, self.allocator) catch |err| {
                std.debug.print("WARNING: Failed to send heartbeat to follower {d}: {}\n", .{ follower.id, err });
                follower.in_sync = false;
                follower.last_heartbeat_ms = 0; // Mark as failed
                follower.disconnect(); // Disconnect so we retry next time
                continue;
            };

            // Update follower state
            follower.last_heartbeat_ms = std.time.milliTimestamp();
            // follower_offset is the follower's last written offset
            follower.last_offset = resp.follower_offset;

            // Update in_sync status based on lag
            // Handle maxInt sentinel (empty follower) specially
            const lag = if (follower.last_offset == std.math.maxInt(u64))
                leader_offset + 1 // Follower is empty, lag is entire log size
            else if (leader_offset >= follower.last_offset)
                leader_offset - follower.last_offset
            else
                0;
            follower.in_sync = (lag <= self.max_lag_entries);
        }

        // CRITICAL: Check if ISR size < quorum_size
        const isr_count = self.countInSync() + 1; // +1 for leader
        if (isr_count < self.quorum_size) {
            std.debug.print("WARNING: ISR size ({d}) < quorum ({d}). System is NOT writable!\n", .{
                isr_count,
                self.quorum_size,
            });
        }
    }

    /// Count how many followers are currently in-sync
    pub fn countInSync(self: *const Leader) u32 {
        var count: u32 = 0;
        for (self.followers) |follower| {
            if (follower.in_sync) {
                count += 1;
            }
        }
        return count;
    }

    /// Check if we have enough in-sync replicas to accept writes
    pub fn hasQuorum(self: *const Leader) bool {
        const isr_count = self.countInSync() + 1; // +1 for leader
        return isr_count >= self.quorum_size;
    }

    /// Get the current commit index
    pub fn getCommitIndex(self: *const Leader) u64 {
        return self.commit_index;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "Leader: init and deinit" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create a temporary log
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.delete() catch {};

    // Create cluster config
    var nodes = try allocator.alloc(NodeConfig, 3);
    // Note: ClusterConfig takes ownership of nodes and will free them in deinit

    nodes[0] = .{ .id = 1, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9001, .role = .leader };
    nodes[1] = .{ .id = 2, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9002, .role = .follower };
    nodes[2] = .{ .id = 3, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9003, .role = .follower };

    var cluster_config = ClusterConfig{
        .nodes = nodes,
        .replication = .{
            .quorum_size = 2,
            .timeout_ms = 5000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 2000,
        },
        .allocator = allocator,
    };
    defer cluster_config.deinit();

    var leader = try Leader.init(allocator, &log, cluster_config);
    defer leader.deinit();

    try testing.expectEqual(@as(usize, 2), leader.followers.len);
    try testing.expectEqual(@as(u32, 2), leader.quorum_size);
    try testing.expectEqual(@as(u64, 0), leader.commit_index);
}

test "Leader: countInSync" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.delete() catch {};

    var nodes = try allocator.alloc(NodeConfig, 3);
    // Note: ClusterConfig takes ownership of nodes and will free them in deinit

    nodes[0] = .{ .id = 1, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9001, .role = .leader };
    nodes[1] = .{ .id = 2, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9002, .role = .follower };
    nodes[2] = .{ .id = 3, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9003, .role = .follower };

    var cluster_config = ClusterConfig{
        .nodes = nodes,
        .replication = .{
            .quorum_size = 2,
            .timeout_ms = 5000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 2000,
        },
        .allocator = allocator,
    };
    defer cluster_config.deinit();

    var leader = try Leader.init(allocator, &log, cluster_config);
    defer leader.deinit();

    // Initially all followers are in-sync
    try testing.expectEqual(@as(u32, 2), leader.countInSync());

    // Mark one follower out-of-sync
    leader.followers[0].in_sync = false;
    try testing.expectEqual(@as(u32, 1), leader.countInSync());

    // Mark all followers out-of-sync
    leader.followers[1].in_sync = false;
    try testing.expectEqual(@as(u32, 0), leader.countInSync());
}

test "Leader: hasQuorum" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.delete() catch {};

    var nodes = try allocator.alloc(NodeConfig, 3);
    // Note: ClusterConfig takes ownership of nodes and will free them in deinit

    nodes[0] = .{ .id = 1, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9001, .role = .leader };
    nodes[1] = .{ .id = 2, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9002, .role = .follower };
    nodes[2] = .{ .id = 3, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9003, .role = .follower };

    var cluster_config = ClusterConfig{
        .nodes = nodes,
        .replication = .{
            .quorum_size = 2,
            .timeout_ms = 5000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 2000,
        },
        .allocator = allocator,
    };
    defer cluster_config.deinit();

    var leader = try Leader.init(allocator, &log, cluster_config);
    defer leader.deinit();

    // Initially: 2 followers + 1 leader = 3 >= 2 (has quorum)
    try testing.expectEqual(true, leader.hasQuorum());

    // One follower down: 1 follower + 1 leader = 2 >= 2 (still has quorum)
    leader.followers[0].in_sync = false;
    try testing.expectEqual(true, leader.hasQuorum());

    // Both followers down: 0 followers + 1 leader = 1 < 2 (no quorum)
    leader.followers[1].in_sync = false;
    try testing.expectEqual(false, leader.hasQuorum());
}

test "FollowerConnection: init and deinit" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var follower = try FollowerConnection.init(allocator, 1, "127.0.0.1", 9000, 0);
    defer follower.deinit();

    try testing.expectEqual(@as(u32, 1), follower.id);
    try testing.expectEqualStrings("127.0.0.1", follower.address);
    try testing.expectEqual(@as(u16, 9000), follower.port);
    try testing.expectEqual(@as(u64, 0), follower.last_offset);
    try testing.expectEqual(true, follower.in_sync);
}

test "Leader: replicate with quorum failure rolls back log" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.delete() catch {};

    // Create cluster config with quorum=2 (need leader + 1 follower)
    var nodes = try allocator.alloc(NodeConfig, 3);
    // Note: ClusterConfig takes ownership

    nodes[0] = .{ .id = 1, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9001, .role = .leader };
    nodes[1] = .{ .id = 2, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9002, .role = .follower };
    nodes[2] = .{ .id = 3, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9003, .role = .follower };

    var cluster_config = ClusterConfig{
        .nodes = nodes,
        .replication = .{
            .quorum_size = 2, // Need leader + 1 follower
            .timeout_ms = 5000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 2000,
        },
        .allocator = allocator,
    };
    defer cluster_config.deinit();

    var leader = try Leader.init(allocator, &log, cluster_config);
    defer leader.deinit();

    // Mark all followers as out-of-sync so quorum will fail
    for (leader.followers) |*follower| {
        follower.in_sync = false;
    }

    // Verify initial log state
    try testing.expectEqual(@as(u64, 0), log.getNextOffset());

    // Try to replicate - should fail due to no quorum
    const record = Record{ .key = "test", .value = "value" };
    const result = leader.replicate(record);

    // Should return QuorumNotReached error
    try testing.expectError(error.QuorumNotReached, result);

    // CRITICAL: Verify the log was rolled back
    try testing.expectEqual(@as(u64, 0), log.getNextOffset());

    // Verify we can't read the failed write
    try testing.expectError(error.OffsetNotFound, log.read(0, allocator));

    // Verify we can successfully append at offset 0 again (offset was reclaimed)
    const new_record = Record{ .key = "new", .value = "new_value" };
    const new_offset = try log.append(new_record);
    try testing.expectEqual(@as(u64, 0), new_offset);
    try testing.expectEqual(@as(u64, 1), log.getNextOffset());
}

test "Leader: replicate with quorum success does not rollback" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.delete() catch {};

    var nodes = try allocator.alloc(NodeConfig, 2);
    // Note: ClusterConfig takes ownership

    nodes[0] = .{ .id = 1, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9001, .role = .leader };
    nodes[1] = .{ .id = 2, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9002, .role = .follower };

    var cluster_config = ClusterConfig{
        .nodes = nodes,
        .replication = .{
            .quorum_size = 1, // Only need leader (no followers required)
            .timeout_ms = 5000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 2000,
        },
        .allocator = allocator,
    };
    defer cluster_config.deinit();

    var leader = try Leader.init(allocator, &log, cluster_config);
    defer leader.deinit();

    // Mark follower as out-of-sync (but we don't need it for quorum=1)
    leader.followers[0].in_sync = false;

    // Replicate - should succeed with just leader
    const record = Record{ .key = "test", .value = "value" };
    const offset = try leader.replicate(record);

    // Should succeed and return offset 0
    try testing.expectEqual(@as(u64, 0), offset);

    // Verify log was NOT rolled back
    try testing.expectEqual(@as(u64, 1), log.getNextOffset());

    // Verify we CAN read the committed write
    const read_rec = try log.read(0, allocator);
    defer {
        if (read_rec.key) |k| allocator.free(k);
        allocator.free(read_rec.value);
    }
    try testing.expectEqualStrings("value", read_rec.value);

    // Verify commit_index was updated
    try testing.expectEqual(@as(u64, 0), leader.commit_index);
}
