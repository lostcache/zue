const std = @import("std");
const Log = @import("../log/log.zig").Log;
const Record = @import("../log/record.zig").Record;
const protocol = @import("../network/protocol.zig");

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;
const ErrorCode = protocol.ErrorCode;

const Allocator = std.mem.Allocator;

pub const Follower = struct {
    log: *Log,
    allocator: Allocator,
    leader_address: []const u8,
    leader_port: u16,
    last_heartbeat_ms: i64,
    commit_index: u64, // Learned from leader

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
            .last_heartbeat_ms = std.time.milliTimestamp(),
            .commit_index = 0,
        };
    }

    /// Clean up resources
    pub fn deinit(self: *Follower) void {
        self.allocator.free(self.leader_address);
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
                // GAP DETECTED: Return error to leader
                // Leader will handle repair via tickRepair()
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

        // Simply return current offset
        // Leader will determine if we're behind and trigger repair via tickRepair()
        return HeartbeatResponse{
            .follower_offset = if (self.log.next_offset > 0) self.log.next_offset - 1 else null,
        };
    }

    /// Check if follower is healthy (received recent heartbeat)
    pub fn isHealthy(self: *const Follower, timeout_ms: u64) bool {
        const now_ms = std.time.milliTimestamp();
        const elapsed: u64 = @intCast(now_ms - self.last_heartbeat_ms);
        return elapsed < timeout_ms;
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
    defer log.closeAndDelete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

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
    defer log.closeAndDelete() catch {};

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
    defer log.closeAndDelete() catch {};

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
    // Leader will handle repair via tickRepair()
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
    defer log.closeAndDelete() catch {};

    var follower = try Follower.init(&log, "127.0.0.1", 9000, allocator);
    defer follower.deinit();

    const initial_heartbeat = follower.last_heartbeat_ms;

    // Wait a bit
    std.Thread.sleep(10 * std.time.ns_per_ms);

    // Send heartbeat with leader_offset = 100 (leader has 100 entries)
    // Follower has 0 entries (leader will detect lag and trigger repair)
    const req = HeartbeatRequest{
        .leader_commit = 42,
        .leader_offset = 100,
    };

    const resp = follower.handleHeartbeat(req);

    try std.testing.expect(follower.last_heartbeat_ms > initial_heartbeat);
    try std.testing.expectEqual(@as(u64, 42), follower.commit_index);
    // Empty log returns null
    try std.testing.expectEqual(@as(?u64, null), resp.follower_offset);
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
    defer log.closeAndDelete() catch {};

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
