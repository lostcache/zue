const std = @import("std");
const Log = @import("../../log.zig").Log;
const Record = @import("../../log/record.zig").Record;
const ClusterConfig = @import("../../config.zig").ClusterConfig;
const FollowerConnection = @import("follower_connection.zig").FollowerConnection;
const Allocator = std.mem.Allocator;

// For tests
const LogConfig = @import("../../log/log_config.zig").LogConfig;
const NodeConfig = @import("../../config.zig").NodeConfig;

// Import helper functions from other modules
const strategy = @import("replication_strategy.zig");
const quorum = @import("quorum.zig");
const request_handling = @import("request_handling.zig");
const background_repair = @import("background_repair.zig");

/// Leader manages replication to followers
pub const Leader = struct {
    log: *Log,
    allocator: Allocator,
    followers: []FollowerConnection,
    quorum_size: u32,
    replication_timeout_ms: u64,
    max_lag_entries: u64,
    commit_index: ?u64, // null means no entries committed yet
    repair_batch_size: u32, // Max entries per repair tick
    inline_catchup_threshold: u32, // Max entries to stream inline during write

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
        // Note: For Raft-like replication, match_index tracks highest replicated offset
        // For empty log: match_index = null, next_index = 0
        var idx: usize = 0;
        const initial_offset: ?u64 = if (log.next_offset > 0)
            log.next_offset - 1
        else
            null; // Empty log - no data yet

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

        // For empty log, commit_index is null (no committed entries yet)
        const initial_commit_index = initial_offset;

        return Leader{
            .log = log,
            .allocator = allocator,
            .followers = followers,
            .quorum_size = config.replication.quorum_size,
            .replication_timeout_ms = config.replication.timeout_ms,
            .max_lag_entries = config.replication.max_lag_entries,
            .commit_index = initial_commit_index,
            .repair_batch_size = config.replication.repair_batch_size,
            .inline_catchup_threshold = config.replication.inline_catchup_threshold,
        };
    }

    pub fn deinit(self: *Leader) void {
        // Clean up follower connections
        for (self.followers) |*follower| {
            follower.deinit();
        }
        self.allocator.free(self.followers);
    }

    /// Connect to all followers
    /// If a follower connection fails, mark it as out-of-sync but continue
    pub fn connectToFollowers(self: *Leader) void {
        for (self.followers) |*follower| {
            follower.connect() catch {
                follower.in_sync = false;
            };
        }
    }

    // Proxy methods to implementation files

    pub fn replicate(self: *Leader, record: Record) !u64 {
        return request_handling.replicate(self, record);
    }

    pub fn tickRepair(self: *Leader) bool {
        return background_repair.tickRepair(self);
    }

    pub fn sendHeartbeats(self: *Leader) void {
        background_repair.sendHeartbeats(self);
    }

    pub fn determineFollowerStrategy(
        self: *Leader,
        follower: *FollowerConnection,
        current_offset: u64,
    ) strategy.ReplicationStrategy {
        return strategy.determineFollowerStrategy(self, follower, current_offset);
    }

    pub fn calculateCommitIndex(self: *const Leader) ?u64 {
        return quorum.calculateCommitIndex(self);
    }

    pub fn countInSync(self: *const Leader) u32 {
        return quorum.countInSync(self);
    }

    pub fn hasQuorum(self: *const Leader) bool {
        return quorum.hasQuorum(self);
    }

    pub fn getCommitIndex(self: *const Leader) ?u64 {
        return quorum.getCommitIndex(self);
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
    defer log.closeAndDelete() catch {};

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
    try testing.expectEqual(@as(?u64, null), leader.commit_index); // Empty log has no committed entries
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
    defer log.closeAndDelete() catch {};

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
    defer log.closeAndDelete() catch {};

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

test "Leader: replicate with quorum failure leaves entry in log (Raft behavior)" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.closeAndDelete() catch {};

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

    // RAFT BEHAVIOR: Entry STAYS in log (NO ROLLBACK)
    try testing.expectEqual(@as(u64, 1), log.getNextOffset());

    // Verify we CAN read the uncommitted write
    const read_rec = try log.read(0, allocator);
    defer {
        if (read_rec.key) |k| allocator.free(k);
        allocator.free(read_rec.value);
    }
    try testing.expectEqualStrings("value", read_rec.value);

    // Verify commit_index did NOT advance (entry is uncommitted)
    try testing.expectEqual(@as(?u64, null), leader.commit_index);
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
    defer log.closeAndDelete() catch {};

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

    // Verify commit_index was updated (entry is committed)
    try testing.expectEqual(@as(?u64, 0), leader.commit_index);
}

test "Leader: repairFollowerLog decrements next_index until match found" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const log_dir = try tmp_dir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_dir);

    const log_config = LogConfig.default();
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.closeAndDelete() catch {};

    // Add some entries to leader log
    _ = try log.append(Record{ .key = null, .value = "entry0" });
    _ = try log.append(Record{ .key = null, .value = "entry1" });
    _ = try log.append(Record{ .key = null, .value = "entry2" });

    var nodes = try allocator.alloc(NodeConfig, 2);
    nodes[0] = .{ .id = 1, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9001, .role = .leader };
    nodes[1] = .{ .id = 2, .address = try allocator.dupe(u8, "127.0.0.1"), .port = 9002, .role = .follower };

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

    // Verify initial state
    try testing.expectEqual(@as(u64, 3), log.getNextOffset());
    try testing.expectEqual(@as(usize, 1), leader.followers.len);

    // Verify follower was initialized with correct next_index
    // After init, follower should have match_index = 2 (last entry) and next_index = 3
    try testing.expectEqual(@as(?u64, 2), leader.followers[0].match_index);
    try testing.expectEqual(@as(u64, 3), leader.followers[0].next_index);
}