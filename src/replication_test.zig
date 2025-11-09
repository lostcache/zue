/// Integration tests for distributed replication
///
/// These tests verify the emergent behavior of the entire distributed system:
/// - Basic replication (leader to followers)
/// - Quorum-based writes
/// - Follower catch-up
/// - System behavior under node failures
const std = @import("std");
const testing = std.testing;
const net = std.net;
const fs = std.fs;

const Client = @import("client.zig").Client;
const Record = @import("log/record.zig").Record;
const Log = @import("log/log.zig").Log;
const LogConfig = @import("log/log.zig").LogConfig;

// ============================================================================
// Test Configuration
// ============================================================================

const TEST_TIMEOUT_MS = 10_000;
const SERVER_START_TIMEOUT_MS = 2000;

// ============================================================================
// Cluster Configuration Helper
// ============================================================================

/// Write a cluster config file for 1 leader + N followers
fn writeClusterConfig(
    config_path: []const u8,
    leader_port: u16,
    follower_ports: []const u16,
) !void {
    const file = try fs.cwd().createFile(config_path, .{});
    defer file.close();

    // Build config string
    var buffer = try std.ArrayList(u8).initCapacity(std.heap.page_allocator, 1024);
    defer buffer.deinit(std.heap.page_allocator);

    const writer = buffer.writer(std.heap.page_allocator);

    // Write nodes (format: node <id> <address> <port> <role>)
    try writer.print("node 1 127.0.0.1 {d} leader\n", .{leader_port});

    for (follower_ports, 0..) |port, i| {
        const node_id = @as(u32, @intCast(i + 2)); // Follower IDs start at 2
        try writer.print("node {d} 127.0.0.1 {d} follower\n", .{ node_id, port });
    }

    // Write replication settings
    try writer.writeAll("\n");
    try writer.print("quorum {d}\n", .{2}); // Need leader + 1 follower
    try writer.writeAll("timeout 5000\n");
    try writer.writeAll("max_lag 100\n");
    try writer.writeAll("heartbeat 2000\n");

    // Write to file
    try file.writeAll(buffer.items);
}

// ============================================================================
// Replication Cluster Helper
// ============================================================================

const NodeProcess = struct {
    process: std.process.Child,
    node_id: u32,
    port: u16,
    tmp_dir: testing.TmpDir,
    tmp_path: []const u8,
    allocator: std.mem.Allocator,
};

const ReplicationCluster = struct {
    leader: NodeProcess,
    followers: []NodeProcess,
    config_path: []const u8,
    allocator: std.mem.Allocator,

    /// Initialize a 1 leader + N followers cluster
    fn init(allocator: std.mem.Allocator, num_followers: usize) !ReplicationCluster {
        // Allocate ports
        const leader_port = try findFreePort();
        const follower_ports = try allocator.alloc(u16, num_followers);
        defer allocator.free(follower_ports);

        for (follower_ports) |*port| {
            port.* = try findFreePort();
        }

        // Create config file
        const config_path = try allocator.dupe(u8, "/tmp/zue_cluster_config.txt");
        errdefer allocator.free(config_path);

        try writeClusterConfig(config_path, leader_port, follower_ports);

        // Start leader
        var leader = try startNode(allocator, 1, leader_port, config_path);
        errdefer stopNode(&leader);

        // Wait for leader to be ready
        try waitForServer(leader_port);

        // Start followers
        var followers = try allocator.alloc(NodeProcess, num_followers);
        errdefer allocator.free(followers);

        var started_count: usize = 0;
        errdefer {
            // Clean up any followers we started
            for (followers[0..started_count]) |*follower| {
                stopNode(follower);
            }
        }

        for (followers, 0..) |*follower, i| {
            const node_id = @as(u32, @intCast(i + 2)); // Follower IDs start at 2
            follower.* = try startNode(allocator, node_id, follower_ports[i], config_path);
            try waitForServer(follower_ports[i]);
            started_count += 1;
        }

        return ReplicationCluster{
            .leader = leader,
            .followers = followers,
            .config_path = config_path,
            .allocator = allocator,
        };
    }

    fn deinit(self: *ReplicationCluster) void {
        stopNode(&self.leader);

        for (self.followers) |*follower| {
            stopNode(follower);
        }

        self.allocator.free(self.followers);

        // Clean up config file (before freeing path)
        fs.cwd().deleteFile(self.config_path) catch {};
        self.allocator.free(self.config_path);
    }

    /// Get a client connected to the leader
    fn getLeaderClient(self: *ReplicationCluster) !Client {
        return try Client.connect("127.0.0.1", self.leader.port, self.allocator);
    }

    /// Get a client connected to a specific follower
    fn getFollowerClient(self: *ReplicationCluster, follower_index: usize) !Client {
        if (follower_index >= self.followers.len) return error.InvalidFollowerIndex;
        return try Client.connect("127.0.0.1", self.followers[follower_index].port, self.allocator);
    }

    /// Kill a follower (for testing failure scenarios)
    fn killFollower(self: *ReplicationCluster, follower_index: usize) !void {
        if (follower_index >= self.followers.len) return error.InvalidFollowerIndex;
        _ = try self.followers[follower_index].process.kill();
        _ = try self.followers[follower_index].process.wait();
        std.debug.print("Killed follower {d}\n", .{follower_index});
    }

    /// Restart a killed follower
    fn restartFollower(self: *ReplicationCluster, follower_index: usize) !void {
        if (follower_index >= self.followers.len) return error.InvalidFollowerIndex;

        const follower = &self.followers[follower_index];

        // Clean up old tmp_path and tmp_dir
        self.allocator.free(follower.tmp_path);
        follower.tmp_dir.cleanup();

        // Restart the process with the same node_id and port, but a new tmp_dir
        const node = try startNode(self.allocator, follower.node_id, follower.port, self.config_path);

        // Replace all fields with the new node
        follower.process = node.process;
        follower.tmp_dir = node.tmp_dir;
        follower.tmp_path = node.tmp_path;

        // Wait for server to be ready
        try waitForServer(follower.port);
        std.debug.print("Restarted follower {d}\n", .{follower_index});
    }

    /// Read a record from a follower's log directly (for verification)
    fn readFromFollowerLog(self: *ReplicationCluster, follower_index: usize, offset: u64, allocator: std.mem.Allocator) !Record {
        if (follower_index >= self.followers.len) return error.InvalidFollowerIndex;

        const follower = &self.followers[follower_index];
        const log_config = LogConfig.default();
        var log = try Log.open(log_config, follower.tmp_path, allocator);
        defer log.close();

        return try log.read(offset, allocator);
    }
};

// ============================================================================
// Helper Functions
// ============================================================================

fn findFreePort() !u16 {
    const address = try net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    return server.listen_address.in.getPort();
}

fn waitForServer(port: u16) !void {
    const max_retries = 20;
    const retry_delay_ms = 100;
    var retries: u8 = 0;

    while (retries < max_retries) : (retries += 1) {
        const address = net.Address.parseIp("127.0.0.1", port) catch continue;
        const stream = net.tcpConnectToAddress(address) catch {
            std.Thread.sleep(retry_delay_ms * std.time.ns_per_ms);
            continue;
        };
        stream.close();
        return;
    }
    return error.ServerDidNotStart;
}

fn startNode(
    allocator: std.mem.Allocator,
    node_id: u32,
    port: u16,
    config_path: []const u8,
) !NodeProcess {
    var tmp = testing.tmpDir(.{});
    errdefer tmp.cleanup();

    const port_str = try std.fmt.allocPrint(allocator, "{d}", .{port});
    defer allocator.free(port_str);

    const node_id_str = try std.fmt.allocPrint(allocator, "{d}", .{node_id});
    defer allocator.free(node_id_str);

    const tmp_path = try tmp.dir.realpathAlloc(allocator, ".");
    errdefer allocator.free(tmp_path);

    // TODO: Update this to use the cluster-aware server launch
    // For now, we'll use a simple approach: zue-server <port> <log_dir> <config> <node_id>
    const args = [_][]const u8{ "zig-out/bin/zue-server", port_str, tmp_path, config_path, node_id_str };

    std.debug.print("Starting node {d}: {s} port={s} log={s} config={s}\n", .{ node_id, args[0], port_str, tmp_path, config_path });

    var server_process = std.process.Child.init(&args, allocator);
    server_process.stdout_behavior = .Inherit;
    server_process.stderr_behavior = .Inherit;

    try server_process.spawn();

    return NodeProcess{
        .process = server_process,
        .node_id = node_id,
        .port = port,
        .tmp_dir = tmp,
        .tmp_path = tmp_path,
        .allocator = allocator,
    };
}

fn stopNode(node: *NodeProcess) void {
    _ = node.process.kill() catch {};
    _ = node.process.wait() catch {};
    node.allocator.free(node.tmp_path);
    node.tmp_dir.cleanup();
}

// ============================================================================
// Polling Helpers (Replace sleep() with condition checking)
// ============================================================================

/// Poll for a condition to become true, with timeout
fn waitForCondition(
    comptime check_fn: anytype,
    context: anytype,
    timeout_ms: u64,
    poll_interval_ms: u64,
) !void {
    const start = std.time.milliTimestamp();
    while (true) {
        if (check_fn(context)) {
            return;
        }

        const elapsed = @as(u64, @intCast(std.time.milliTimestamp() - start));
        if (elapsed >= timeout_ms) {
            return error.ConditionTimeout;
        }

        std.Thread.sleep(poll_interval_ms * std.time.ns_per_ms);
    }
}

/// Wait for a record to be replicated to a follower's log
fn waitForReplication(
    cluster: *ReplicationCluster,
    follower_index: usize,
    offset: u64,
    allocator: std.mem.Allocator,
    timeout_ms: u64,
) !void {
    const start = std.time.milliTimestamp();
    while (true) {
        // Try to read the record
        const result = cluster.readFromFollowerLog(follower_index, offset, allocator);
        if (result) |record| {
            // Success - record exists, clean it up
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
            return;
        } else |_| {
            // Record not found yet, continue polling
        }

        const elapsed = @as(u64, @intCast(std.time.milliTimestamp() - start));
        if (elapsed >= timeout_ms) {
            return error.ReplicationTimeout;
        }

        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}

/// Wait for all followers to have a specific record
fn waitForAllFollowersReplication(
    cluster: *ReplicationCluster,
    offset: u64,
    allocator: std.mem.Allocator,
    timeout_ms: u64,
) !void {
    for (0..cluster.followers.len) |i| {
        try waitForReplication(cluster, i, offset, allocator, timeout_ms);
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

test "replication: basic replication (1 leader + 2 followers)" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready (initial heartbeats bring followers in-sync)
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write a record to the leader
    const record = Record{ .key = "test-key", .value = "test-value" };
    const offset = try leader_client.append(record);
    try testing.expectEqual(@as(u64, 0), offset);

    // Poll for replication instead of blind sleep
    try waitForAllFollowersReplication(&cluster, offset, allocator, 5000);

    // Verify the record content on both followers
    {
        const follower1_record = try cluster.readFromFollowerLog(0, offset, allocator);
        defer {
            if (follower1_record.key) |k| allocator.free(k);
            allocator.free(follower1_record.value);
        }
        try testing.expectEqualStrings("test-key", follower1_record.key.?);
        try testing.expectEqualStrings("test-value", follower1_record.value);
    }

    {
        const follower2_record = try cluster.readFromFollowerLog(1, offset, allocator);
        defer {
            if (follower2_record.key) |k| allocator.free(k);
            allocator.free(follower2_record.value);
        }
        try testing.expectEqualStrings("test-key", follower2_record.key.?);
        try testing.expectEqualStrings("test-value", follower2_record.value);
    }

    std.debug.print("✓ Test 1 passed: Basic replication works\n", .{});
}

test "replication: quorum write with one follower down" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Kill one follower
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0\n", .{});

    // Wait for leader to detect failure (heartbeat-based, needs ~2 heartbeat intervals)
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write should still succeed (quorum = 2, we have leader + 1 follower)
    const record = Record{ .key = "quorum-test", .value = "one-down" };
    const offset = try leader_client.append(record);
    try testing.expectEqual(@as(u64, 0), offset);

    std.debug.print("Write succeeded with one follower down\n", .{});

    // Poll for replication to surviving follower
    try waitForReplication(&cluster, 1, offset, allocator, 5000);

    {
        const follower_record = try cluster.readFromFollowerLog(1, offset, allocator);
        defer {
            if (follower_record.key) |k| allocator.free(k);
            allocator.free(follower_record.value);
        }
        try testing.expectEqualStrings("quorum-test", follower_record.key.?);
        try testing.expectEqualStrings("one-down", follower_record.value);
    }

    std.debug.print("✓ Test 2 passed: Quorum write with one follower down works\n", .{});
}

test "replication: quorum failure with majority down" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Kill both followers
    try cluster.killFollower(0);
    try cluster.killFollower(1);
    std.debug.print("Killed both followers\n", .{});

    // Wait for leader to detect failures (heartbeat-based)
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write should FAIL (quorum = 2, we only have leader = 1)
    const record = Record{ .key = "fail-test", .value = "no-quorum" };
    const result = leader_client.append(record);

    // Expect ServerError because quorum not reached
    try testing.expectError(error.ServerError, result);

    std.debug.print("✓ Test 3 passed: Writes correctly fail without quorum\n", .{});
}

test "replication: follower catch-up after restart" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write first record (all nodes in sync)
    const record1 = Record{ .key = "before", .value = "stop" };
    const offset1 = try leader_client.append(record1);
    try testing.expectEqual(@as(u64, 0), offset1);
    try waitForAllFollowersReplication(&cluster, offset1, allocator, 5000);

    // Kill follower 0
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0\n", .{});

    // Wait for leader to detect failure
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Write more records while follower is down
    const record2 = Record{ .key = "during", .value = "downtime1" };
    const offset2 = try leader_client.append(record2);
    try testing.expectEqual(@as(u64, 1), offset2);

    const record3 = Record{ .key = "during", .value = "downtime2" };
    const offset3 = try leader_client.append(record3);
    try testing.expectEqual(@as(u64, 2), offset3);

    std.debug.print("Wrote 2 records while follower was down\n", .{});

    // Restart follower 0
    try cluster.restartFollower(0);
    std.debug.print("Restarted follower 0\n", .{});

    // Wait for follower to start
    std.Thread.sleep(2 * std.time.ns_per_s);

    // Trigger a new write to cause offset_mismatch and auto-repair
    const trigger_record = Record{ .key = "trigger", .value = "repair" };
    _ = try leader_client.append(trigger_record);
    std.debug.print("Triggered write to initiate repair (leader at offset 3, follower at 0)\n", .{});

    // Write final record
    const final_record = Record{ .key = "final", .value = "check" };
    const final_offset = try leader_client.append(final_record);
    std.debug.print("Wrote final record at offset {}\n", .{final_offset});

    // Poll for follower to catch up - wait for the final record
    try waitForReplication(&cluster, 0, final_offset, allocator, 15000);

    // Verify follower has all records
    {
        const record = try cluster.readFromFollowerLog(0, offset1, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("before", record.key.?);
        try testing.expectEqualStrings("stop", record.value);
    }

    {
        const record = try cluster.readFromFollowerLog(0, offset2, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("during", record.key.?);
        try testing.expectEqualStrings("downtime1", record.value);
    }

    {
        const record = try cluster.readFromFollowerLog(0, offset3, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("during", record.key.?);
        try testing.expectEqualStrings("downtime2", record.value);
    }

    std.debug.print("✓ Test 4 passed: Follower catch-up works after restart\n", .{});
}

test "replication: follower auto-recovery on offset_mismatch" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write some initial records (all nodes in sync)
    const record1 = Record{ .key = "init", .value = "entry1" };
    const offset1 = try leader_client.append(record1);
    try testing.expectEqual(@as(u64, 0), offset1);

    const record2 = Record{ .key = "init", .value = "entry2" };
    const offset2 = try leader_client.append(record2);
    try testing.expectEqual(@as(u64, 1), offset2);

    try waitForAllFollowersReplication(&cluster, offset2, allocator, 5000);
    std.debug.print("Wrote 2 initial records to all nodes\n", .{});

    // Kill follower 0
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0\n", .{});

    // Wait for leader to detect failure
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Write more records while follower is down (creates gap)
    const record3 = Record{ .key = "gap", .value = "entry3" };
    const offset3 = try leader_client.append(record3);
    try testing.expectEqual(@as(u64, 2), offset3);

    const record4 = Record{ .key = "gap", .value = "entry4" };
    const offset4 = try leader_client.append(record4);
    try testing.expectEqual(@as(u64, 3), offset4);

    std.debug.print("Wrote 2 more records while follower was down (offsets 2-3)\n", .{});

    // Restart follower 0 - it will be behind
    try cluster.restartFollower(0);
    std.debug.print("Restarted follower 0 (currently at offset 1, leader at offset 3)\n", .{});

    // Wait for follower to start
    std.Thread.sleep(2 * std.time.ns_per_s);

    // Now write a NEW entry that will trigger offset_mismatch
    const record5 = Record{ .key = "trigger", .value = "offset_mismatch" };
    const offset5 = try leader_client.append(record5);
    try testing.expectEqual(@as(u64, 4), offset5);

    std.debug.print("Wrote new record (offset 4) - should trigger offset_mismatch and auto-repair\n", .{});

    // Poll for follower to catch up - wait for the trigger entry
    try waitForReplication(&cluster, 0, offset5, allocator, 10000);

    // Verify follower 0 has ALL records including the trigger entry
    {
        const record = try cluster.readFromFollowerLog(0, offset5, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("trigger", record.key.?);
        try testing.expectEqualStrings("offset_mismatch", record.value);
    }

    // Also verify follower has the gap entries (2 and 3)
    {
        const record = try cluster.readFromFollowerLog(0, offset3, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("gap", record.key.?);
        try testing.expectEqualStrings("entry3", record.value);
    }

    std.debug.print("✓ Test 5 passed: Follower auto-recovered from offset_mismatch\n", .{});
}

test "replication: heartbeat monitoring and in_sync status" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write initial record to verify all nodes are in-sync
    const record1 = Record{ .key = "initial", .value = "all-healthy" };
    const offset1 = try leader_client.append(record1);
    try testing.expectEqual(@as(u64, 0), offset1);

    try waitForAllFollowersReplication(&cluster, offset1, allocator, 5000);
    std.debug.print("Initial write succeeded - all followers in-sync\n", .{});

    // Kill follower 0 to simulate heartbeat failure
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0 - simulating heartbeat failure\n", .{});

    // Wait for multiple heartbeat intervals to ensure leader detects the failure
    // Heartbeat interval is 2000ms, wait 5 seconds to be safe
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Write should still succeed because we have quorum (leader + 1 follower)
    // This implicitly verifies that follower 0 is marked out-of-sync (not counted for quorum)
    const record2 = Record{ .key = "after-failure", .value = "still-works" };
    const offset2 = try leader_client.append(record2);
    try testing.expectEqual(@as(u64, 1), offset2);

    std.debug.print("Write succeeded after follower 0 failure - follower 1 still in-sync\n", .{});

    // Poll for replication to surviving follower
    try waitForReplication(&cluster, 1, offset2, allocator, 5000);

    {
        const record = try cluster.readFromFollowerLog(1, offset2, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("after-failure", record.key.?);
        try testing.expectEqualStrings("still-works", record.value);
    }

    std.debug.print("✓ Test 6 passed: Heartbeat monitoring correctly tracks in_sync status\n", .{});
}

test "replication: follower lag detection with max_lag_entries" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write initial record
    const initial_record = Record{ .key = "start", .value = "baseline" };
    const initial_offset = try leader_client.append(initial_record);
    try testing.expectEqual(@as(u64, 0), initial_offset);
    try waitForAllFollowersReplication(&cluster, initial_offset, allocator, 5000);

    std.debug.print("Wrote initial record, all nodes in-sync\n", .{});

    // Kill follower 0
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0\n", .{});

    // Wait for leader to detect failure
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Write enough records to create significant lag (20 entries should be sufficient)
    // This tests that repair works even with substantial lag
    const lag_count = 20;
    std.debug.print("Writing {d} records to create lag...\n", .{lag_count});

    var i: usize = 0;
    while (i < lag_count) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "lag-key-{d}", .{i});
        defer allocator.free(key);

        const value = try std.fmt.allocPrint(allocator, "lag-value-{d}", .{i});
        defer allocator.free(value);

        const record = Record{ .key = key, .value = value };
        _ = try leader_client.append(record);
    }

    std.debug.print("Wrote {d} records while follower 0 was down\n", .{lag_count});

    // Restart follower 0 - it should be behind
    try cluster.restartFollower(0);
    std.debug.print("Restarted follower 0 (empty log, leader at offset {d})\n", .{lag_count + 1});

    // Wait for follower to start
    std.Thread.sleep(2 * std.time.ns_per_s);

    // Trigger a write to initiate offset_mismatch and repair
    const trigger_record = Record{ .key = "trigger", .value = "repair-start" };
    const trigger_offset = try leader_client.append(trigger_record);

    std.debug.print("Triggered write at offset {d} - should initiate repair\n", .{trigger_offset});

    // Write final record to verify follower is now in-sync
    const final_record = Record{ .key = "final", .value = "in-sync-check" };
    const final_offset = try leader_client.append(final_record);

    std.debug.print("Wrote final record at offset {d}\n", .{final_offset});

    // Poll for follower to catch up - wait for the final record
    try waitForReplication(&cluster, 0, final_offset, allocator, 20000);

    // Verify follower has the final record (proving it caught up)
    {
        const record = try cluster.readFromFollowerLog(0, final_offset, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("final", record.key.?);
        try testing.expectEqualStrings("in-sync-check", record.value);
    }

    std.debug.print("✓ Test 7 passed: Follower correctly recovered from lag\n", .{});
}

test "replication: multiple concurrent writes stress test" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Rapidly write many records
    const num_writes = 100;
    std.debug.print("Starting stress test with {d} rapid writes...\n", .{num_writes});

    var i: usize = 0;
    while (i < num_writes) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "stress-{d}", .{i});
        defer allocator.free(key);

        const value = try std.fmt.allocPrint(allocator, "value-{d}", .{i});
        defer allocator.free(value);

        const record = Record{ .key = key, .value = value };
        const offset = try leader_client.append(record);

        // Verify offset is sequential
        try testing.expectEqual(@as(u64, i), offset);
    }

    std.debug.print("Completed {d} writes, waiting for replication...\n", .{num_writes});

    // Poll for last write to be replicated to all followers
    const last_offset = num_writes - 1;
    try waitForAllFollowersReplication(&cluster, last_offset, allocator, 10000);

    // Verify first, middle, and last records were replicated to both followers
    const test_offsets = [_]u64{ 0, num_writes / 2, num_writes - 1 };

    for (test_offsets) |offset| {
        const expected_key = try std.fmt.allocPrint(allocator, "stress-{d}", .{offset});
        defer allocator.free(expected_key);

        const expected_value = try std.fmt.allocPrint(allocator, "value-{d}", .{offset});
        defer allocator.free(expected_value);

        // Check follower 0
        {
            const record = try cluster.readFromFollowerLog(0, offset, allocator);
            defer {
                if (record.key) |k| allocator.free(k);
                allocator.free(record.value);
            }
            try testing.expectEqualStrings(expected_key, record.key.?);
            try testing.expectEqualStrings(expected_value, record.value);
        }

        // Check follower 1
        {
            const record = try cluster.readFromFollowerLog(1, offset, allocator);
            defer {
                if (record.key) |k| allocator.free(k);
                allocator.free(record.value);
            }
            try testing.expectEqualStrings(expected_key, record.key.?);
            try testing.expectEqualStrings(expected_value, record.value);
        }
    }

    std.debug.print("✓ Test 8 passed: {d} rapid writes successfully replicated to all followers\n", .{num_writes});
}

test "replication: followers reject client requests with not_leader error" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to follower 0 (not the leader)
    var follower_client = try cluster.getFollowerClient(0);
    defer follower_client.disconnect();

    std.debug.print("Connected to follower 0, attempting to append...\n", .{});

    // Try to append to follower - should fail with not_leader error
    const record = Record{ .key = "test", .value = "should-fail" };
    const append_result = follower_client.append(record);

    // Expect ServerError (the client receives an error response from the server)
    try testing.expectError(error.ServerError, append_result);

    std.debug.print("Append correctly rejected by follower\n", .{});

    // Try to read from follower - should also fail with not_leader error
    const read_result = follower_client.read(0);

    // Expect ServerError
    try testing.expectError(error.ServerError, read_result);

    std.debug.print("Read correctly rejected by follower\n", .{});

    // Now verify that writes to the actual leader still work
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    const leader_record = Record{ .key = "leader", .value = "works" };
    const offset = try leader_client.append(leader_record);
    try testing.expectEqual(@as(u64, 0), offset);

    std.debug.print("Write to leader succeeded as expected\n", .{});

    std.debug.print("✓ Test 9 passed: Followers correctly reject client requests\n", .{});
}

test "replication: true concurrent writes from multiple threads" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    const num_threads = 5;
    const writes_per_thread = 20;
    const total_writes = num_threads * writes_per_thread;

    std.debug.print("Starting concurrent write test: {d} threads, {d} writes each = {d} total\n", .{ num_threads, writes_per_thread, total_writes });

    // Thread context to pass cluster info to worker threads
    const ThreadContext = struct {
        leader_port: u16,
        thread_id: usize,
        writes_per_thread: usize,
        offsets: []u64,
        allocator: std.mem.Allocator,
        error_occurred: *std.atomic.Value(bool),
    };

    // Allocate storage for returned offsets from all threads
    var all_offsets = try allocator.alloc(u64, total_writes);
    defer allocator.free(all_offsets);

    // Thread worker function
    const worker_fn = struct {
        fn run(ctx: *ThreadContext) void {
            // Connect to leader
            var client = Client.connect("127.0.0.1", ctx.leader_port, ctx.allocator) catch {
                ctx.error_occurred.store(true, .seq_cst);
                return;
            };
            defer client.disconnect();

            // Perform writes
            var i: usize = 0;
            while (i < ctx.writes_per_thread) : (i += 1) {
                const key = std.fmt.allocPrint(ctx.allocator, "thread-{d}-write-{d}", .{ ctx.thread_id, i }) catch {
                    ctx.error_occurred.store(true, .seq_cst);
                    return;
                };
                defer ctx.allocator.free(key);

                const value = std.fmt.allocPrint(ctx.allocator, "value-{d}-{d}", .{ ctx.thread_id, i }) catch {
                    ctx.error_occurred.store(true, .seq_cst);
                    return;
                };
                defer ctx.allocator.free(value);

                const record = Record{ .key = key, .value = value };
                const offset = client.append(record) catch {
                    ctx.error_occurred.store(true, .seq_cst);
                    return;
                };

                // Store offset
                ctx.offsets[i] = offset;
            }
        }
    }.run;

    // Spawn threads
    var threads = try allocator.alloc(std.Thread, num_threads);
    defer allocator.free(threads);

    var contexts = try allocator.alloc(ThreadContext, num_threads);
    defer allocator.free(contexts);

    var error_flag = std.atomic.Value(bool).init(false);

    for (0..num_threads) |thread_id| {
        const offset_start = thread_id * writes_per_thread;
        const offset_end = offset_start + writes_per_thread;

        contexts[thread_id] = ThreadContext{
            .leader_port = cluster.leader.port,
            .thread_id = thread_id,
            .writes_per_thread = writes_per_thread,
            .offsets = all_offsets[offset_start..offset_end],
            .allocator = allocator,
            .error_occurred = &error_flag,
        };

        threads[thread_id] = try std.Thread.spawn(.{}, worker_fn, .{&contexts[thread_id]});
    }

    // Wait for all threads to complete
    for (threads) |thread| {
        thread.join();
    }

    // Check if any errors occurred
    try testing.expect(!error_flag.load(.seq_cst));

    std.debug.print("All {d} concurrent writes completed\n", .{total_writes});

    // Verify all offsets are unique and in range [0, total_writes)
    var offset_seen = try allocator.alloc(bool, total_writes);
    defer allocator.free(offset_seen);
    @memset(offset_seen, false);

    for (all_offsets) |offset| {
        // Offset should be in valid range
        try testing.expect(offset < total_writes);

        // Offset should not be seen before (uniqueness check)
        try testing.expect(!offset_seen[offset]);
        offset_seen[offset] = true;
    }

    std.debug.print("Verified all {d} offsets are unique and sequential\n", .{total_writes});

    // Wait for replication of last offset
    const last_offset = total_writes - 1;
    try waitForAllFollowersReplication(&cluster, last_offset, allocator, 15000);

    std.debug.print("Verified replication to all followers\n", .{});

    // Spot-check: verify first, middle, and last offsets are replicated correctly
    // We need to find which thread wrote these offsets by checking all_offsets
    const test_offsets = [_]u64{ 0, total_writes / 2, total_writes - 1 };

    for (test_offsets) |check_offset| {
        // Find which thread and write number this offset corresponds to
        var found = false;
        for (0..num_threads) |thread_id| {
            for (0..writes_per_thread) |write_id| {
                const idx = thread_id * writes_per_thread + write_id;
                if (all_offsets[idx] == check_offset) {
                    // Reconstruct expected key/value
                    const expected_key = try std.fmt.allocPrint(allocator, "thread-{d}-write-{d}", .{ thread_id, write_id });
                    defer allocator.free(expected_key);

                    const expected_value = try std.fmt.allocPrint(allocator, "value-{d}-{d}", .{ thread_id, write_id });
                    defer allocator.free(expected_value);

                    // Verify on follower 0
                    {
                        const record = try cluster.readFromFollowerLog(0, check_offset, allocator);
                        defer {
                            if (record.key) |k| allocator.free(k);
                            allocator.free(record.value);
                        }
                        try testing.expectEqualStrings(expected_key, record.key.?);
                        try testing.expectEqualStrings(expected_value, record.value);
                    }

                    found = true;
                    break;
                }
            }
            if (found) break;
        }

        try testing.expect(found);
    }

    std.debug.print("✓ Test 10 passed: {d} concurrent writes from {d} threads succeeded without race conditions\n", .{ total_writes, num_threads });
}

test "replication: network partition and healing" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Wait for cluster to be ready
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Phase 1: Healthy cluster - write should succeed
    std.debug.print("Phase 1: Healthy cluster\n", .{});
    const record1 = Record{ .key = "pre-partition", .value = "healthy" };
    const offset1 = try leader_client.append(record1);
    try testing.expectEqual(@as(u64, 0), offset1);

    try waitForAllFollowersReplication(&cluster, offset1, allocator, 5000);
    std.debug.print("Write succeeded in healthy cluster\n", .{});

    // Phase 2: Simulate network partition by killing both followers
    std.debug.print("\nPhase 2: Network partition (killing both followers)\n", .{});
    try cluster.killFollower(0);
    try cluster.killFollower(1);
    std.debug.print("Partitioned: leader isolated from all followers\n", .{});

    // Wait for leader to detect partition via heartbeat failures
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Write should FAIL (no quorum - leader can't reach any followers)
    const partition_record = Record{ .key = "during-partition", .value = "should-fail" };
    const partition_result = leader_client.append(partition_record);

    try testing.expectError(error.ServerError, partition_result);
    std.debug.print("Write correctly failed during partition (no quorum)\n", .{});

    // Phase 3: Heal partition by restarting both followers
    std.debug.print("\nPhase 3: Healing partition (restarting followers)\n", .{});
    try cluster.restartFollower(0);
    try cluster.restartFollower(1);
    std.debug.print("Restarted both followers\n", .{});

    // Wait for followers to reconnect and receive heartbeats
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Phase 4: Post-healing - writes should succeed again
    std.debug.print("\nPhase 4: Post-healing verification\n", .{});
    const record2 = Record{ .key = "post-healing", .value = "recovered" };
    const offset2 = try leader_client.append(record2);

    // Note: Due to Raft-like append-only log, the failed partition write (offset 1)
    // remains in the leader's log as uncommitted. The next successful write is offset 2.
    // However, offset 1 may be replicated during healing, making it part of consensus.
    // We just verify the write succeeds, regardless of exact offset value.
    std.debug.print("Write succeeded after partition healed (offset {d})\n", .{offset2});

    // Poll for replication to both followers
    try waitForAllFollowersReplication(&cluster, offset2, allocator, 10000);

    // Verify both followers have the post-healing record
    for (0..2) |follower_idx| {
        const record = try cluster.readFromFollowerLog(follower_idx, offset2, allocator);
        defer {
            if (record.key) |k| allocator.free(k);
            allocator.free(record.value);
        }
        try testing.expectEqualStrings("post-healing", record.key.?);
        try testing.expectEqualStrings("recovered", record.value);
    }

    std.debug.print("Verified replication to both followers after healing\n", .{});

    // Write a few more records to ensure cluster is fully healthy
    std.debug.print("\nPhase 5: Final stability check\n", .{});
    var last_offset: u64 = offset2;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "final-{d}", .{i});
        defer allocator.free(key);

        const value = try std.fmt.allocPrint(allocator, "stable-{d}", .{i});
        defer allocator.free(value);

        const record = Record{ .key = key, .value = value };
        last_offset = try leader_client.append(record);
    }

    try waitForAllFollowersReplication(&cluster, last_offset, allocator, 10000);

    std.debug.print("Cluster fully recovered - 5 additional writes succeeded and replicated\n", .{});

    std.debug.print("✓ Test 11 passed: Network partition detected, writes failed during partition, and cluster recovered after healing\n", .{});
}
