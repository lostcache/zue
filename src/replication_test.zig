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

        // Clean up old tmp_path
        self.allocator.free(follower.tmp_path);

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
// Integration Tests
// ============================================================================

test "replication: basic replication (1 leader + 2 followers)" {
    const allocator = testing.allocator;

    var cluster = try ReplicationCluster.init(allocator, 2);
    defer cluster.deinit();

    // Give the cluster time to establish connections and run heartbeats
    // Leader connects on startup (may fail), then heartbeats every 2s bring followers in-sync
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write a record to the leader
    const record = Record{ .key = "test-key", .value = "test-value" };
    const offset = try leader_client.append(record);
    try testing.expectEqual(@as(u64, 0), offset);

    // Give replication time to complete
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Verify the record was replicated to both followers by reading from their logs
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

    // Give the cluster time to establish connections and run heartbeats
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Kill one follower
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0\n", .{});

    // Give time for leader to detect failure (via heartbeat)
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write should still succeed (quorum = 2, we have leader + 1 follower)
    const record = Record{ .key = "quorum-test", .value = "one-down" };
    const offset = try leader_client.append(record);
    try testing.expectEqual(@as(u64, 0), offset);

    std.debug.print("Write succeeded with one follower down\n", .{});

    // Verify the record was replicated to the surviving follower
    std.Thread.sleep(500 * std.time.ns_per_ms);

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

    // Give the cluster time to establish connections
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Kill both followers
    try cluster.killFollower(0);
    try cluster.killFollower(1);
    std.debug.print("Killed both followers\n", .{});

    // Give time for leader to detect failure (via heartbeat)
    std.Thread.sleep(3 * std.time.ns_per_s);

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

    // Give the cluster time to establish connections
    std.Thread.sleep(5 * std.time.ns_per_s);

    // Connect to leader
    var leader_client = try cluster.getLeaderClient();
    defer leader_client.disconnect();

    // Write first record (all nodes in sync)
    const record1 = Record{ .key = "before", .value = "stop" };
    const offset1 = try leader_client.append(record1);
    try testing.expectEqual(@as(u64, 0), offset1);
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Kill follower 0
    try cluster.killFollower(0);
    std.debug.print("Killed follower 0\n", .{});

    // Wait for leader to detect failure
    std.Thread.sleep(3 * std.time.ns_per_s);

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

    // Wait for follower to catch up (timer-based catch-up is triggered every 2s)
    std.Thread.sleep(5 * std.time.ns_per_s);

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
