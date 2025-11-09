const std = @import("std");

pub const NodeRole = enum {
    leader,
    follower,
};

pub const NodeConfig = struct {
    id: u32,
    address: []const u8,
    port: u16,
    role: NodeRole,
};

pub const ReplicationConfig = struct {
    quorum_size: u32,
    timeout_ms: u64,
    max_lag_entries: u64,
    heartbeat_interval_ms: u64,
    repair_batch_size: u32 = 100, // Max entries to send per repair tick
    repair_max_retries: u32 = 100, // Max probe attempts when finding common prefix
    inline_catchup_threshold: u32 = 10, // Max entries to stream inline during write (prevents head-of-line blocking for large lags)
};

pub const ClusterConfig = struct {
    nodes: []NodeConfig,
    replication: ReplicationConfig,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ClusterConfig) void {
        for (self.nodes) |node| {
            self.allocator.free(node.address);
        }
        self.allocator.free(self.nodes);
    }

    /// Validate cluster configuration
    /// Returns error if configuration is invalid
    pub fn validate(self: *const ClusterConfig) !void {
        if (self.nodes.len == 0) {
            return error.NoNodesConfigured;
        }

        // Check quorum_size > node_count / 2
        const min_quorum = (self.nodes.len / 2) + 1;
        if (self.replication.quorum_size < min_quorum) {
            return error.InsufficientQuorum;
        }

        if (self.replication.quorum_size > self.nodes.len) {
            return error.QuorumExceedsNodeCount;
        }

        // Check exactly one leader
        var leader_count: u32 = 0;
        for (self.nodes) |node| {
            if (node.role == .leader) {
                leader_count += 1;
            }
        }

        if (leader_count == 0) {
            return error.NoLeaderConfigured;
        }

        if (leader_count > 1) {
            return error.MultipleLeadersConfigured;
        }

        // Check unique node IDs
        for (self.nodes, 0..) |node1, i| {
            for (self.nodes[i + 1 ..]) |node2| {
                if (node1.id == node2.id) {
                    return error.DuplicateNodeId;
                }
            }
        }
    }

    /// Get the leader node config
    pub fn getLeader(self: *const ClusterConfig) ?NodeConfig {
        for (self.nodes) |node| {
            if (node.role == .leader) {
                return node;
            }
        }
        return null;
    }

    /// Iterator for followers - no allocation
    pub const FollowerIterator = struct {
        nodes: []const NodeConfig,
        index: usize = 0,

        pub fn next(self: *FollowerIterator) ?NodeConfig {
            while (self.index < self.nodes.len) {
                const node = self.nodes[self.index];
                self.index += 1;
                if (node.role == .follower) {
                    return node;
                }
            }
            return null;
        }
    };

    /// Get follower iterator (zero allocation)
    pub fn followerIterator(self: *const ClusterConfig) FollowerIterator {
        return FollowerIterator{ .nodes = self.nodes };
    }

    /// Count followers
    pub fn followerCount(self: *const ClusterConfig) usize {
        var count: usize = 0;
        for (self.nodes) |node| {
            if (node.role == .follower) count += 1;
        }
        return count;
    }
};

/// Parse and validate config - returns only valid configs
/// Parse cluster configuration from a file
pub fn parseFile(path: []const u8, allocator: std.mem.Allocator) !ClusterConfig {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const max_size = 1024 * 1024; // 1MB max config file
    const contents = try file.readToEndAlloc(allocator, max_size);
    defer allocator.free(contents);

    return try parseConfig(allocator, contents);
}

/// Format: whitespace-tolerant line-based
/// node <id> <address> <port> <role>
/// quorum <size>
/// timeout <ms>
/// max_lag <entries>
/// heartbeat <ms>
pub fn parseConfig(allocator: std.mem.Allocator, text: []const u8) !ClusterConfig {
    var nodes: std.ArrayList(NodeConfig) = .{};
    errdefer {
        for (nodes.items) |node| {
            allocator.free(node.address);
        }
        nodes.deinit(allocator);
    }

    var quorum_size: ?u32 = null;
    var timeout_ms: ?u64 = null;
    var max_lag_entries: ?u64 = null;
    var heartbeat_interval_ms: ?u64 = null;

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;

        // Use tokenizeAny for whitespace tolerance
        var parts = std.mem.tokenizeAny(u8, trimmed, " \t");
        const cmd = parts.next() orelse continue;

        if (std.mem.eql(u8, cmd, "node")) {
            const id_str = parts.next() orelse return error.InvalidNodeConfig;
            const address = parts.next() orelse return error.InvalidNodeConfig;
            const port_str = parts.next() orelse return error.InvalidNodeConfig;
            const role_str = parts.next() orelse return error.InvalidNodeConfig;

            const id = try std.fmt.parseInt(u32, id_str, 10);
            const port = try std.fmt.parseInt(u16, port_str, 10);

            const role = if (std.mem.eql(u8, role_str, "leader"))
                NodeRole.leader
            else if (std.mem.eql(u8, role_str, "follower"))
                NodeRole.follower
            else
                return error.InvalidNodeRole;

            const address_copy = try allocator.dupe(u8, address);
            errdefer allocator.free(address_copy);

            try nodes.append(allocator, NodeConfig{
                .id = id,
                .address = address_copy,
                .port = port,
                .role = role,
            });
        } else if (std.mem.eql(u8, cmd, "quorum")) {
            const val_str = parts.next() orelse return error.InvalidQuorumConfig;
            quorum_size = try std.fmt.parseInt(u32, val_str, 10);
        } else if (std.mem.eql(u8, cmd, "timeout")) {
            const val_str = parts.next() orelse return error.InvalidTimeoutConfig;
            timeout_ms = try std.fmt.parseInt(u64, val_str, 10);
        } else if (std.mem.eql(u8, cmd, "max_lag")) {
            const val_str = parts.next() orelse return error.InvalidMaxLagConfig;
            max_lag_entries = try std.fmt.parseInt(u64, val_str, 10);
        } else if (std.mem.eql(u8, cmd, "heartbeat")) {
            const val_str = parts.next() orelse return error.InvalidHeartbeatConfig;
            heartbeat_interval_ms = try std.fmt.parseInt(u64, val_str, 10);
        }
    }

    // Validate all required fields present
    if (nodes.items.len == 0) return error.NoNodesConfigured;
    if (quorum_size == null) return error.MissingQuorumConfig;
    if (timeout_ms == null) return error.MissingTimeoutConfig;
    if (max_lag_entries == null) return error.MissingMaxLagConfig;
    if (heartbeat_interval_ms == null) return error.MissingHeartbeatConfig;

    var config = ClusterConfig{
        .nodes = try nodes.toOwnedSlice(allocator),
        .replication = ReplicationConfig{
            .quorum_size = quorum_size.?,
            .timeout_ms = timeout_ms.?,
            .max_lag_entries = max_lag_entries.?,
            .heartbeat_interval_ms = heartbeat_interval_ms.?,
        },
        .allocator = allocator,
    };

    // Validate before returning - never return invalid config
    config.validate() catch |err| {
        config.deinit();
        return err;
    };

    return config;
}

// ============================================================================
// Unit Tests
// ============================================================================

test "ClusterConfig: validate - valid config" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .follower },
        .{ .id = 3, .address = "127.0.0.1", .port = 9003, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 2,
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    try config.validate();
}

test "ClusterConfig: validate - no leader" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .follower },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 2,
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    try std.testing.expectError(error.NoLeaderConfigured, config.validate());
}

test "ClusterConfig: validate - multiple leaders" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .leader },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 2,
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    try std.testing.expectError(error.MultipleLeadersConfigured, config.validate());
}

test "ClusterConfig: validate - insufficient quorum" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .follower },
        .{ .id = 3, .address = "127.0.0.1", .port = 9003, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 1, // Too small for 3 nodes
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    try std.testing.expectError(error.InsufficientQuorum, config.validate());
}

test "ClusterConfig: validate - quorum exceeds node count" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 5, // More than node count
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    try std.testing.expectError(error.QuorumExceedsNodeCount, config.validate());
}

test "ClusterConfig: validate - duplicate node IDs" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 1, .address = "127.0.0.1", .port = 9002, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 2,
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    try std.testing.expectError(error.DuplicateNodeId, config.validate());
}

test "ClusterConfig: getLeader" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 2,
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    const leader = config.getLeader();
    try std.testing.expect(leader != null);
    try std.testing.expectEqual(@as(u32, 1), leader.?.id);
    try std.testing.expectEqual(NodeRole.leader, leader.?.role);
}

test "ClusterConfig: follower iteration" {
    var nodes = [_]NodeConfig{
        .{ .id = 1, .address = "127.0.0.1", .port = 9001, .role = .leader },
        .{ .id = 2, .address = "127.0.0.1", .port = 9002, .role = .follower },
        .{ .id = 3, .address = "127.0.0.1", .port = 9003, .role = .follower },
    };

    const config = ClusterConfig{
        .nodes = &nodes,
        .replication = ReplicationConfig{
            .quorum_size = 2,
            .timeout_ms = 1000,
            .max_lag_entries = 100,
            .heartbeat_interval_ms = 1000,
        },
        .allocator = std.testing.allocator,
    };

    // Zero-allocation iteration
    var iter = config.followerIterator();
    const f1 = iter.next();
    const f2 = iter.next();
    const f3 = iter.next();

    try std.testing.expect(f1 != null);
    try std.testing.expect(f2 != null);
    try std.testing.expect(f3 == null);
    try std.testing.expectEqual(@as(u32, 2), f1.?.id);
    try std.testing.expectEqual(@as(u32, 3), f2.?.id);

    try std.testing.expectEqual(@as(usize, 2), config.followerCount());
}

test "parseConfig: valid config" {
    const config_text =
        \\node 1 127.0.0.1 9001 leader
        \\node 2 127.0.0.1 9002 follower
        \\node 3 127.0.0.1 9003 follower
        \\quorum 2
        \\timeout 1000
        \\max_lag 100
        \\heartbeat 1000
    ;

    var config = try parseConfig(std.testing.allocator, config_text);
    defer config.deinit();

    try std.testing.expectEqual(@as(usize, 3), config.nodes.len);
    try std.testing.expectEqual(@as(u32, 2), config.replication.quorum_size);
    try std.testing.expectEqual(@as(u64, 1000), config.replication.timeout_ms);
    try std.testing.expectEqual(@as(u64, 100), config.replication.max_lag_entries);
    try std.testing.expectEqual(@as(u64, 1000), config.replication.heartbeat_interval_ms);
    // No need to call validate() - parseConfig() already did it
}

test "parseConfig: whitespace tolerant" {
    // Test with tabs and multiple spaces
    const config_text = "# Cluster configuration\nnode\t1   127.0.0.1\t9001\tleader\n\n# Followers\nnode  2  127.0.0.1  9002  follower\n\nquorum   2\ntimeout 1000\nmax_lag 100\nheartbeat 1000";

    var config = try parseConfig(std.testing.allocator, config_text);
    defer config.deinit();

    try std.testing.expectEqual(@as(usize, 2), config.nodes.len);
}

test "parseConfig: missing quorum" {
    const config_text =
        \\node 1 127.0.0.1 9001 leader
        \\timeout 1000
        \\max_lag 100
        \\heartbeat 1000
    ;

    try std.testing.expectError(error.MissingQuorumConfig, parseConfig(std.testing.allocator, config_text));
}

test "parseConfig: invalid config caught during parse" {
    // Multiple leaders - should fail during parseConfig (not separate validate call)
    const config_text =
        \\node 1 127.0.0.1 9001 leader
        \\node 2 127.0.0.1 9002 leader
        \\quorum 2
        \\timeout 1000
        \\max_lag 100
        \\heartbeat 1000
    ;

    try std.testing.expectError(error.MultipleLeadersConfigured, parseConfig(std.testing.allocator, config_text));
}
