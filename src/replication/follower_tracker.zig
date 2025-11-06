/// Follower Offset Tracker
///
/// Tracks the replication progress of each follower in the cluster.
/// The leader uses this to:
/// - Determine which followers are in-sync (ISR)
/// - Detect gaps that require catch-up
/// - Monitor follower health via heartbeat timestamps
const std = @import("std");

/// State for a single follower
pub const FollowerState = struct {
    id: u32,
    last_offset: u64,              // Follower's last known offset
    last_heartbeat_ms: i64,        // Last heartbeat timestamp
    in_sync: bool,                 // Is follower within acceptable lag?

    pub fn init(id: u32, initial_offset: u64, now_ms: i64) FollowerState {
        return FollowerState{
            .id = id,
            .last_offset = initial_offset,
            .last_heartbeat_ms = now_ms,
            .in_sync = true,  // Start optimistic
        };
    }

    /// Update follower state after receiving a heartbeat or replication response
    pub fn updateOffset(self: *FollowerState, offset: u64, now_ms: i64) void {
        self.last_offset = offset;
        self.last_heartbeat_ms = now_ms;
    }

    /// Calculate offset lag compared to leader
    pub fn calculateLag(self: *const FollowerState, leader_offset: u64) u64 {
        if (leader_offset >= self.last_offset) {
            return leader_offset - self.last_offset;
        }
        // Follower ahead of leader (should not happen in normal operation)
        return 0;
    }

    /// Check if follower has exceeded heartbeat timeout
    pub fn isHealthy(self: *const FollowerState, now_ms: i64, timeout_ms: u64) bool {
        const elapsed = now_ms - self.last_heartbeat_ms;
        return elapsed < @as(i64, @intCast(timeout_ms));
    }
};

/// Tracks all followers in the cluster
pub const FollowerTracker = struct {
    followers: std.AutoHashMap(u32, FollowerState),
    allocator: std.mem.Allocator,
    max_lag_entries: u64,      // Max offset difference before marking out-of-sync

    pub fn init(allocator: std.mem.Allocator, max_lag_entries: u64) FollowerTracker {
        return FollowerTracker{
            .followers = std.AutoHashMap(u32, FollowerState).init(allocator),
            .allocator = allocator,
            .max_lag_entries = max_lag_entries,
        };
    }

    pub fn deinit(self: *FollowerTracker) void {
        self.followers.deinit();
    }

    /// Add a follower to track
    pub fn addFollower(self: *FollowerTracker, id: u32, initial_offset: u64, now_ms: i64) !void {
        const state = FollowerState.init(id, initial_offset, now_ms);
        try self.followers.put(id, state);
    }

    /// Remove a follower from tracking
    pub fn removeFollower(self: *FollowerTracker, id: u32) !void {
        if (!self.followers.remove(id)) {
            return error.FollowerNotFound;
        }
    }

    /// Update follower's offset after replication or heartbeat
    pub fn updateFollower(
        self: *FollowerTracker,
        id: u32,
        offset: u64,
        now_ms: i64,
    ) !void {
        var entry = self.followers.getPtr(id) orelse return error.FollowerNotFound;
        entry.updateOffset(offset, now_ms);
    }

    /// Update in-sync status for all followers based on current leader offset
    pub fn updateInSyncStatus(self: *FollowerTracker, leader_offset: u64) void {
        var it = self.followers.valueIterator();
        while (it.next()) |follower| {
            const lag = follower.calculateLag(leader_offset);
            follower.in_sync = lag <= self.max_lag_entries;
        }
    }

    /// Mark follower as unhealthy (failed heartbeat or replication)
    pub fn markUnhealthy(self: *FollowerTracker, id: u32) !void {
        var entry = self.followers.getPtr(id) orelse return error.FollowerNotFound;
        entry.in_sync = false;
    }

    /// Get follower state
    pub fn getFollower(self: *FollowerTracker, id: u32) ?FollowerState {
        return self.followers.get(id);
    }

    /// Count how many followers are in-sync
    pub fn countInSync(self: *const FollowerTracker) u32 {
        var in_sync_count: u32 = 0;
        var it = self.followers.valueIterator();
        while (it.next()) |follower| {
            if (follower.in_sync) {
                in_sync_count += 1;
            }
        }
        return in_sync_count;
    }

    /// Count how many followers are healthy (received heartbeat recently)
    pub fn countHealthy(self: *const FollowerTracker, now_ms: i64, timeout_ms: u64) u32 {
        var healthy_count: u32 = 0;
        var it = self.followers.valueIterator();
        while (it.next()) |follower| {
            if (follower.isHealthy(now_ms, timeout_ms)) {
                healthy_count += 1;
            }
        }
        return healthy_count;
    }

    /// Get all follower IDs that are out of sync
    pub fn getOutOfSyncFollowers(self: *const FollowerTracker, allocator: std.mem.Allocator) ![]u32 {
        var out_of_sync: std.ArrayList(u32) = .{};
        errdefer out_of_sync.deinit(allocator);

        var it = self.followers.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.in_sync) {
                try out_of_sync.append(allocator, entry.key_ptr.*);
            }
        }

        return out_of_sync.toOwnedSlice(allocator);
    }

    /// Get total number of followers
    pub fn count(self: *const FollowerTracker) usize {
        return self.followers.count();
    }

    /// Check if we have enough in-sync replicas to form quorum
    /// Quorum includes leader + in-sync followers
    pub fn hasQuorum(self: *const FollowerTracker, quorum_size: u32) bool {
        const isr_count = self.countInSync();
        const total_isr = isr_count + 1; // +1 for leader
        return total_isr >= quorum_size;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "FollowerState: init" {
    const state = FollowerState.init(1, 100, 1000);
    try std.testing.expectEqual(@as(u32, 1), state.id);
    try std.testing.expectEqual(@as(u64, 100), state.last_offset);
    try std.testing.expectEqual(@as(i64, 1000), state.last_heartbeat_ms);
    try std.testing.expectEqual(true, state.in_sync);
}

test "FollowerState: updateOffset" {
    var state = FollowerState.init(1, 100, 1000);
    state.updateOffset(150, 2000);

    try std.testing.expectEqual(@as(u64, 150), state.last_offset);
    try std.testing.expectEqual(@as(i64, 2000), state.last_heartbeat_ms);
}

test "FollowerState: calculateLag" {
    const state = FollowerState.init(1, 100, 1000);

    // Leader at 150, follower at 100 -> lag = 50
    try std.testing.expectEqual(@as(u64, 50), state.calculateLag(150));

    // Leader at 100, follower at 100 -> lag = 0
    try std.testing.expectEqual(@as(u64, 0), state.calculateLag(100));

    // Follower ahead (shouldn't happen normally) -> lag = 0
    try std.testing.expectEqual(@as(u64, 0), state.calculateLag(50));
}

test "FollowerState: isHealthy" {
    const state = FollowerState.init(1, 100, 1000);

    // Within timeout (1500ms - 1000ms = 500ms < 1000ms timeout)
    try std.testing.expectEqual(true, state.isHealthy(1500, 1000));

    // At timeout boundary
    try std.testing.expectEqual(false, state.isHealthy(2000, 1000));

    // Exceeded timeout
    try std.testing.expectEqual(false, state.isHealthy(2500, 1000));
}

test "FollowerTracker: init and deinit" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try std.testing.expectEqual(@as(usize, 0), tracker.count());
}

test "FollowerTracker: addFollower and getFollower" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 50, 1000);
    try tracker.addFollower(2, 60, 1000);

    try std.testing.expectEqual(@as(usize, 2), tracker.count());

    const f1 = tracker.getFollower(1);
    try std.testing.expect(f1 != null);
    try std.testing.expectEqual(@as(u64, 50), f1.?.last_offset);

    const f2 = tracker.getFollower(2);
    try std.testing.expect(f2 != null);
    try std.testing.expectEqual(@as(u64, 60), f2.?.last_offset);

    const f3 = tracker.getFollower(999);
    try std.testing.expect(f3 == null);
}

test "FollowerTracker: removeFollower" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 50, 1000);
    try tracker.addFollower(2, 60, 1000);

    try tracker.removeFollower(1);
    try std.testing.expectEqual(@as(usize, 1), tracker.count());
    try std.testing.expect(tracker.getFollower(1) == null);
    try std.testing.expect(tracker.getFollower(2) != null);
}

test "FollowerTracker: removeFollower non-existent returns error" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try std.testing.expectError(error.FollowerNotFound, tracker.removeFollower(999));
}

test "FollowerTracker: updateFollower" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 50, 1000);

    try tracker.updateFollower(1, 75, 2000);

    const f1 = tracker.getFollower(1);
    try std.testing.expectEqual(@as(u64, 75), f1.?.last_offset);
    try std.testing.expectEqual(@as(i64, 2000), f1.?.last_heartbeat_ms);
}

test "FollowerTracker: updateInSyncStatus" {
    var tracker = FollowerTracker.init(std.testing.allocator, 50); // max lag = 50
    defer tracker.deinit();

    try tracker.addFollower(1, 100, 1000); // lag = 0 from leader
    try tracker.addFollower(2, 80, 1000);  // lag = 20 from leader
    try tracker.addFollower(3, 40, 1000);  // lag = 60 from leader (out of sync)

    const leader_offset: u64 = 100;
    tracker.updateInSyncStatus(leader_offset);

    const f1 = tracker.getFollower(1);
    try std.testing.expectEqual(true, f1.?.in_sync); // lag = 0

    const f2 = tracker.getFollower(2);
    try std.testing.expectEqual(true, f2.?.in_sync); // lag = 20

    const f3 = tracker.getFollower(3);
    try std.testing.expectEqual(false, f3.?.in_sync); // lag = 60 > 50
}

test "FollowerTracker: markUnhealthy" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 100, 1000);
    try tracker.addFollower(2, 100, 1000);

    try tracker.markUnhealthy(1);

    const f1 = tracker.getFollower(1);
    try std.testing.expectEqual(false, f1.?.in_sync);

    const f2 = tracker.getFollower(2);
    try std.testing.expectEqual(true, f2.?.in_sync);
}

test "FollowerTracker: countInSync" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 100, 1000);
    try tracker.addFollower(2, 100, 1000);
    try tracker.addFollower(3, 100, 1000);

    try std.testing.expectEqual(@as(u32, 3), tracker.countInSync());

    try tracker.markUnhealthy(1);
    try std.testing.expectEqual(@as(u32, 2), tracker.countInSync());

    try tracker.markUnhealthy(2);
    try std.testing.expectEqual(@as(u32, 1), tracker.countInSync());
}

test "FollowerTracker: countHealthy" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 100, 1000);
    try tracker.addFollower(2, 100, 2000);
    try tracker.addFollower(3, 100, 3000);

    // At time 3500ms with 1000ms timeout:
    // - Follower 1: 3500 - 1000 = 2500ms elapsed (unhealthy)
    // - Follower 2: 3500 - 2000 = 1500ms elapsed (unhealthy)
    // - Follower 3: 3500 - 3000 = 500ms elapsed (healthy)
    try std.testing.expectEqual(@as(u32, 1), tracker.countHealthy(3500, 1000));
}

test "FollowerTracker: getOutOfSyncFollowers" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    try tracker.addFollower(1, 100, 1000);
    try tracker.addFollower(2, 100, 1000);
    try tracker.addFollower(3, 100, 1000);

    try tracker.markUnhealthy(1);
    try tracker.markUnhealthy(3);

    const out_of_sync = try tracker.getOutOfSyncFollowers(std.testing.allocator);
    defer std.testing.allocator.free(out_of_sync);

    try std.testing.expectEqual(@as(usize, 2), out_of_sync.len);

    // Verify the IDs (order may vary due to HashMap)
    var found_1 = false;
    var found_3 = false;
    for (out_of_sync) |id| {
        if (id == 1) found_1 = true;
        if (id == 3) found_3 = true;
    }
    try std.testing.expect(found_1);
    try std.testing.expect(found_3);
}

test "FollowerTracker: hasQuorum" {
    var tracker = FollowerTracker.init(std.testing.allocator, 100);
    defer tracker.deinit();

    // 3-node cluster: need quorum of 2 (majority)
    // Leader + 2 followers
    try tracker.addFollower(1, 100, 1000);
    try tracker.addFollower(2, 100, 1000);

    // All in sync: 2 followers + 1 leader = 3 >= 2 (quorum satisfied)
    try std.testing.expectEqual(true, tracker.hasQuorum(2));

    // One follower out of sync: 1 follower + 1 leader = 2 >= 2 (still quorum)
    try tracker.markUnhealthy(1);
    try std.testing.expectEqual(true, tracker.hasQuorum(2));

    // Both followers out of sync: 0 followers + 1 leader = 1 < 2 (no quorum)
    try tracker.markUnhealthy(2);
    try std.testing.expectEqual(false, tracker.hasQuorum(2));
}
