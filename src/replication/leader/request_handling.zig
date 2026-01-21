const std = @import("std");
const Leader = @import("main.zig").Leader;
const FollowerConnection = @import("follower_connection.zig").FollowerConnection;
const protocol = @import("../../network/protocol.zig");
const Record = @import("../../log/record.zig").Record;
const strategy = @import("replication_strategy.zig");
const quorum = @import("quorum.zig");
const Allocator = std.mem.Allocator;

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;

/// Prepared replication request with allocations that need cleanup
pub const PreparedRequest = struct {
    request: ReplicateRequest,
    records: []Record,
    entries: []protocol.ReplicatedEntry,

    /// Free all allocations
    pub fn deinit(self: PreparedRequest, allocator: Allocator) void {
        for (self.records) |rec| {
            if (rec.key) |k| allocator.free(k);
            allocator.free(rec.value);
        }
        allocator.free(self.records);
        allocator.free(self.entries);
    }
};

/// Prepare inline catch-up request for a follower
/// Reads entries from log and builds ReplicateRequest
/// Caller must call deinit() on returned PreparedRequest to free memory
pub fn prepareInlineCatchupRequest(
    self: *Leader,
    follower: *FollowerConnection,
    current_offset: u64,
) !PreparedRequest {
    // Calculate how many entries to send
    const lag = if (follower.next_index <= current_offset)
        current_offset - follower.next_index + 1
    else
        0;

    const count: u32 = @intCast(lag);

    // Read records from log
    const records = try self.log.readRange(follower.next_index, count, self.allocator);
    errdefer {
        for (records) |rec| {
            if (rec.key) |k| self.allocator.free(k);
            self.allocator.free(rec.value);
        }
        self.allocator.free(records);
    }

    // Build entries array
    const entries = try self.allocator.alloc(protocol.ReplicatedEntry, records.len);
    errdefer self.allocator.free(entries);

    for (records, 0..) |rec, i| {
        entries[i] = protocol.ReplicatedEntry{
            .offset = follower.next_index + i,
            .record = rec,
        };
    }

    const request = ReplicateRequest{
        .entries = entries,
        .leader_commit = self.commit_index orelse 0,
    };

    return PreparedRequest{
        .request = request,
        .records = records,
        .entries = entries,
    };
}

/// Process a follower's replication response
/// Updates follower state based on response (match_index, next_index, in_sync, etc.)
pub fn processFollowerResponse(
    self: *Leader,
    follower: *FollowerConnection,
    current_offset: u64,
) !void {
    // Read response
    const resp = try follower.receiveReplicateResponse(self.allocator);

    if (resp.success) {
        // Update Raft state: follower has replicated up to this offset
        follower.match_index = resp.follower_offset;
        follower.next_index = if (resp.follower_offset) |off| off + 1 else 0;
        follower.last_offset = resp.follower_offset orelse 0;
        follower.state = .replicating; // Ensure in normal state

        // Mark as in_sync if lag is acceptable
        const current_lag = if (follower.next_index <= current_offset)
            current_offset - follower.next_index + 1
        else
            0;
        follower.in_sync = (current_lag <= self.max_lag_entries);
    } else if (resp.error_code == .offset_mismatch) {
        // Offset mismatch - use follower's actual offset to jump directly
        follower.match_index = resp.follower_offset;
        follower.next_index = if (resp.follower_offset) |off| off + 1 else 0;
        follower.state = .replicating; // Start streaming from this position
        follower.in_sync = false;
    } else {
        // Other error
        follower.in_sync = false;
    }
}

/// Send replication requests to all followers in parallel
/// Returns list of followers awaiting responses
pub fn sendReplicationRequests(
    self: *Leader,
    current_offset: u64,
) !std.ArrayListUnmanaged(*FollowerConnection) {
    var pending_followers: std.ArrayListUnmanaged(*FollowerConnection) = .{};
    errdefer pending_followers.deinit(self.allocator);

    for (self.followers) |*follower| {
        // Determine strategy for this follower
        const s = strategy.determineFollowerStrategy(self, follower, current_offset);

        switch (s) {
            .send_inline => {
                // Prepare and send catch-up request
                const prepared = prepareInlineCatchupRequest(self, follower, current_offset) catch {
                    follower.in_sync = false;
                    continue;
                };
                defer prepared.deinit(self.allocator);

                // Send request (non-blocking)
                follower.sendReplicateRequestNonBlocking(prepared.request, self.allocator) catch {
                    follower.in_sync = false;
                    continue;
                };

                // Add to pending list
                try pending_followers.append(self.allocator, follower);
            },
            .defer_to_background => {
                // Defer to background repair via tickRepair()
                continue;
            },
            .skip => {
                // Follower is caught up or connection failed
                continue;
            },
        }
    }

    return pending_followers;
}

/// Collect replication responses from pending followers using poll()
/// Processes responses as they arrive, updating follower state
/// Returns when all responses received or timeout occurs
pub fn collectReplicationResponses(
    self: *Leader,
    pending_followers: *std.ArrayListUnmanaged(*FollowerConnection),
    current_offset: u64,
) !void {
    const start_time = std.time.milliTimestamp();

    while (pending_followers.items.len > 0) {
        // Check timeout
        const elapsed = std.time.milliTimestamp() - start_time;
        if (elapsed > self.replication_timeout_ms) {
            break;
        }

        // Prepare poll fds
        var poll_fds = try self.allocator.alloc(std.posix.pollfd, pending_followers.items.len);
        defer self.allocator.free(poll_fds);

        for (pending_followers.items, 0..) |follower, i| {
            poll_fds[i] = .{
                .fd = follower.stream.?.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            };
        }

        // Poll with remaining timeout
        const remaining_timeout: i32 = @intCast(@max(0, @as(i64, @intCast(self.replication_timeout_ms)) - elapsed));
        const ready = std.posix.poll(poll_fds, remaining_timeout) catch {
            break;
        };

        if (ready == 0) continue; // Timeout, try again

        // Process ready followers
        var i: usize = 0;
        while (i < pending_followers.items.len) {
            if (poll_fds[i].revents & std.posix.POLL.IN != 0) {
                const follower = pending_followers.items[i];

                // Process response
                processFollowerResponse(self, follower, current_offset) catch {
                    follower.in_sync = false;
                };

                // Remove from pending list
                _ = pending_followers.swapRemove(i);
            } else {
                i += 1;
            }
        }
    }

    // Mark any remaining non-responsive followers as out-of-sync
    for (pending_followers.items) |follower| {
        follower.in_sync = false;
    }
}

/// Replicate record to followers (PARALLEL using non-blocking I/O)
/// Uses Raft-like replication: entries are never rolled back, only commit_index advances
/// Returns the committed offset on success
pub fn replicate(self: *Leader, record: Record) !u64 {
    // 1. Append to local log
    const offset = try self.log.append(record);

    // 2. Send replication requests to followers in parallel
    var pending_followers = try sendReplicationRequests(self, offset);
    defer pending_followers.deinit(self.allocator);

    // 3. Collect responses from followers
    try collectReplicationResponses(self, &pending_followers, offset);

    // 4. Calculate commit_index based on quorum of match_index values
    const new_commit_index = quorum.calculateCommitIndex(self);

    // 5. Check if our entry is committed
    // calculateCommitIndex() only counts in-sync replicas, so if it returns
    // an index >= our offset, we have quorum
    if (new_commit_index) |commit_idx| {
        if (commit_idx >= offset) {
            self.commit_index = new_commit_index;
            return offset;
        }
    }

    // RAFT PRINCIPLE: Entry stays in log (NO ROLLBACK)
    // commit_index will advance when quorum is reached
    return error.QuorumNotReached;
}
