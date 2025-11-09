/// Leader Implementation for Phase 2 Replication
///
/// Manages replication to followers with:
/// - PARALLEL replication to followers using non-blocking I/O
/// - Quorum-based commit logic (Raft-style)
/// - In-Sync Replica (ISR) tracking
/// - Heartbeat management
/// - Automatic log repair on offset mismatch
const std = @import("std");
const Log = @import("../log/log.zig").Log;
const LogConfig = @import("../log/log_config.zig").LogConfig;
const Record = @import("../log/record.zig").Record;
const protocol = @import("../network/protocol.zig");
const ClusterConfig = @import("../config.zig").ClusterConfig;
const NodeConfig = @import("../config.zig").NodeConfig;

// Compile-time flag for verbose debug logging (disable in production)
const enable_debug_logging = false;

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;
const Allocator = std.mem.Allocator;

/// Follower state machine for async repair process
pub const FollowerState = enum {
    replicating,            // Normal replication, follower is in-sync
    needs_repair,           // Detected offset_mismatch or lag, repair needed
    repairing_probe_sent,   // Sent probe entry to find common prefix, awaiting response
    repairing_stream_sent,  // Sent batch of catch-up entries, awaiting response
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
    next_index: u64,   // Next offset to send to this follower

    // Async repair state
    repair_probe_offset: ?u64, // Offset currently being probed during repair (null if not probing)
    repair_retry_count: u32,   // Number of probe attempts made during current repair

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
            .repair_probe_offset = null,
            .repair_retry_count = 0,
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
    commit_index: ?u64, // null means no entries committed yet
    repair_batch_size: u32, // Max entries per repair tick
    repair_max_retries: u32, // Max probe retries during repair
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
            .repair_max_retries = config.replication.repair_max_retries,
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

    /// Helper: Check if a socket has data ready to read (non-blocking)
    fn isSocketReady(stream: std.net.Stream, timeout_ms: i32) bool {
        var poll_fds = [_]std.posix.pollfd{.{
            .fd = stream.handle,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};

        const ready = std.posix.poll(&poll_fds, timeout_ms) catch return false;
        return ready > 0 and (poll_fds[0].revents & std.posix.POLL.IN) != 0;
    }

    /// Replicate record to followers (PARALLEL using non-blocking I/O)
    /// Uses Raft-like replication: entries are never rolled back, only commit_index advances
    /// Returns the committed offset on success
    pub fn replicate(self: *Leader, record: Record) !u64 {
        // 1. Append to local log
        const offset = try self.log.append(record);

        // 2. Send to followers IN PARALLEL using non-blocking I/O
        // Track which followers we're waiting for
        var pending_followers: std.ArrayList(*FollowerConnection) = .{};
        defer pending_followers.deinit(self.allocator);

        // Phase 1: Send all requests in parallel (non-blocking)
        // For each follower, decide whether to:
        // - Send only newest entry (if caught up)
        // - Stream small catch-up batch inline (if slightly behind, within threshold)
        // - Defer to background repair (if far behind, exceeds threshold)
        for (self.followers) |*follower| {
            // Hybrid approach: If follower is out-of-sync, try to reconnect and assess lag
            // before deciding whether to repair inline or defer to background
            if (!follower.in_sync) {
                // Try to reconnect (follower might have restarted)
                if (follower.stream == null) {
                    follower.connect() catch {
                        follower.state = .needs_repair;
                        continue;
                    };
                }

                // Successfully connected (or already connected), calculate lag
                const lag = if (follower.next_index <= offset)
                    offset - follower.next_index + 1
                else
                    0;

                // If lag is within inline threshold, try to catch up during this write
                // This optimizes for the common case where followers are only slightly behind
                if (lag > self.inline_catchup_threshold) {
                    // Too far behind, defer to background repair
                    follower.state = .needs_repair;
                    continue;
                }
                // Fall through to normal inline catch-up logic below
            }

            // At this point, follower is either:
            // - in_sync (normal case)
            // - out_of_sync but within inline catch-up threshold (optimistic recovery)

            // Calculate how many entries follower is behind
            const lag = if (follower.next_index <= offset)
                offset - follower.next_index + 1
            else
                0;

            // Decide on strategy based on lag
            if (lag <= self.inline_catchup_threshold and lag > 0) {
                // Unified path: send catch-up batch inline (includes hot path where lag == 1)
                // Use labeled block with errdefer to handle cleanup on any failure
                follower_scope: {
                    const count: u32 = @intCast(lag);
                    const records = self.log.readRange(follower.next_index, count, self.allocator) catch {
                        follower.in_sync = false;
                        break :follower_scope;
                    };

                    // Track allocations for proper cleanup
                    var entries_to_send: []protocol.ReplicatedEntry = undefined;
                    var entries_allocated = false;

                    // CRITICAL: errdefer ensures cleanup on ANY error (including append failure)
                    errdefer {
                        // Always clean up records
                        for (records) |rec| {
                            if (rec.key) |k| self.allocator.free(k);
                            self.allocator.free(rec.value);
                        }
                        self.allocator.free(records);

                        // Clean up entries if allocated
                        if (entries_allocated) {
                            self.allocator.free(entries_to_send);
                        }
                    }

                    entries_to_send = self.allocator.alloc(protocol.ReplicatedEntry, records.len) catch {
                        follower.in_sync = false;
                        break :follower_scope;
                    };
                    entries_allocated = true;

                    for (records, 0..) |rec, i| {
                        entries_to_send[i] = protocol.ReplicatedEntry{
                            .offset = follower.next_index + i,
                            .record = rec,
                        };
                    }

                    const req = ReplicateRequest{
                        .entries = entries_to_send,
                        .leader_commit = self.commit_index orelse 0,
                    };

                    self.sendReplicateRequestNonBlocking(follower, req) catch {
                        follower.in_sync = false;
                        break :follower_scope;
                    };

                    // This can fail with OOM - errdefer will handle cleanup
                    try pending_followers.append(self.allocator, follower);

                    // Success path: clean up allocations manually
                    self.allocator.free(entries_to_send);
                    for (records) |rec| {
                        if (rec.key) |k| self.allocator.free(k);
                        self.allocator.free(rec.value);
                    }
                    self.allocator.free(records);
                }
            } else if (lag > self.inline_catchup_threshold) {
                // Follower is far behind - defer to background repair to avoid blocking
                follower.state = .needs_repair;
                continue;
            }
        }

        // Phase 2: Collect responses using poll()
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

            // Collect responses from ready followers
            var i: usize = 0;
            while (i < pending_followers.items.len) {
                if (poll_fds[i].revents & std.posix.POLL.IN != 0) {
                    const follower = pending_followers.items[i];

                    // Read response
                    const resp = self.receiveReplicateResponse(follower) catch {
                        follower.in_sync = false;
                        _ = pending_followers.swapRemove(i);
                        continue;
                    };

                    if (resp.success) {
                        // Update Raft state: follower has replicated up to this offset
                        follower.match_index = resp.follower_offset;
                        follower.next_index = if (resp.follower_offset) |off| off + 1 else 0;
                        follower.last_offset = resp.follower_offset orelse 0;
                        follower.state = .replicating; // Ensure in normal state

                        // Mark as in_sync if lag is acceptable
                        // Recalculate lag based on current leader offset
                        const current_lag = if (follower.next_index <= offset)
                            offset - follower.next_index + 1
                        else
                            0;
                        follower.in_sync = (current_lag <= self.max_lag_entries);
                    } else if (resp.error_code == .offset_mismatch) {
                        // Offset mismatch detected - mark follower as needing repair
                        // Background repair process will handle this
                        follower.state = .needs_repair;
                        follower.in_sync = false;
                    } else {
                        follower.in_sync = false;
                    }

                    _ = pending_followers.swapRemove(i);
                } else {
                    i += 1;
                }
            }
        }

        // 3. Calculate commit_index based on quorum of match_index values
        const new_commit_index = self.calculateCommitIndex();

        // 4. Check if our entry is committed
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

    /// Send replication request without waiting for response (non-blocking)
    fn sendReplicateRequestNonBlocking(
        self: *Leader,
        follower: *FollowerConnection,
        req: ReplicateRequest,
    ) !void {
        if (follower.stream == null) {
            return error.NotConnected;
        }

        // Serialize request
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .replicate_request = req,
        };

        try protocol.serializeMessage(msg_writer, request, self.allocator);
        const msg_bytes = msg_stream.getWritten();

        // Non-blocking write
        _ = try std.posix.write(follower.stream.?.handle, msg_bytes);
    }

    /// Receive replication response (blocking read, but only when data is ready)
    fn receiveReplicateResponse(
        self: *Leader,
        follower: *FollowerConnection,
    ) !ReplicateResponse {
        if (follower.stream == null) {
            return error.NotConnected;
        }

        var response_buffer: [65536]u8 = undefined;
        const message_bytes = try protocol.readCompleteMessage(follower.stream.?.handle, &response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, self.allocator);

        return switch (response) {
            .replicate_response => |resp| resp,
            else => error.UnexpectedResponse,
        };
    }

    /// Calculate commit_index based on quorum of match_index values (Raft algorithm)
    /// Returns the highest offset that has been replicated on a quorum of replicas
    fn calculateCommitIndex(self: *const Leader) ?u64 {
        // Special case: empty log
        if (self.log.next_offset == 0) {
            return null; // No entries to commit
        }

        // Collect match_index values for in-sync replicas only (including leader)
        const max_nodes = self.followers.len + 1; // +1 for leader
        var match_indices = self.allocator.alloc(u64, max_nodes) catch {
            // Fallback: return current commit_index on allocation failure
            return self.commit_index;
        };
        defer self.allocator.free(match_indices);

        // Leader's match_index is its own log's last offset
        const leader_offset = self.log.next_offset - 1;
        match_indices[0] = leader_offset;
        var count: usize = 1;

        // Collect follower match_indices (ONLY for in-sync followers)
        for (self.followers) |follower| {
            if (follower.in_sync) {
                match_indices[count] = follower.match_index orelse 0;
                count += 1;
            }
        }

        // Check if we have enough in-sync replicas for quorum
        if (count < self.quorum_size) {
            // Not enough replicas in sync - no progress
            return self.commit_index;
        }

        // Sort in descending order (only the valid entries)
        std.mem.sort(u64, match_indices[0..count], {}, comptime std.sort.desc(u64));

        // The commit_index is the value at index (quorum_size - 1)
        // This ensures at least quorum_size replicas have this value or higher
        const commit_idx_position = @min(self.quorum_size - 1, count - 1);
        const new_commit = match_indices[commit_idx_position];

        // Never decrease commit_index
        if (self.commit_index) |old| {
            return @max(new_commit, old);
        } else {
            return new_commit;
        }
    }

    /// Background repair process - called periodically by event loop
    /// Fully async: uses poll() to check socket readiness, never blocks
    /// Processes ONE follower state transition per call
    /// Returns true if repair work was performed
    pub fn tickRepair(self: *Leader) bool {
        // Fix empty log bug: nothing to replicate if log is empty
        if (self.log.next_offset == 0) return false;

        const leader_offset = self.log.next_offset - 1;

        // Find first follower that needs work
        for (self.followers) |*follower| {
            // Skip if already caught up
            if (follower.next_index > leader_offset and follower.state == .replicating) {
                continue;
            }

            switch (follower.state) {
                .needs_repair => {
                    // Start repair: send probe entry to find common prefix
                    // Check retry limit
                    if (follower.repair_retry_count >= self.repair_max_retries) {
                        // Give up on this follower
                        follower.in_sync = false;
                        follower.state = .replicating; // Reset for future attempts
                        follower.repair_retry_count = 0;
                        return true;
                    }

                    // If next_index is 0, we've reached the beginning
                    if (follower.next_index == 0) {
                        // Start from offset 0
                        follower.repair_probe_offset = null;
                        follower.repair_retry_count = 0;
                        // Transition to streaming state
                        follower.state = .replicating;
                        return true;
                    }

                    // Probe at next_index - 1
                    const probe_offset = follower.next_index - 1;
                    const probe_record = self.log.read(probe_offset, self.allocator) catch {
                        follower.in_sync = false;
                        return true;
                    };
                    defer {
                        if (probe_record.key) |k| self.allocator.free(k);
                        self.allocator.free(probe_record.value);
                    }

                    var entries = self.allocator.alloc(protocol.ReplicatedEntry, 1) catch {
                        follower.in_sync = false;
                        return true;
                    };
                    defer self.allocator.free(entries);

                    entries[0] = protocol.ReplicatedEntry{
                        .offset = probe_offset,
                        .record = probe_record,
                    };

                    const req = ReplicateRequest{
                        .entries = entries,
                        .leader_commit = self.commit_index orelse 0,
                    };

                    // Send probe (non-blocking)
                    self.sendReplicateRequestNonBlocking(follower, req) catch {
                        follower.in_sync = false;
                        return true;
                    };

                    // Transition to awaiting probe response
                    follower.repair_probe_offset = probe_offset;
                    follower.repair_retry_count += 1;
                    follower.state = .repairing_probe_sent;
                    return true;
                },

                .repairing_probe_sent => {
                    // Check if probe response is ready (non-blocking)
                    if (follower.stream == null) {
                        follower.state = .needs_repair;
                        follower.in_sync = false;
                        return true;
                    }

                    // Poll with 0 timeout (non-blocking check)
                    if (!isSocketReady(follower.stream.?, 0)) {
                        // Not ready yet, yield
                        return false;
                    }

                    // Read response (data is ready, should not block)
                    const resp = self.receiveReplicateResponse(follower) catch {
                        follower.state = .needs_repair;
                        follower.in_sync = false;
                        return true;
                    };

                    if (resp.success) {
                        // Found common prefix!
                        const probe_offset = follower.repair_probe_offset.?;
                        follower.match_index = probe_offset;
                        follower.next_index = probe_offset + 1;
                        follower.repair_probe_offset = null;
                        follower.repair_retry_count = 0;
                        // Transition to replicating state (will stream missing entries next tick)
                        follower.state = .replicating;
                    } else if (resp.error_code == .offset_mismatch) {
                        // Decrement and retry
                        follower.next_index = follower.repair_probe_offset.?;
                        follower.repair_probe_offset = null;
                        follower.state = .needs_repair;
                    } else {
                        // Other error, give up
                        follower.state = .needs_repair;
                        follower.in_sync = false;
                    }

                    return true;
                },

                .repairing_stream_sent => {
                    // Check if streaming response is ready (non-blocking)
                    if (follower.stream == null) {
                        follower.state = .needs_repair;
                        follower.in_sync = false;
                        return true;
                    }

                    // Poll with 0 timeout (non-blocking check)
                    if (!isSocketReady(follower.stream.?, 0)) {
                        // Not ready yet, yield
                        return false;
                    }

                    // Read response
                    const resp = self.receiveReplicateResponse(follower) catch {
                        follower.state = .needs_repair;
                        follower.in_sync = false;
                        return true;
                    };

                    if (!resp.success) {
                        if (resp.error_code == .offset_mismatch) {
                            // Need to repair
                            follower.state = .needs_repair;
                            follower.repair_retry_count = 0;
                        } else {
                            follower.in_sync = false;
                            follower.state = .needs_repair;
                        }
                        return true;
                    }

                    // Update follower state
                    follower.match_index = resp.follower_offset;
                    follower.next_index = if (resp.follower_offset) |off| off + 1 else 0;
                    follower.last_offset = resp.follower_offset orelse 0;

                    // Check if fully caught up
                    if (follower.next_index > leader_offset) {
                        follower.state = .replicating;
                        follower.in_sync = true;
                    } else {
                        // More to stream, stay in replicating state for next tick
                        follower.state = .replicating;
                    }

                    return true;
                },

                .replicating => {
                    // Follower is healthy but lagging - stream catch-up batch
                    if (follower.next_index > leader_offset) {
                        // Already caught up
                        follower.in_sync = true;
                        continue;
                    }

                    // Calculate batch size
                    const remaining = leader_offset - follower.next_index + 1;
                    const batch_size: u32 = @intCast(@min(remaining, self.repair_batch_size));

                    const records = self.log.readRange(follower.next_index, batch_size, self.allocator) catch {
                        follower.in_sync = false;
                        return true;
                    };
                    defer {
                        for (records) |rec| {
                            if (rec.key) |k| self.allocator.free(k);
                            self.allocator.free(rec.value);
                        }
                        self.allocator.free(records);
                    }

                    var entries = self.allocator.alloc(protocol.ReplicatedEntry, records.len) catch {
                        follower.in_sync = false;
                        return true;
                    };
                    defer self.allocator.free(entries);

                    for (records, 0..) |rec, i| {
                        entries[i] = protocol.ReplicatedEntry{
                            .offset = follower.next_index + i,
                            .record = rec,
                        };
                    }

                    const req = ReplicateRequest{
                        .entries = entries,
                        .leader_commit = self.commit_index orelse 0,
                    };

                    // Send batch (non-blocking)
                    self.sendReplicateRequestNonBlocking(follower, req) catch {
                        follower.in_sync = false;
                        return true;
                    };

                    // Transition to awaiting stream response
                    follower.state = .repairing_stream_sent;
                    return true;
                },
            }
        }

        return false; // No work needed
    }

    /// Send heartbeats to all followers (called periodically)
    /// Updates follower ISR status based on their responses
    pub fn sendHeartbeats(self: *Leader) void {
        // Send next_offset (not last offset) to avoid ambiguity with empty logs
        const leader_next_offset = self.log.next_offset;

        for (self.followers) |*follower| {
            // Try to connect if not already connected
            if (follower.stream == null) {
                follower.connect() catch {
                    follower.in_sync = false;
                    follower.last_heartbeat_ms = 0;
                    continue;
                };
            }

            const req = HeartbeatRequest{
                .leader_commit = self.commit_index orelse 0,
                .leader_offset = leader_next_offset,
            };

            const resp = follower.sendHeartbeat(req, self.allocator) catch {
                follower.in_sync = false;
                follower.last_heartbeat_ms = 0; // Mark as failed
                follower.disconnect(); // Disconnect so we retry next time
                continue;
            };

            // Update follower state (Raft-like tracking)
            follower.last_heartbeat_ms = std.time.milliTimestamp();

            // Update match_index and next_index from response
            follower.match_index = resp.follower_offset;
            follower.next_index = if (resp.follower_offset) |offset| offset + 1 else 0;
            follower.last_offset = resp.follower_offset orelse 0; // For compatibility

            // Update in_sync status based on lag
            const lag = if (follower.match_index) |match_idx|
                if (leader_next_offset > match_idx)
                    leader_next_offset - match_idx - 1
                else
                    0
            else
                leader_next_offset; // Follower is empty, lag is entire log size

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
    pub fn getCommitIndex(self: *const Leader) ?u64 {
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

test "Leader: replicate with quorum failure leaves entry in log (Raft behavior)" {
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
    defer log.delete() catch {};

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
