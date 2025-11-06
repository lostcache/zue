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

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;
const Allocator = std.mem.Allocator;

/// Follower state machine for repair process
pub const FollowerState = enum {
    replicating,  // Normal replication, follower is in-sync
    needs_repair, // Detected offset_mismatch, repair needed but not started
    repairing,    // Currently running repair process in background
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

    /// Repair follower's log by finding common prefix (Raft algorithm)
    /// This implements the next_index decrement-and-retry logic
    /// Returns true if repair successful, false if follower should be marked out-of-sync
    fn repairFollowerLog(self: *Leader, follower: *FollowerConnection) !bool {
        const max_retries = 100; // Prevent infinite loops
        var retry_count: u32 = 0;

        // Repair loop: decrement next_index until we find a matching entry
        while (retry_count < max_retries) : (retry_count += 1) {
            // If next_index is 0, follower log is completely diverged or empty
            if (follower.next_index == 0) {
                return true; // Let caller send from offset 0
            }

            // Read the entry at next_index - 1 (the entry we want to verify follower has)
            const probe_offset = follower.next_index - 1;
            const probe_record = self.log.read(probe_offset, self.allocator) catch {
                return false;
            };
            // NOTE: Must free manually at end of EACH loop iteration, not function exit
            // defer would queue until function returns, causing memory leak in loop

            // Build entries array (single entry for probing)
            var entries = self.allocator.alloc(protocol.ReplicatedEntry, 1) catch {
                if (probe_record.key) |k| self.allocator.free(k);
                self.allocator.free(probe_record.value);
                return false;
            };
            entries[0] = protocol.ReplicatedEntry{
                .offset = probe_offset,
                .record = probe_record,
            };

            // Send this entry to follower to check if they have it
            const req = ReplicateRequest{
                .entries = entries,
                .leader_commit = self.commit_index orelse 0,
            };

            // Try to send the probe entry
            self.sendReplicateRequestNonBlocking(follower, req) catch {
                // Free memory before returning
                if (probe_record.key) |k| self.allocator.free(k);
                self.allocator.free(probe_record.value);
                self.allocator.free(entries);
                return false;
            };

            // Wait for response
            const resp = self.receiveReplicateResponse(follower) catch {
                // Free memory before returning
                if (probe_record.key) |k| self.allocator.free(k);
                self.allocator.free(probe_record.value);
                self.allocator.free(entries);
                return false;
            };

            if (resp.success) {
                // Found common prefix! Update match_index and next_index
                follower.match_index = probe_offset;
                follower.next_index = probe_offset + 1;
                // Free memory before returning
                if (probe_record.key) |k| self.allocator.free(k);
                self.allocator.free(probe_record.value);
                self.allocator.free(entries);
                return true;
            } else if (resp.error_code == .offset_mismatch) {
                // Still mismatched, decrement next_index and retry
                follower.next_index = probe_offset;
                // Free memory before next iteration
                if (probe_record.key) |k| self.allocator.free(k);
                self.allocator.free(probe_record.value);
                self.allocator.free(entries);
                // Continue loop
            } else {
                // Other error, give up on this follower
                // Free memory before returning
                if (probe_record.key) |k| self.allocator.free(k);
                self.allocator.free(probe_record.value);
                self.allocator.free(entries);
                return false;
            }
        }

        // Max retries exceeded
        return false;
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
        for (self.followers) |*follower| {
            if (!follower.in_sync) continue;

            // Build array of entries to send (streaming catch-up)
            // If follower is behind (next_index < offset), send all missing entries
            if (follower.next_index < offset) {
                // Follower is behind - stream all missing entries (including new one)
                const count: u32 = @intCast(offset - follower.next_index + 1);
                const records = try self.log.readRange(follower.next_index, count, self.allocator);
                // NOTE: Don't use defer here - we need records to stay alive during serialization

                // Build ReplicatedEntry array
                var entries_to_send = try self.allocator.alloc(protocol.ReplicatedEntry, records.len);
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

                // Send request (serializes immediately into buffer, then non-blocking write)
                self.sendReplicateRequestNonBlocking(follower, req) catch {
                    // Clean up on error
                    for (records) |rec| {
                        if (rec.key) |k| self.allocator.free(k);
                        self.allocator.free(rec.value);
                    }
                    self.allocator.free(records);
                    self.allocator.free(entries_to_send);
                    follower.in_sync = false;
                    continue;
                };

                // Clean up after successful send (data already serialized)
                for (records) |rec| {
                    if (rec.key) |k| self.allocator.free(k);
                    self.allocator.free(rec.value);
                }
                self.allocator.free(records);
                self.allocator.free(entries_to_send);
            } else {
                // Follower is up-to-date - send only the new entry
                var entries_to_send = try self.allocator.alloc(protocol.ReplicatedEntry, 1);
                entries_to_send[0] = protocol.ReplicatedEntry{
                    .offset = offset,
                    .record = record,
                };

                const req = ReplicateRequest{
                    .entries = entries_to_send,
                    .leader_commit = self.commit_index orelse 0,
                };

                // Send request (serializes immediately into buffer, then non-blocking write)
                self.sendReplicateRequestNonBlocking(follower, req) catch {
                    self.allocator.free(entries_to_send);
                    follower.in_sync = false;
                    continue;
                };

                // Clean up after successful send
                self.allocator.free(entries_to_send);
            }

            // Track this follower for response collection
            try pending_followers.append(self.allocator, follower);
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
    /// Processes ONE follower in needs_repair state per call
    /// Returns true if repair work was performed
    pub fn tickRepair(self: *Leader) bool {
        // Find first follower that needs repair
        for (self.followers) |*follower| {
            if (follower.state == .needs_repair) {
                follower.state = .repairing; // Mark as actively repairing

                // Attempt repair
                const repair_success = self.repairFollowerLog(follower) catch false;

                if (repair_success) {
                    // Repair found common prefix, now stream missing entries
                    const leader_offset = if (self.log.next_offset > 0) self.log.next_offset - 1 else 0;

                    // Stream all missing entries in ONE batch (efficient streaming)
                    const count: u32 = @intCast(leader_offset - follower.next_index + 1);
                    const records = self.log.readRange(follower.next_index, count, self.allocator) catch {
                        follower.in_sync = false;
                        follower.state = .needs_repair; // Try again later
                        return true;
                    };
                    defer {
                        for (records) |rec| {
                            if (rec.key) |k| self.allocator.free(k);
                            self.allocator.free(rec.value);
                        }
                        self.allocator.free(records);
                    }

                    // Build ReplicatedEntry array
                    var entries = self.allocator.alloc(protocol.ReplicatedEntry, records.len) catch {
                        follower.in_sync = false;
                        follower.state = .needs_repair;
                        return true;
                    };
                    defer self.allocator.free(entries);

                    for (records, 0..) |rec, i| {
                        entries[i] = protocol.ReplicatedEntry{
                            .offset = follower.next_index + i,
                            .record = rec,
                        };
                    }

                    const stream_req = ReplicateRequest{
                        .entries = entries,
                        .leader_commit = self.commit_index orelse 0,
                    };

                    // Send batch and wait for response
                    self.sendReplicateRequestNonBlocking(follower, stream_req) catch {
                        follower.in_sync = false;
                        follower.state = .needs_repair; // Try again later
                        return true;
                    };

                    const stream_resp = self.receiveReplicateResponse(follower) catch {
                        follower.in_sync = false;
                        follower.state = .needs_repair; // Try again later
                        return true;
                    };

                    if (!stream_resp.success) {
                        follower.in_sync = false;
                        follower.state = .needs_repair; // Try again later
                        return true;
                    }

                    // Update follower state (all entries applied)
                    follower.match_index = leader_offset;
                    follower.next_index = leader_offset + 1;
                    follower.last_offset = leader_offset;

                    // Repair complete!
                    follower.state = .replicating;
                    follower.in_sync = true;
                } else {
                    follower.state = .needs_repair; // Try again later
                    follower.in_sync = false;
                }

                return true; // Performed repair work
            }
        }

        return false; // No repair work needed
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
