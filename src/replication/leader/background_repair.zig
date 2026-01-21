const std = @import("std");
const Leader = @import("main.zig").Leader;
const protocol = @import("../../network/protocol.zig");
const utils = @import("utils.zig");
const quorum = @import("quorum.zig");

const ReplicateRequest = protocol.ReplicateRequest;
const HeartbeatRequest = protocol.HeartbeatRequest;

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
            .repairing_stream_sent => {
                // Check if streaming response is ready (non-blocking)
                if (follower.stream == null) {
                    follower.state = .replicating;
                    follower.in_sync = false;
                    return true;
                }

                // Poll with 0 timeout (non-blocking check)
                if (!utils.isSocketReady(follower.stream.?, 0)) {
                    // Not ready yet, yield
                    return false;
                }

                // Read response
                const resp = follower.receiveReplicateResponse(self.allocator) catch {
                    follower.state = .replicating;
                    follower.in_sync = false;
                    return true;
                };

                if (!resp.success) {
                    if (resp.error_code == .offset_mismatch) {
                        // Use follower's actual offset to jump directly
                        follower.match_index = resp.follower_offset;
                        follower.next_index = if (resp.follower_offset) |off| off + 1 else 0;
                        follower.state = .replicating;
                        follower.in_sync = false;
                    } else {
                        follower.in_sync = false;
                        follower.state = .replicating;
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
                follower.sendReplicateRequestNonBlocking(req, self.allocator) catch {
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
    const isr_count = quorum.countInSync(self) + 1; // +1 for leader
    if (isr_count < self.quorum_size) {
        std.debug.print("WARNING: ISR size ({d}) < quorum ({d}). System is NOT writable!\n", .{
            isr_count,
            self.quorum_size,
        });
    }
}
