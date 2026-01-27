const std = @import("std");
const Log = @import("../../log/log.zig").Log;
const protocol = @import("../../network/protocol.zig");

const ReplicateRequest = protocol.ReplicateRequest;
const ReplicateResponse = protocol.ReplicateResponse;
const HeartbeatRequest = protocol.HeartbeatRequest;
const HeartbeatResponse = protocol.HeartbeatResponse;
const ErrorCode = protocol.ErrorCode;

/// Handle replication request from leader
/// CRITICAL: Validates offset to detect gaps
pub fn handleReplicateRequest(
    log: *Log,
    commit_index: *u64,
    req: ReplicateRequest,
) !ReplicateResponse {
    // Handle batch of entries (streaming replication)
    var last_offset: ?u64 = if (log.next_offset > 0) log.next_offset - 1 else null;

    for (req.entries) |entry| {
        // CRITICAL: Validate entry.offset == log.next_offset
        const expected_offset = log.next_offset;

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
        last_offset = try log.append(entry.record);
    }

    // Flush to disk for durability (async - schedules write without blocking)
    try log.syncAsync();

    // Update commit index from leader
    commit_index.* = req.leader_commit;

    return ReplicateResponse{
        .success = true,
        .follower_offset = last_offset,
        .error_code = @enumFromInt(0), // .none - using raw value for compatibility
    };
}

/// Handle heartbeat from leader
pub fn handleHeartbeat(
    log: *Log,
    commit_index: *u64,
    last_heartbeat_ms: *i64,
    req: HeartbeatRequest,
) HeartbeatResponse {
    last_heartbeat_ms.* = std.time.milliTimestamp();
    commit_index.* = req.leader_commit; // Advance commit index

    // Simply return current offset
    // Leader will determine if we're behind and trigger repair via tickRepair()
    return HeartbeatResponse{
        .follower_offset = if (log.next_offset > 0) log.next_offset - 1 else null,
    };
}
