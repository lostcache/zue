const std = @import("std");
pub const Record = @import("../log/record.zig").Record;

pub const MessageType = enum(u8) {
    // Client-Server messages
    append_request = 0x01,
    append_response = 0x02,
    read_request = 0x03,
    read_response = 0x04,
    error_response = 0xFF,

    // Replication messages (Leader <-> Follower)
    replicate_request = 0x10,
    replicate_response = 0x11,
    heartbeat_request = 0x20,
    heartbeat_response = 0x21,
};

pub const ErrorCode = enum(u8) {
    unknown = 0x00,
    invalid_offset = 0x01,
    record_not_found = 0x02,
    io_error = 0x03,
    serialization_error = 0x04,
    invalid_message = 0x05,
    log_full = 0x06,
    // Replication-specific errors
    not_leader = 0x10,
    quorum_failed = 0x11,
    replication_timeout = 0x12,
    offset_mismatch = 0x13,
    insufficient_isr = 0x14,
};

pub const AppendRequest = struct {
    record: Record,
};

pub const AppendResponse = struct {
    offset: u64,
};

pub const ReadRequest = struct {
    offset: u64,
};

pub const ReadResponse = struct {
    record: Record,
};

pub const ErrorResponse = struct {
    code: ErrorCode,
    message: []const u8,
};

// ============================================================================
// Replication Protocol Messages
// ============================================================================

pub const ReplicatedEntry = struct {
    offset: u64,
    record: Record,
};

pub const ReplicateRequest = struct {
    entries: []ReplicatedEntry,  // One or more entries to replicate (with their offsets)
    leader_commit: u64,           // Leader's commit index (for follower to advance)
};

pub const ReplicateResponse = struct {
    success: bool,
    follower_offset: ?u64,   // Follower's current last offset (null if log is empty)
    error_code: ErrorCode,  // Error code on failure
};

pub const HeartbeatRequest = struct {
    leader_commit: u64,    // For follower to advance commit index
    leader_offset: u64,    // Leader's last offset
};

pub const HeartbeatResponse = struct {
    follower_offset: ?u64,  // Follower's last offset (null if log is empty)
};

pub const Message = union(MessageType) {
    append_request: AppendRequest,
    append_response: AppendResponse,
    read_request: ReadRequest,
    read_response: ReadResponse,
    error_response: ErrorResponse,
    // Replication messages
    replicate_request: ReplicateRequest,
    replicate_response: ReplicateResponse,
    heartbeat_request: HeartbeatRequest,
    heartbeat_response: HeartbeatResponse,
};
