const std = @import("std");
const message = @import("message.zig");
const Record = message.Record;
const Message = message.Message;
const MessageType = message.MessageType;
const ErrorCode = message.ErrorCode;
const AppendRequest = message.AppendRequest;
const AppendResponse = message.AppendResponse;
const ReadRequest = message.ReadRequest;
const ReadResponse = message.ReadResponse;
const ErrorResponse = message.ErrorResponse;
const ReplicateRequest = message.ReplicateRequest;
const ReplicateResponse = message.ReplicateResponse;
const ReplicatedEntry = message.ReplicatedEntry;
const HeartbeatRequest = message.HeartbeatRequest;
const HeartbeatResponse = message.HeartbeatResponse;

pub const ProtocolError = error{
    InvalidMessageType,
    InvalidMessageLength,
    MessageTooLarge,
    UnexpectedEndOfStream,
    SerializationError,
    DeserializationError,
};

/// Maximum message size (10MB)
pub const MAX_MESSAGE_SIZE: u32 = 10 * 1024 * 1024;

// Helper functions for binary I/O
fn writeIntBinary(writer: anytype, comptime T: type, value: T, endian: std.builtin.Endian) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, endian);
    try writer.writeAll(&bytes);
}

fn readIntFromBinary(reader: anytype, comptime T: type, endian: std.builtin.Endian) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    try readFull(reader, &bytes);
    return std.mem.readInt(T, &bytes, endian);
}

fn readFull(reader: anytype, buffer: []u8) !void {
    const bytes_read = try reader.readAll(buffer);
    if (bytes_read < buffer.len) {
        return error.EndOfStream;
    }
}

pub fn serializeMessage(writer: anytype, msg: Message, allocator: std.mem.Allocator) !void {
    var payload_buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer payload_buffer.deinit(allocator);

    const payload_writer = payload_buffer.writer(allocator);

    switch (msg) {
        .append_request => |req| try serializeAppendRequest(payload_writer, req),
        .append_response => |res| try serializeAppendResponse(payload_writer, res),
        .read_request => |req| try serializeReadRequest(payload_writer, req),
        .read_response => |res| try serializeReadResponse(payload_writer, res),
        .error_response => |err| try serializeErrorResponse(payload_writer, err),
        .replicate_request => |req| try serializeReplicateRequest(payload_writer, req),
        .replicate_response => |res| try serializeReplicateResponse(payload_writer, res),
        .heartbeat_request => |req| try serializeHeartbeatRequest(payload_writer, req),
        .heartbeat_response => |res| try serializeHeartbeatResponse(payload_writer, res),
    }

    const payload = payload_buffer.items;
    const total_length: u32 = @intCast(1 + payload.len);

    if (total_length > MAX_MESSAGE_SIZE) {
        return ProtocolError.MessageTooLarge;
    }

    try writeIntBinary(writer, u32, total_length, .little);
    const type_byte = @intFromEnum(msg);

    try writer.writeAll(&[_]u8{type_byte});
    try writer.writeAll(payload);
}

pub fn deserializeMessage(reader: anytype, allocator: std.mem.Allocator) !Message {
    const total_length = try readIntFromBinary(reader, u32, .little);

    if (total_length > MAX_MESSAGE_SIZE) {
        return ProtocolError.MessageTooLarge;
    }

    if (total_length < 1) {
        return ProtocolError.InvalidMessageLength;
    }

    const body = try allocator.alloc(u8, total_length);
    defer allocator.free(body);
    try readFull(reader, body);

    var body_stream = std.io.fixedBufferStream(body);
    return try deserializeMessageBody(body_stream.reader(), allocator);
}

pub fn deserializeMessageBody(reader: anytype, allocator: std.mem.Allocator) !Message {
    var type_bytes: [1]u8 = undefined;
    try readFull(reader, &type_bytes);
    const msg_type = std.meta.intToEnum(MessageType, type_bytes[0]) catch {
        return ProtocolError.InvalidMessageType;
    };

    return switch (msg_type) {
        .append_request => Message{ .append_request = try deserializeAppendRequest(reader, allocator) },
        .append_response => Message{ .append_response = try deserializeAppendResponse(reader) },
        .read_request => Message{ .read_request = try deserializeReadRequest(reader) },
        .read_response => Message{ .read_response = try deserializeReadResponse(reader, allocator) },
        .error_response => Message{ .error_response = try deserializeErrorResponse(reader, allocator) },
        .replicate_request => Message{ .replicate_request = try deserializeReplicateRequest(reader, allocator) },
        .replicate_response => Message{ .replicate_response = try deserializeReplicateResponse(reader) },
        .heartbeat_request => Message{ .heartbeat_request = try deserializeHeartbeatRequest(reader) },
        .heartbeat_response => Message{ .heartbeat_response = try deserializeHeartbeatResponse(reader) },
    };
}

fn serializeAppendRequest(writer: anytype, req: AppendRequest) !void {
    if (req.record.key) |key| {
        try writeIntBinary(writer, i32, @intCast(key.len), .little);
        try writer.writeAll(key);
    } else {
        try writeIntBinary(writer, i32, -1, .little);
    }
    try writeIntBinary(writer, i32, @intCast(req.record.value.len), .little);
    try writer.writeAll(req.record.value);
}

fn deserializeAppendRequest(reader: anytype, allocator: std.mem.Allocator) !AppendRequest {
    const key_len = try readIntFromBinary(reader, i32, .little);
    const key: ?[]const u8 = if (key_len >= 0) blk: {
        const k = try allocator.alloc(u8, @intCast(key_len));
        try readFull(reader, k);
        break :blk k;
    } else null;

    const value_len = try readIntFromBinary(reader, i32, .little);
    if (value_len < 0) {
        if (key) |k| allocator.free(k);
        return ProtocolError.DeserializationError;
    }

    const value = try allocator.alloc(u8, @intCast(value_len));
    try readFull(reader, value);

    return AppendRequest{
        .record = Record{ .key = key, .value = value },
    };
}

fn serializeAppendResponse(writer: anytype, res: AppendResponse) !void {
    try writeIntBinary(writer, u64, res.offset, .little);
}

fn deserializeAppendResponse(reader: anytype) !AppendResponse {
    return AppendResponse{
        .offset = try readIntFromBinary(reader, u64, .little),
    };
}

fn serializeReadRequest(writer: anytype, req: ReadRequest) !void {
    try writeIntBinary(writer, u64, req.offset, .little);
}

fn deserializeReadRequest(reader: anytype) !ReadRequest {
    return ReadRequest{
        .offset = try readIntFromBinary(reader, u64, .little),
    };
}

fn serializeReadResponse(writer: anytype, res: ReadResponse) !void {
    if (res.record.key) |key| {
        try writeIntBinary(writer, i32, @intCast(key.len), .little);
        try writer.writeAll(key);
    } else {
        try writeIntBinary(writer, i32, -1, .little);
    }
    try writeIntBinary(writer, i32, @intCast(res.record.value.len), .little);
    try writer.writeAll(res.record.value);
}

fn deserializeReadResponse(reader: anytype, allocator: std.mem.Allocator) !ReadResponse {
    const key_len = try readIntFromBinary(reader, i32, .little);
    const key: ?[]const u8 = if (key_len >= 0) blk: {
        const k = try allocator.alloc(u8, @intCast(key_len));
        try readFull(reader, k);
        break :blk k;
    } else null;

    const value_len = try readIntFromBinary(reader, i32, .little);
    if (value_len < 0) {
        if (key) |k| allocator.free(k);
        return ProtocolError.DeserializationError;
    }

    const value = try allocator.alloc(u8, @intCast(value_len));
    try readFull(reader, value);

    return ReadResponse{
        .record = Record{ .key = key, .value = value },
    };
}

fn serializeErrorResponse(writer: anytype, err: ErrorResponse) !void {
    try writer.writeAll(&[_]u8{@intFromEnum(err.code)});
    try writeIntBinary(writer, i32, @intCast(err.message.len), .little);
    try writer.writeAll(err.message);
}

fn deserializeErrorResponse(reader: anytype, allocator: std.mem.Allocator) !ErrorResponse {
    var code_bytes: [1]u8 = undefined;
    try readFull(reader, &code_bytes);
    const code = std.meta.intToEnum(ErrorCode, code_bytes[0]) catch ErrorCode.unknown;

    const msg_len = try readIntFromBinary(reader, i32, .little);
    if (msg_len < 0) return ProtocolError.DeserializationError;

    const message_str = try allocator.alloc(u8, @intCast(msg_len));
    try readFull(reader, message_str);

    return ErrorResponse{ .code = code, .message = message_str };
}

fn serializeReplicateRequest(writer: anytype, req: ReplicateRequest) !void {
    try writeIntBinary(writer, u64, req.leader_commit, .little);
    try writeIntBinary(writer, u32, @intCast(req.entries.len), .little);

    for (req.entries) |entry| {
        try writeIntBinary(writer, u64, entry.offset, .little);
        if (entry.record.key) |key| {
            try writeIntBinary(writer, i32, @intCast(key.len), .little);
            try writer.writeAll(key);
        } else {
            try writeIntBinary(writer, i32, -1, .little);
        }
        try writeIntBinary(writer, i32, @intCast(entry.record.value.len), .little);
        try writer.writeAll(entry.record.value);
    }
}

fn deserializeReplicateRequest(reader: anytype, allocator: std.mem.Allocator) !ReplicateRequest {
    const leader_commit = try readIntFromBinary(reader, u64, .little);
    const entry_count = try readIntFromBinary(reader, u32, .little);

    var entries = try std.ArrayList(ReplicatedEntry).initCapacity(allocator, entry_count);
    errdefer {
        for (entries.items) |entry| {
            if (entry.record.key) |k| allocator.free(k);
            allocator.free(entry.record.value);
        }
        entries.deinit(allocator);
    }

    var i: u32 = 0;
    while (i < entry_count) : (i += 1) {
        const offset = try readIntFromBinary(reader, u64, .little);
        const key_len = try readIntFromBinary(reader, i32, .little);
        const key: ?[]const u8 = if (key_len >= 0) blk: {
            const k = try allocator.alloc(u8, @intCast(key_len));
            try readFull(reader, k);
            break :blk k;
        } else null;

        const value_len = try readIntFromBinary(reader, i32, .little);
        if (value_len < 0) {
            if (key) |k| allocator.free(k);
            return ProtocolError.DeserializationError;
        }

        const value = try allocator.alloc(u8, @intCast(value_len));
        try readFull(reader, value);

        try entries.append(allocator, ReplicatedEntry{
            .offset = offset,
            .record = Record{ .key = key, .value = value },
        });
    }

    return ReplicateRequest{
        .entries = try entries.toOwnedSlice(allocator),
        .leader_commit = leader_commit,
    };
}

fn serializeReplicateResponse(writer: anytype, res: ReplicateResponse) !void {
    try writer.writeAll(&[_]u8{if (res.success) 1 else 0});
    if (res.follower_offset) |offset| {
        try writer.writeAll(&[_]u8{1});
        try writeIntBinary(writer, u64, offset, .little);
    } else {
        try writer.writeAll(&[_]u8{0});
    }
    try writer.writeAll(&[_]u8{@intFromEnum(res.error_code)});
}

fn deserializeReplicateResponse(reader: anytype) !ReplicateResponse {
    var success_bytes: [1]u8 = undefined;
    try readFull(reader, &success_bytes);
    const success = success_bytes[0] != 0;

    var has_offset_bytes: [1]u8 = undefined;
    try readFull(reader, &has_offset_bytes);
    const follower_offset: ?u64 = if (has_offset_bytes[0] == 1)
        try readIntFromBinary(reader, u64, .little)
    else
        null;

    var error_code_bytes: [1]u8 = undefined;
    try readFull(reader, &error_code_bytes);
    const error_code = std.meta.intToEnum(ErrorCode, error_code_bytes[0]) catch ErrorCode.unknown;

    return ReplicateResponse{
        .success = success,
        .follower_offset = follower_offset,
        .error_code = error_code,
    };
}

fn serializeHeartbeatRequest(writer: anytype, req: HeartbeatRequest) !void {
    try writeIntBinary(writer, u64, req.leader_commit, .little);
    try writeIntBinary(writer, u64, req.leader_offset, .little);
}

fn deserializeHeartbeatRequest(reader: anytype) !HeartbeatRequest {
    return HeartbeatRequest{
        .leader_commit = try readIntFromBinary(reader, u64, .little),
        .leader_offset = try readIntFromBinary(reader, u64, .little),
    };
}

fn serializeHeartbeatResponse(writer: anytype, res: HeartbeatResponse) !void {
    if (res.follower_offset) |offset| {
        try writer.writeAll(&[_]u8{1});
        try writeIntBinary(writer, u64, offset, .little);
    } else {
        try writer.writeAll(&[_]u8{0});
    }
}

fn deserializeHeartbeatResponse(reader: anytype) !HeartbeatResponse {
    var has_offset_bytes: [1]u8 = undefined;
    try readFull(reader, &has_offset_bytes);
    const follower_offset: ?u64 = if (has_offset_bytes[0] == 1)
        try readIntFromBinary(reader, u64, .little)
    else
        null;

    return HeartbeatResponse{ .follower_offset = follower_offset };
}
