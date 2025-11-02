/// Network protocol for Zue distributed log
///
/// Protocol Design:
/// - Simple length-prefixed binary format
/// - Message structure: [4-byte length][1-byte type][payload]
/// - All integers are little-endian
/// - Strings are length-prefixed: [4-byte length][bytes]
///
/// Message Types:
/// - AppendRequest: Client requests to append a record
/// - AppendResponse: Server responds with assigned offset
/// - ReadRequest: Client requests to read a record at offset
/// - ReadResponse: Server responds with record data
/// - ErrorResponse: Server reports error
const std = @import("std");
pub const Record = @import("../log/record.zig").Record;

pub const MessageType = enum(u8) {
    append_request = 0x01,
    append_response = 0x02,
    read_request = 0x03,
    read_response = 0x04,
    error_response = 0xFF,
};

pub const ErrorCode = enum(u8) {
    unknown = 0x00,
    invalid_offset = 0x01,
    record_not_found = 0x02,
    io_error = 0x03,
    serialization_error = 0x04,
    invalid_message = 0x05,
    log_full = 0x06,
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

pub const Message = union(MessageType) {
    append_request: AppendRequest,
    append_response: AppendResponse,
    read_request: ReadRequest,
    read_response: ReadResponse,
    error_response: ErrorResponse,
};

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

// Helper functions for binary I/O in Zig 0.15.1
fn writeIntBinary(writer: anytype, comptime T: type, value: T, endian: std.builtin.Endian) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, endian);
    // writer can be *std.io.Writer or ArrayList.GenericWriter - both have writeAll()
    try writer.writeAll(&bytes);
}

fn readIntFromBinary(reader: anytype, comptime T: type, endian: std.builtin.Endian) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    try readFull(reader, &bytes);
    return std.mem.readInt(T, &bytes, endian);
}

// Read exactly the required number of bytes using reader
fn readFull(reader: anytype, buffer: []u8) !void {
    const bytes_read = try reader.readAll(buffer);
    if (bytes_read < buffer.len) {
        return error.EndOfStream;
    }
}

/// Serialize a message to a writer
/// IMPORTANT: For net.Stream.Writer, pass &writer.interface (not writer directly) to avoid @fieldParentPtr issues
pub fn serializeMessage(writer: anytype, message: Message, allocator: std.mem.Allocator) !void {
    // First, serialize the payload to calculate total size
    var payload_buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer payload_buffer.deinit(allocator);

    const payload_writer = payload_buffer.writer(allocator);

    switch (message) {
        .append_request => |req| try serializeAppendRequest(payload_writer, req),
        .append_response => |res| try serializeAppendResponse(payload_writer, res),
        .read_request => |req| try serializeReadRequest(payload_writer, req),
        .read_response => |res| try serializeReadResponse(payload_writer, res),
        .error_response => |err| try serializeErrorResponse(payload_writer, err),
    }

    const payload = payload_buffer.items;
    const total_length: u32 = @intCast(1 + payload.len); // 1 byte for type + payload

    if (total_length > MAX_MESSAGE_SIZE) {
        return ProtocolError.MessageTooLarge;
    }

    // Write: [length][type][payload]
    try writeIntBinary(writer, u32, total_length, .little);
    const type_byte = @intFromEnum(message);

    try writer.writeAll(&[_]u8{type_byte});
    try writer.writeAll(payload);
    // NOTE: Caller must call flush() on network writers after this returns
}

/// Deserialize a message from a reader (expects to read length prefix first)
pub fn deserializeMessage(reader: anytype, allocator: std.mem.Allocator) !Message {
    // Read total length
    const total_length = try readIntFromBinary(reader, u32, .little);

    if (total_length > MAX_MESSAGE_SIZE) {
        return ProtocolError.MessageTooLarge;
    }

    if (total_length < 1) {
        return ProtocolError.InvalidMessageLength;
    }

    // Read message body (type + payload)
    const body = try allocator.alloc(u8, total_length);
    defer allocator.free(body);
    try readFull(reader, body);

    // Deserialize the body
    var body_stream = std.io.fixedBufferStream(body);
    return try deserializeMessageBody(body_stream.reader(), allocator);
}

fn decodeMessageType(reader: anytype)  !MessageType {
    var type_bytes: [1]u8 = undefined;
    try readFull(reader, &type_bytes);
    const type_byte = type_bytes[0];
    const msg_type = std.meta.intToEnum(MessageType, type_byte) catch {
        return ProtocolError.InvalidMessageType;
    };

    return msg_type;
}

pub fn deserializeMessageBody(reader: anytype, allocator: std.mem.Allocator) !Message {
    const msg_type = try decodeMessageType(reader);

    return switch (msg_type) {
        .append_request => Message{ .append_request = try deserializeAppendRequest(reader, allocator) },
        .append_response => Message{ .append_response = try deserializeAppendResponse(reader) },
        .read_request => Message{ .read_request = try deserializeReadRequest(reader) },
        .read_response => Message{ .read_response = try deserializeReadResponse(reader, allocator) },
        .error_response => Message{ .error_response = try deserializeErrorResponse(reader, allocator) },
    };
}

/// Simulates how network layer reads: length prefix separately, then message body
fn simulateNetworkRead(full_buffer: []const u8, body_buffer: []u8) !struct { body: []u8 } {
    // Step 1: Read 4-byte length prefix (like network code does)
    if (full_buffer.len < 4) return error.BufferTooSmall;
    const message_len = std.mem.readInt(u32, full_buffer[0..4], .little);

    // Step 2: Read message body into separate buffer (like network code does)
    const total_needed = 4 + message_len;
    if (full_buffer.len < total_needed) return error.BufferTooSmall;
    if (body_buffer.len < message_len) return error.BufferTooSmall;

    @memcpy(body_buffer[0..message_len], full_buffer[4..total_needed]);

    return .{ .body = body_buffer[0..message_len] };
}

/// Mimics what readCompleteMessage() does:
/// 1. Read 4-byte length prefix
/// 2. Read message body (type + payload)
/// 3. Return ONLY the message body (without length prefix)
fn readMessageBodyOnly(buffer: []const u8) ![]const u8 {
    if (buffer.len < 4) return error.BufferTooSmall;

    const message_len = std.mem.readInt(u32, buffer[0..4], .little);
    const total_len = 4 + message_len;

    if (buffer.len < total_len) return error.BufferTooSmall;

    // Return ONLY message body (skip length prefix)
    return buffer[4..total_len];
}

test "network compatibility: serialize -> network read -> deserialize works correctly" {
    const allocator = std.testing.allocator;

    const original = Message{
        .append_request = AppendRequest{
            .record = Record{ .key = "test-key", .value = "test-value" },
        },
    };

    // Serialize
    var serialize_buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer serialize_buffer.deinit(allocator);
    try serializeMessage(serialize_buffer.writer(allocator), original, allocator);

    // Simulate network read (reads length separately, then body)
    var body_buffer: [4096]u8 = undefined;
    const result = try simulateNetworkRead(serialize_buffer.items, &body_buffer);

    // Deserialize from body using the correct API
    var body_stream = std.io.fixedBufferStream(result.body);
    const deserialized = try deserializeMessageBody(body_stream.reader(), allocator);
    defer {
        if (deserialized.append_request.record.key) |k| allocator.free(k);
        allocator.free(deserialized.append_request.record.value);
    }

    // Verify it worked
    try std.testing.expectEqualStrings("test-key", deserialized.append_request.record.key.?);
    try std.testing.expectEqualStrings("test-value", deserialized.append_request.record.value);
}

test "network compatibility: all message types work correctly" {
    const allocator = std.testing.allocator;

    const test_cases = [_]Message{
        Message{ .append_request = AppendRequest{ .record = Record{ .key = "k", .value = "v" } } },
        Message{ .append_response = AppendResponse{ .offset = 42 } },
        Message{ .read_request = ReadRequest{ .offset = 123 } },
        Message{ .read_response = ReadResponse{ .record = Record{ .key = null, .value = "data" } } },
        Message{ .error_response = ErrorResponse{ .code = .io_error, .message = "error" } },
    };

    for (test_cases) |original| {
        var serialize_buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
        defer serialize_buffer.deinit(allocator);
        try serializeMessage(serialize_buffer.writer(allocator), original, allocator);

        var body_buffer: [4096]u8 = undefined;
        const result = try simulateNetworkRead(serialize_buffer.items, &body_buffer);

        var body_stream = std.io.fixedBufferStream(result.body);
        const deserialized = try deserializeMessageBody(body_stream.reader(), allocator);

        // Free allocated memory
        switch (deserialized) {
            .append_request => |req| {
                if (req.record.key) |k| allocator.free(k);
                allocator.free(req.record.value);
            },
            .read_response => |res| {
                if (res.record.key) |k| allocator.free(k);
                allocator.free(res.record.value);
            },
            .error_response => |err| allocator.free(err.message),
            else => {},
        }
    }
}

test "framing: deserialize after reading message body only" {
    const allocator = std.testing.allocator;

    // Serialize a message (this includes length prefix)
    const original = Message{
        .append_request = .{
            .record = Record{
                .key = "test-key",
                .value = "test-value",
            },
        },
    };

    var buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer buffer.deinit(allocator);

    try serializeMessage(buffer.writer(allocator), original, allocator);

    // Now mimic network code: read length prefix, get body only
    const message_body = try readMessageBodyOnly(buffer.items);

    // Deserialize the body using deserializeMessageBody
    var body_stream = std.io.fixedBufferStream(message_body);
    const deserialized = try deserializeMessageBody(body_stream.reader(), allocator);
    defer {
        if (deserialized.append_request.record.key) |k| allocator.free(k);
        allocator.free(deserialized.append_request.record.value);
    }

    try std.testing.expectEqualStrings("test-key", deserialized.append_request.record.key.?);
    try std.testing.expectEqualStrings("test-value", deserialized.append_request.record.value);
}

test "round-trip: multiple messages" {
    const allocator = std.testing.allocator;

    const messages = [_]Message{
        .{ .append_request = .{ .record = .{ .key = "k1", .value = "v1" } } },
        .{ .append_response = .{ .offset = 0 } },
        .{ .read_request = .{ .offset = 0 } },
        .{ .read_response = .{ .record = .{ .key = "k1", .value = "v1" } } },
    };

    for (messages) |msg| {
        var buffer: [4096]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buffer);
        try serializeMessage(stream.writer(), msg, allocator);

        var read_stream = std.io.fixedBufferStream(stream.getWritten());
        const deserialized = try deserializeMessage(read_stream.reader(), allocator);

        // Free allocated memory
        switch (deserialized) {
            .append_request => |req| {
                if (req.record.key) |k| allocator.free(k);
                allocator.free(req.record.value);
            },
            .read_response => |res| {
                if (res.record.key) |k| allocator.free(k);
                allocator.free(res.record.value);
            },
            .error_response => |err| allocator.free(err.message),
            else => {},
        }
    }
}

test "serializeMessage: reject message that exceeds MAX_MESSAGE_SIZE" {
    const allocator = std.testing.allocator;

    // Create a message with huge value
    const huge_value = try allocator.alloc(u8, MAX_MESSAGE_SIZE);
    defer allocator.free(huge_value);

    const original = Message{
        .append_request = AppendRequest{
            .record = Record{
                .key = null,
                .value = huge_value,
            },
        },
    };

    var buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer buffer.deinit(allocator);

    const result = serializeMessage(buffer.writer(allocator), original, allocator);
    try std.testing.expectError(ProtocolError.MessageTooLarge, result);
}

// ============================================================================
// Individual message serialization/deserialization
// ============================================================================

fn serializeAppendRequest(writer: anytype, req: AppendRequest) !void {
    // Serialize record: [key_length][key?][value_length][value]
    if (req.record.key) |key| {
        const key_len: i32 = @intCast(key.len);
        try writeIntBinary(writer, i32, key_len, .little);
        try writer.writeAll(key); // ArrayList.writer() has writeAll directly
    } else {
        try writeIntBinary(writer, i32, -1, .little);
    }

    const value_len: i32 = @intCast(req.record.value.len);
    try writeIntBinary(writer, i32, value_len, .little);
    try writer.writeAll(req.record.value);
}

fn deserializeAppendRequest(reader: anytype, allocator: std.mem.Allocator) !AppendRequest {
    // Deserialize record: [key_length][key?][value_length][value]
    const key_len = try readIntFromBinary(reader, i32, .little);
    const key: ?[]const u8 = if (key_len >= 0) blk: {
        const k = try allocator.alloc(u8, @intCast(key_len));
        try readFull(reader, k);
        break :blk k;
    } else null;

    // Read value
    const value_len = try readIntFromBinary(reader, i32, .little);
    if (value_len < 0) {
        if (key) |k| allocator.free(k);
        return ProtocolError.DeserializationError;
    }

    const value = try allocator.alloc(u8, @intCast(value_len));
    try readFull(reader, value);

    return AppendRequest{
        .record = Record{
            .key = key,
            .value = value,
        },
    };
}

test "serializeAppendRequest and deserializeAppendRequest with key" {
    const allocator = std.testing.allocator;

    const record = Record{
        .key = "user:123",
        .value = "Alice",
    };
    const request = Message{
        .append_request = .{ .record = record },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    const writer = stream.writer();

    try serializeMessage(writer, request, allocator);
    const serialized = stream.getWritten();

    var read_stream = std.io.fixedBufferStream(serialized);
    const reader = read_stream.reader();

    const deserialized = try deserializeMessage(reader, allocator);
    defer {
        if (deserialized.append_request.record.key) |k| allocator.free(k);
        allocator.free(deserialized.append_request.record.value);
    }

    try std.testing.expectEqualStrings("user:123", deserialized.append_request.record.key.?);
    try std.testing.expectEqualStrings("Alice", deserialized.append_request.record.value);
}

test "serializeAppendRequest and deserializeAppendRequest without key" {
    const allocator = std.testing.allocator;

    const record = Record{
        .key = null,
        .value = "NoKeyValue",
    };
    const request = Message{
        .append_request = .{ .record = record },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serializeMessage(stream.writer(), request, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try deserializeMessage(read_stream.reader(), allocator);
    defer allocator.free(deserialized.append_request.record.value);

    try std.testing.expect(deserialized.append_request.record.key == null);
    try std.testing.expectEqualStrings("NoKeyValue", deserialized.append_request.record.value);
}

test "serializeAppendRequest and deserializeAppendRequest large value" {
    const allocator = std.testing.allocator;

    const large_value = try allocator.alloc(u8, 5000);
    defer allocator.free(large_value);
    @memset(large_value, 'X');

    const record = Record{
        .key = "large",
        .value = large_value,
    };
    const request = Message{
        .append_request = .{ .record = record },
    };

    var buffer: [8192]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serializeMessage(stream.writer(), request, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try deserializeMessage(read_stream.reader(), allocator);
    defer {
        if (deserialized.append_request.record.key) |k| allocator.free(k);
        allocator.free(deserialized.append_request.record.value);
    }

    try std.testing.expectEqual(large_value.len, deserialized.append_request.record.value.len);
    try std.testing.expectEqualSlices(u8, large_value, deserialized.append_request.record.value);
}

fn serializeAppendResponse(writer: anytype, res: AppendResponse) !void {
    try writeIntBinary(writer, u64, res.offset, .little);
}

fn deserializeAppendResponse(reader: anytype) !AppendResponse {
    return AppendResponse{
        .offset = try readIntFromBinary(reader, u64, .little),
    };
}

test "serializeAppendResponse and deserializeAppendResponse" {
    const allocator = std.testing.allocator;

    const response = Message{
        .append_response = .{ .offset = 42 },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serializeMessage(stream.writer(), response, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 42), deserialized.append_response.offset);
}

fn serializeReadRequest(writer: anytype, req: ReadRequest) !void {
    try writeIntBinary(writer, u64, req.offset, .little);
}

fn deserializeReadRequest(reader: anytype) !ReadRequest {
    return ReadRequest{
        .offset = try readIntFromBinary(reader, u64, .little),
    };
}

test "serializeReadRequest and deserializeReadRequest" {
    const allocator = std.testing.allocator;

    const request = Message{
        .read_request = .{ .offset = 100 },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serializeMessage(stream.writer(), request, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 100), deserialized.read_request.offset);
}

fn serializeReadResponse(writer: anytype, res: ReadResponse) !void {
    // Same format as AppendRequest record
    if (res.record.key) |key| {
        const key_len: i32 = @intCast(key.len);
        try writeIntBinary(writer, i32, key_len, .little);
        try writer.writeAll(key);
    } else {
        try writeIntBinary(writer, i32, -1, .little);
    }

    const value_len: i32 = @intCast(res.record.value.len);
    try writeIntBinary(writer, i32, value_len, .little);
    try writer.writeAll(res.record.value);
}

fn deserializeReadResponse(reader: anytype, allocator: std.mem.Allocator) !ReadResponse {
    // Same format as AppendRequest
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
        .record = Record{
            .key = key,
            .value = value,
        },
    };
}

test "serializeReadResponse and deserializeReadResponse" {
    const allocator = std.testing.allocator;

    const record = Record{
        .key = "key",
        .value = "value",
    };
    const response = Message{
        .read_response = .{ .record = record },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serializeMessage(stream.writer(), response, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try deserializeMessage(read_stream.reader(), allocator);
    defer {
        if (deserialized.read_response.record.key) |k| allocator.free(k);
        allocator.free(deserialized.read_response.record.value);
    }

    try std.testing.expectEqualStrings("key", deserialized.read_response.record.key.?);
    try std.testing.expectEqualStrings("value", deserialized.read_response.record.value);
}

fn serializeErrorResponse(writer: anytype, err: ErrorResponse) !void {
    const code_byte = @intFromEnum(err.code);
    try writer.writeAll(&[_]u8{code_byte});
    const msg_len: i32 = @intCast(err.message.len);
    try writeIntBinary(writer, i32, msg_len, .little);
    try writer.writeAll(err.message);
}

fn deserializeErrorResponse(reader: anytype, allocator: std.mem.Allocator) !ErrorResponse {
    var code_bytes: [1]u8 = undefined;
    try readFull(reader, &code_bytes);
    const code = std.meta.intToEnum(ErrorCode, code_bytes[0]) catch ErrorCode.unknown;

    const msg_len = try readIntFromBinary(reader, i32, .little);
    if (msg_len < 0) {
        return ProtocolError.DeserializationError;
    }

    const message = try allocator.alloc(u8, @intCast(msg_len));
    try readFull(reader, message);

    return ErrorResponse{
        .code = code,
        .message = message,
    };
}

test "serializeErrorResponse and deserializeErrorResponse" {
    const allocator = std.testing.allocator;

    const response = Message{
        .error_response = .{
            .code = .io_error,
            .message = "Test error message",
        },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serializeMessage(stream.writer(), response, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try deserializeMessage(read_stream.reader(), allocator);
    defer allocator.free(deserialized.error_response.message);

    try std.testing.expectEqual(ErrorCode.io_error, deserialized.error_response.code);
    try std.testing.expectEqualStrings("Test error message", deserialized.error_response.message);
}



