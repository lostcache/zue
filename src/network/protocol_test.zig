const std = @import("std");
const message = @import("message.zig");
const serialization = @import("serialization.zig");
const transport = @import("transport.zig");

const Message = message.Message;
const AppendRequest = message.AppendRequest;
const Record = message.Record;
const AppendResponse = message.AppendResponse;
const ReadRequest = message.ReadRequest;
const ReadResponse = message.ReadResponse;
const ErrorResponse = message.ErrorResponse;
const ErrorCode = message.ErrorCode;
const ReplicatedEntry = message.ReplicatedEntry;
const ProtocolError = serialization.ProtocolError;

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
    try serialization.serializeMessage(serialize_buffer.writer(allocator), original, allocator);

    // Simulate network read
    var body_buffer: [4096]u8 = undefined;
    const message_len = std.mem.readInt(u32, serialize_buffer.items[0..4], .little);
    @memcpy(body_buffer[0..message_len], serialize_buffer.items[4 .. 4 + message_len]);
    const result_body = body_buffer[0..message_len];

    // Deserialize
    var body_stream = std.io.fixedBufferStream(result_body);
    const deserialized = try serialization.deserializeMessageBody(body_stream.reader(), allocator);
    defer {
        if (deserialized.append_request.record.key) |k| allocator.free(k);
        allocator.free(deserialized.append_request.record.value);
    }

    try std.testing.expectEqualStrings("test-key", deserialized.append_request.record.key.?);
    try std.testing.expectEqualStrings("test-value", deserialized.append_request.record.value);
}

test "ReplicateRequest: serialize and deserialize" {
    const allocator = std.testing.allocator;

    var entries = try allocator.alloc(ReplicatedEntry, 2);
    defer allocator.free(entries);
    entries[0] = ReplicatedEntry{
        .offset = 42,
        .record = Record{ .key = "test-key-1", .value = "test-value-1" },
    };
    entries[1] = ReplicatedEntry{
        .offset = 43,
        .record = Record{ .key = "test-key-2", .value = "test-value-2" },
    };

    const original = Message{
        .replicate_request = .{
            .entries = entries,
            .leader_commit = 41,
        },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serialization.serializeMessage(stream.writer(), original, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);
    defer {
        for (deserialized.replicate_request.entries) |entry| {
            if (entry.record.key) |k| allocator.free(k);
            allocator.free(entry.record.value);
        }
        allocator.free(deserialized.replicate_request.entries);
    }

    try std.testing.expectEqual(@as(u64, 41), deserialized.replicate_request.leader_commit);
    try std.testing.expectEqual(@as(usize, 2), deserialized.replicate_request.entries.len);
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
        try serialization.serializeMessage(serialize_buffer.writer(allocator), original, allocator);

        // Extract body
        const message_len = std.mem.readInt(u32, serialize_buffer.items[0..4], .little);
        const body_buffer = serialize_buffer.items[4 .. 4 + message_len];

        var body_stream = std.io.fixedBufferStream(body_buffer);
        const deserialized = try serialization.deserializeMessageBody(body_stream.reader(), allocator);

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

    try serialization.serializeMessage(buffer.writer(allocator), original, allocator);

    // Now mimic network code: read length prefix, get body only
    const message_len = std.mem.readInt(u32, buffer.items[0..4], .little);
    const message_body = buffer.items[4 .. 4 + message_len];

    // Deserialize the body using deserializeMessageBody
    var body_stream = std.io.fixedBufferStream(message_body);
    const deserialized = try serialization.deserializeMessageBody(body_stream.reader(), allocator);
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
        try serialization.serializeMessage(stream.writer(), msg, allocator);

        var read_stream = std.io.fixedBufferStream(stream.getWritten());
        const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);

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
    const huge_value = try allocator.alloc(u8, serialization.MAX_MESSAGE_SIZE);
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

    const result = serialization.serializeMessage(buffer.writer(allocator), original, allocator);
    try std.testing.expectError(ProtocolError.MessageTooLarge, result);
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

    try serialization.serializeMessage(writer, request, allocator);
    const serialized = stream.getWritten();

    var read_stream = std.io.fixedBufferStream(serialized);
    const reader = read_stream.reader();

    const deserialized = try serialization.deserializeMessage(reader, allocator);
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
    try serialization.serializeMessage(stream.writer(), request, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);
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
    try serialization.serializeMessage(stream.writer(), request, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);
    defer {
        if (deserialized.append_request.record.key) |k| allocator.free(k);
        allocator.free(deserialized.append_request.record.value);
    }

    try std.testing.expectEqual(large_value.len, deserialized.append_request.record.value.len);
    try std.testing.expectEqualSlices(u8, large_value, deserialized.append_request.record.value);
}

test "serializeAppendResponse and deserializeAppendResponse" {
    const allocator = std.testing.allocator;

    const response = Message{
        .append_response = .{ .offset = 42 },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serialization.serializeMessage(stream.writer(), response, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 42), deserialized.append_response.offset);
}

test "serializeReadRequest and deserializeReadRequest" {
    const allocator = std.testing.allocator;

    const request = Message{
        .read_request = .{ .offset = 100 },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serialization.serializeMessage(stream.writer(), request, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 100), deserialized.read_request.offset);
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
    try serialization.serializeMessage(stream.writer(), response, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);
    defer {
        if (deserialized.read_response.record.key) |k| allocator.free(k);
        allocator.free(deserialized.read_response.record.value);
    }

    try std.testing.expectEqualStrings("key", deserialized.read_response.record.key.?);
    try std.testing.expectEqualStrings("value", deserialized.read_response.record.value);
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
    try serialization.serializeMessage(stream.writer(), response, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);
    defer allocator.free(deserialized.error_response.message);

    try std.testing.expectEqual(ErrorCode.io_error, deserialized.error_response.code);
    try std.testing.expectEqualStrings("Test error message", deserialized.error_response.message);
}

test "ReplicateResponse: serialize and deserialize" {
    const allocator = std.testing.allocator;

    const original = Message{
        .replicate_response = .{
            .success = true,
            .follower_offset = 42,
            .error_code = .unknown,
        },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serialization.serializeMessage(stream.writer(), original, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(true, deserialized.replicate_response.success);
    try std.testing.expectEqual(@as(u64, 42), deserialized.replicate_response.follower_offset);
}

test "HeartbeatRequest: serialize and deserialize" {
    const allocator = std.testing.allocator;

    const original = Message{
        .heartbeat_request = .{
            .leader_commit = 100,
            .leader_offset = 101,
        },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serialization.serializeMessage(stream.writer(), original, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 100), deserialized.heartbeat_request.leader_commit);
    try std.testing.expectEqual(@as(u64, 101), deserialized.heartbeat_request.leader_offset);
}

test "HeartbeatResponse: serialize and deserialize" {
    const allocator = std.testing.allocator;

    const original = Message{
        .heartbeat_response = .{
            .follower_offset = 99,
        },
    };

    var buffer: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try serialization.serializeMessage(stream.writer(), original, allocator);

    var read_stream = std.io.fixedBufferStream(stream.getWritten());
    const deserialized = try serialization.deserializeMessage(read_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 99), deserialized.heartbeat_response.follower_offset);
}