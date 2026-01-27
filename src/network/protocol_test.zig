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
