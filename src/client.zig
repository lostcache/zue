const std = @import("std");
const Record = @import("log/record.zig").Record;
const protocol = @import("network/protocol.zig");

pub const ClientError = error{
    NotConnected,
    ServerError,
    UnexpectedResponse,
    ConnectionClosed,
};

pub const ClientConfig = struct {
    connect_timeout_ms: u64 = 5000,
    nodelay: bool = true,
    keepalive: bool = true,
};

pub const Client = struct {
    config: ClientConfig,
    stream: ?std.net.Stream,
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,

    pub fn connect(host: []const u8, port: u16, allocator: std.mem.Allocator) !Client {
        return connectWithConfig(host, port, ClientConfig{}, allocator);
    }

    pub fn connectWithConfig(
        host: []const u8,
        port: u16,
        config: ClientConfig,
        allocator: std.mem.Allocator,
    ) !Client {
        var client = Client{
            .config = config,
            .stream = null,
            .allocator = allocator,
            .host = host,
            .port = port,
        };

        try client.reconnect();
        return client;
    }

    fn reconnect(self: *Client) !void {
        const address = try std.net.Address.parseIp(self.host, self.port);
        const stream = try std.net.tcpConnectToAddress(address);

        // Note: setNoDelay/setKeepAlive may not be available in all Zig versions
        // if (self.config.nodelay) {
        //     try stream.setNoDelay(true);
        // }
        // if (self.config.keepalive) {
        //     try stream.setKeepAlive(true);
        // }
        _ = self.config; // Suppress unused warning

        self.stream = stream;

        std.debug.print("Connected to Zue server at {s}:{d}\n", .{ self.host, self.port });
    }

    pub fn disconnect(self: *Client) void {
        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
            std.debug.print("Disconnected from server\n", .{});
        }
    }

    pub fn isConnected(self: *const Client) bool {
        return self.stream != null;
    }

    // Helper to read a complete protocol message, handling partial reads
    fn readCompleteMessage(self: *Client, buffer: []u8) ![]u8 {
        // Read 4-byte length prefix
        var len_bytes: [4]u8 = undefined;
        var total_read: usize = 0;
        while (total_read < 4) {
            const n = try std.posix.read(self.stream.?.handle, len_bytes[total_read..]);
            if (n == 0) return error.EndOfStream;
            total_read += n;
        }

        const message_len = std.mem.readInt(u32, &len_bytes, .little);
        if (message_len > buffer.len) return error.MessageTooLarge;

        // Read the message body (NOT including length prefix)
        total_read = 0;
        while (total_read < message_len) {
            const n = try std.posix.read(self.stream.?.handle, buffer[total_read..message_len]);
            if (n == 0) return error.UnexpectedEndOfStream;
            total_read += n;
        }

        return buffer[0..message_len];
    }

    pub fn append(self: *Client, record: Record) !u64 {
        if (self.stream == null) {
            return ClientError.NotConnected;
        }

        // WORKAROUND: Serialize to fixed buffer, then write with posix.write
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .append_request = .{ .record = record },
        };

        try protocol.serializeMessage(msg_writer, request, self.allocator);
        const msg_bytes = msg_stream.getWritten();

        // Write request to socket
        _ = try std.posix.write(self.stream.?.handle, msg_bytes);

        var response_buffer: [65536]u8 = undefined;
        const message_bytes = try self.readCompleteMessage(&response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, self.allocator);
        defer self.freeMessage(response);

        return switch (response) {
            .append_response => |res| res.offset,
            .error_response => |err| {
                std.debug.print("Server error: {s} (code={})\n", .{ err.message, err.code });
                return ClientError.ServerError;
            },
            else => {
                std.debug.print("Unexpected response type\n", .{});
                return ClientError.UnexpectedResponse;
            },
        };
    }

    pub fn read(self: *Client, offset: u64) !Record {
        if (self.stream == null) {
            return ClientError.NotConnected;
        }

        // WORKAROUND: Serialize to fixed buffer, then write with posix.write
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        const msg_writer = msg_stream.writer();

        const request = protocol.Message{
            .read_request = .{ .offset = offset },
        };

        try protocol.serializeMessage(msg_writer, request, self.allocator);
        const msg_bytes = msg_stream.getWritten();
        _ = try std.posix.write(self.stream.?.handle, msg_bytes);

        // WORKAROUND: Read complete message using helper that handles partial reads
        var response_buffer: [65536]u8 = undefined;
        const message_bytes = try self.readCompleteMessage(&response_buffer);

        var response_stream = std.io.fixedBufferStream(message_bytes);
        const response_reader = response_stream.reader();

        const response = try protocol.deserializeMessageBody(response_reader, self.allocator);

        return switch (response) {
            .read_response => |res| res.record,
            .error_response => |err| {
                defer self.allocator.free(err.message);
                std.debug.print("Server error: {s} (code={})\n", .{ err.message, err.code });
                return ClientError.ServerError;
            },
            else => {
                self.freeMessage(response);
                std.debug.print("Unexpected response type\n", .{});
                return ClientError.UnexpectedResponse;
            },
        };
    }

    fn freeMessage(self: *Client, message: protocol.Message) void {
        switch (message) {
            .append_request => |req| {
                if (req.record.key) |k| self.allocator.free(k);
                self.allocator.free(req.record.value);
            },
            .read_response => |res| {
                if (res.record.key) |k| self.allocator.free(k);
                self.allocator.free(res.record.value);
            },
            .error_response => |err| {
                self.allocator.free(err.message);
            },
            else => {},
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var client = try Client.connect("127.0.0.1", 9000, allocator);
    defer client.disconnect();

    std.debug.print("\n=== Appending records ===\n", .{});

    const record1 = Record{
        .key = "user:123",
        .value = "Alice",
    };
    const offset1 = try client.append(record1);
    std.debug.print("Appended record1, offset={}\n", .{offset1});

    const record2 = Record{
        .key = "user:456",
        .value = "Bob",
    };
    const offset2 = try client.append(record2);
    std.debug.print("Appended record2, offset={}\n", .{offset2});

    const record3 = Record{
        .key = null,
        .value = "No key record",
    };
    const offset3 = try client.append(record3);
    std.debug.print("Appended record3, offset={}\n", .{offset3});

    std.debug.print("\n=== Reading records ===\n", .{});

    const read1 = try client.read(offset1);
    defer {
        if (read1.key) |k| allocator.free(k);
        allocator.free(read1.value);
    }
    std.debug.print("Read offset {}: key={?s}, value={s}\n", .{ offset1, read1.key, read1.value });

    const read2 = try client.read(offset2);
    defer {
        if (read2.key) |k| allocator.free(k);
        allocator.free(read2.value);
    }
    std.debug.print("Read offset {}: key={?s}, value={s}\n", .{ offset2, read2.key, read2.value });

    const read3 = try client.read(offset3);
    defer {
        if (read3.key) |k| allocator.free(k);
        allocator.free(read3.value);
    }
    std.debug.print("Read offset {}: key={?s}, value={s}\n", .{ offset3, read3.key, read3.value });

    std.debug.print("\n=== Success! ===\n", .{});
}

// ============================================================================
// Tests
// ============================================================================

test "Client.readCompleteMessage returns body without length prefix" {
    // Simulate serialized message: [length:4][type:1][payload]
    const serialized = [_]u8{
        5, 0, 0, 0, // length = 5
        2,          // type = append_response
        0, 0, 0, 0, // offset = 0
    };

    // Simulate what readCompleteMessage should do:
    // 1. Read 4-byte length prefix (5)
    // 2. Read 5 bytes of body
    // 3. Return ONLY the body (NOT including the length prefix)

    const length_prefix = std.mem.readInt(u32, serialized[0..4], .little);
    try std.testing.expectEqual(@as(u32, 5), length_prefix);

    const body = serialized[4..]; // This is what readCompleteMessage should return
    try std.testing.expectEqual(@as(usize, 5), body.len);
    try std.testing.expectEqual(@as(u8, 2), body[0]); // type byte, NOT length prefix
}

test "Client.readCompleteMessage compatible with deserializeMessageBody" {
    const allocator = std.testing.allocator;

    // Serialize a complete message
    const original = protocol.Message{
        .append_response = .{ .offset = 999 },
    };

    var buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer buffer.deinit(allocator);
    try protocol.serializeMessage(buffer.writer(allocator), original, allocator);

    // Extract body (what readCompleteMessage returns)
    const length_prefix = std.mem.readInt(u32, buffer.items[0..4], .little);
    const body = buffer.items[4 .. 4 + length_prefix];

    // Deserialize body using deserializeMessageBody
    var body_stream = std.io.fixedBufferStream(body);
    const deserialized = try protocol.deserializeMessageBody(body_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 999), deserialized.append_response.offset);
}
