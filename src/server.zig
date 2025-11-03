const std = @import("std");
const Log = @import("log/log.zig").Log;
const LogConfig = @import("log/log.zig").LogConfig;
const SegmentConfig = @import("log/segment.zig").SegmentConfig;
const Protocol = @import("network/protocol.zig");
const Record = @import("log/record.zig").Record;
const EventLoop = @import("event_loop.zig").EventLoop;
const EventHandlers = @import("event_loop.zig").EventHandlers;

pub const ServerConfig = struct {
    port: u16 = 9000,
    address: []const u8 = "0.0.0.0",
    backlog: u31 = 128,
    keepalive: bool = true,
    nodelay: bool = true,
};

const ClientState = struct {
    fd: std.posix.fd_t,
    read_buffer: [65536]u8,
};

pub const Server = struct {
    config: ServerConfig,
    log: *Log,
    allocator: std.mem.Allocator,
    listener: ?std.net.Server,
    running: bool,
    event_loop: EventLoop,
    clients: std.AutoHashMap(std.posix.fd_t, *ClientState),

    pub fn init(config: ServerConfig, log: *Log, allocator: std.mem.Allocator) Server {
        return Server{
            .config = config,
            .log = log,
            .allocator = allocator,
            .listener = null,
            .running = false,
            .event_loop = EventLoop.init(allocator, 2000),
            .clients = std.AutoHashMap(std.posix.fd_t, *ClientState).init(allocator),
        };
    }

    pub fn start(self: *Server) !void {
        const address = try std.net.Address.parseIp(self.config.address, self.config.port);

        self.listener = try address.listen(.{
            .reuse_address = true,
            .kernel_backlog = self.config.backlog,
        });

        self.running = true;

        std.debug.print("Zue server listening on {s}:{d}\n", .{ self.config.address, self.config.port });

        try self.event_loop.registerSocket(self.listener.?.stream.handle, .listener);

        const handlers = EventHandlers{
            .onListenReady = onListenReady,
            .onClientReady = onClientReady,
            .onTimer = onTimer,
            .context = self,
        };

        while (self.running) {
            try self.event_loop.run(handlers, 100);
        }
    }

    pub fn stop(self: *Server) void {
        self.running = false;

        var it = self.clients.iterator();
        while (it.next()) |entry| {
            std.posix.close(entry.key_ptr.*);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.clients.deinit();

        self.event_loop.deinit();

        if (self.listener) |*listener| {
            listener.deinit();
            self.listener = null;
        }
    }

    // ========================================================================
    // Event Handlers
    // ========================================================================

    fn onListenReady(ctx: *anyopaque) !void {
        const self: *Server = @ptrCast(@alignCast(ctx));

        const connection = self.listener.?.accept() catch |err| {
            std.debug.print("Error accepting connection: {}\n", .{err});
            return;
        };

        std.debug.print("Client connected from {any}\n", .{connection.address});

        const client_state = try self.allocator.create(ClientState);
        client_state.* = .{
            .fd = connection.stream.handle,
            .read_buffer = undefined,
        };

        try self.clients.put(connection.stream.handle, client_state);

        try self.event_loop.registerSocket(connection.stream.handle, .client);
    }

    fn onClientReady(ctx: *anyopaque, fd: std.posix.fd_t) !void {
        const self: *Server = @ptrCast(@alignCast(ctx));

        const client_state = self.clients.get(fd) orelse {
            std.debug.print("Unknown client fd: {}\n", .{fd});
            return;
        };

        self.handleClientMessage(fd, &client_state.read_buffer) catch |err| {
            switch (err) {
                error.EndOfStream, error.ConnectionResetByPeer => {
                    std.debug.print("Client disconnected\n", .{});
                },
                else => {
                    std.debug.print("Error handling client message: {}\n", .{err});
                },
            }
            // Clean up client resources
            // NOTE: event_loop.unregisterSocket() is called automatically by EventLoop.run()
            // when this handler returns an error. We just handle our own cleanup here.
            std.posix.close(fd);
            _ = self.clients.remove(fd);
            self.allocator.destroy(client_state);

            // Return error to signal event loop to unregister this socket
            return err;
        };
    }

    fn onTimer(ctx: *anyopaque) !void {
        _ = ctx;
        std.debug.print("[TIMER] Event loop tick (2s interval)\n", .{});
    }

    fn readCompleteMessage(socket_handle: std.posix.socket_t, buffer: []u8) ![]u8 {

        var len_bytes: [4]u8 = undefined;
        var total_read: usize = 0;
        while (total_read < 4) {
            const n = try std.posix.read(socket_handle, len_bytes[total_read..]);
            if (n == 0) return error.EndOfStream;
            total_read += n;
        }

        const message_len = std.mem.readInt(u32, &len_bytes, .little);

        if (message_len > buffer.len) return error.MessageTooLarge;

        total_read = 0;
        while (total_read < message_len) {
            const n = try std.posix.read(socket_handle, buffer[total_read..message_len]);
            if (n == 0) return error.UnexpectedEndOfStream;
            total_read += n;
        }

        return buffer[0..message_len];
    }

    fn handleClientMessage(self: *Server, socket_handle: std.posix.socket_t, read_buffer: []u8) !void {
        const message_bytes = try readCompleteMessage(socket_handle, read_buffer);

        var request_stream = std.io.fixedBufferStream(message_bytes);
        const request_reader = request_stream.reader();

        const request = Protocol.deserializeMessageBody(request_reader, self.allocator) catch |err| {
            std.debug.print("Error deserializing message: {}\n", .{err});
            try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Failed to deserialize message");
            return err;
        };
        defer self.freeMessage(request);

        try self.processRequest(request, socket_handle);
    }

    fn processRequest(self: *Server, request: Protocol.Message, socket_handle: std.posix.socket_t) !void {
        switch (request) {
            .append_request => |req| {
                std.debug.print("Processing append request (key={?s}, value_len={})\n", .{
                    req.record.key,
                    req.record.value.len,
                });

                const offset = self.log.append(req.record) catch |err| {
                    std.debug.print("Error appending to log: {}\n", .{err});
                    try self.sendErrorResponseDirect(socket_handle, .io_error, "Failed to append record");
                    return;
                };

                // WORKAROUND: Serialize to buffer then write with posix.write
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .append_response = .{ .offset = offset },
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                std.debug.print("Append successful, offset={}\n", .{offset});
            },

            .read_request => |req| {
                std.debug.print("Processing read request (offset={})\n", .{req.offset});

                const record = self.log.read(req.offset, self.allocator) catch |err| {
                    std.debug.print("Error reading from log: {}\n", .{err});

                    const error_code: Protocol.ErrorCode = switch (err) {
                        error.OffsetOutOfBounds => .invalid_offset,
                        else => .io_error,
                    };

                    try self.sendErrorResponseDirect(socket_handle, error_code, "Failed to read record");
                    return;
                };
                defer {
                    if (record.key) |k| self.allocator.free(k);
                    self.allocator.free(record.value);
                }

                // WORKAROUND: Serialize to buffer then write with posix.write
                var msg_buffer: [65536]u8 = undefined;
                var msg_stream = std.io.fixedBufferStream(&msg_buffer);
                try Protocol.serializeMessage(msg_stream.writer(), Protocol.Message{
                    .read_response = .{ .record = record },
                }, self.allocator);
                _ = try std.posix.write(socket_handle, msg_stream.getWritten());

                std.debug.print("Read successful (key={?s}, value_len={})\n", .{
                    record.key,
                    record.value.len,
                });
            },

            else => {
                std.debug.print("Unexpected message type from client\n", .{});
                try self.sendErrorResponseDirect(socket_handle, .invalid_message, "Unexpected message type");
            },
        }
    }

    fn sendErrorResponseDirect(self: *Server, socket_handle: std.posix.socket_t, code: Protocol.ErrorCode, message: []const u8) !void {
        const response = Protocol.Message{
            .error_response = Protocol.ErrorResponse{
                .code = code,
                .message = message,
            },
        };
        // WORKAROUND: Serialize to buffer then write with posix.write
        var msg_buffer: [65536]u8 = undefined;
        var msg_stream = std.io.fixedBufferStream(&msg_buffer);
        try Protocol.serializeMessage(msg_stream.writer(), response, self.allocator);
        _ = try std.posix.write(socket_handle, msg_stream.getWritten());
    }

    fn freeMessage(self: *Server, message: Protocol.Message) void {
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

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var port: u16 = 9000; // Default port
    if (args.len > 1) {
        port = std.fmt.parseInt(u16, args[1], 10) catch blk: {
            std.debug.print("Invalid port argument: {s}, using default 9000\n", .{args[1]});
            break :blk 9000;
        };
    }

    const log_dir = if (args.len > 2) args[2] else "/tmp/zue_server_test";

    const log_config = LogConfig{
        .segment_config = SegmentConfig.default(),
    };
    var log = try Log.create(log_config, log_dir, allocator);
    defer log.close();

    const server_config = ServerConfig{
        .port = port,
    };
    var server = Server.init(server_config, &log, allocator);

    std.debug.print("Starting Zue server on port {d} with log directory {s}...\n", .{ port, log_dir });
    try server.start();
}

// ============================================================================
// Tests
// ============================================================================

test "Server.readCompleteMessage returns body without length prefix" {
    // Simulate serialized message
    const serialized = [_]u8{
        10, 0, 0, 0, // length = 10
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // body
    };

    const length_prefix = std.mem.readInt(u32, serialized[0..4], .little);
    try std.testing.expectEqual(@as(u32, 10), length_prefix);

    const body = serialized[4..];
    try std.testing.expectEqual(@as(usize, 10), body.len);
    try std.testing.expectEqual(@as(u8, 1), body[0]); // First byte of body, NOT length
}

test "Server.readCompleteMessage compatible with deserializeMessageBody" {
    const allocator = std.testing.allocator;

    const original = Protocol.Message{
        .read_request = .{ .offset = 555 },
    };

    var buffer = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer buffer.deinit(allocator);
    try Protocol.serializeMessage(buffer.writer(allocator), original, allocator);

    // Extract body
    const length_prefix = std.mem.readInt(u32, buffer.items[0..4], .little);
    const body = buffer.items[4 .. 4 + length_prefix];

    // Deserialize
    var body_stream = std.io.fixedBufferStream(body);
    const deserialized = try Protocol.deserializeMessageBody(body_stream.reader(), allocator);

    try std.testing.expectEqual(@as(u64, 555), deserialized.read_request.offset);
}
