const std = @import("std");
const testing = std.testing;
const net = std.net;

const Client = @import("client.zig").Client;
const Record = @import("log/record.zig").Record;

// ============================================================================
// Test Configuration
// ============================================================================

const TEST_TIMEOUT_MS = 5_000;

const SERVER_START_TIMEOUT_MS = 1000;

/// Configuration for test behavior
const TestConfig = struct {
    capture_server_output: bool = false,
    timeout_ms: u64 = TEST_TIMEOUT_MS,
};

// ============================================================================
// Test Helpers
// ============================================================================

/// Finds a free ephemeral port by binding to port 0 and letting the OS assign one.
/// Returns the port number assigned by the OS.
fn findFreePort() !u16 {
    const address = try net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    return server.listen_address.in.getPort();
}

/// Waits for a TCP port to become available with timeout.
/// This is how we know the server is ready.
fn waitForServer(port: u16) !void {
    const max_retries = 10;
    const retry_delay_ms = 100;
    var retries: u8 = 0;

    while (retries < max_retries) : (retries += 1) {
        const address = net.Address.parseIp("127.0.0.1", port) catch continue;
        const stream = net.tcpConnectToAddress(address) catch {
            // Connection failed, so server is not ready yet.
            std.Thread.sleep(retry_delay_ms * std.time.ns_per_ms);
            continue;
        };
        // If we get here, connection succeeded. The port is open.
        stream.close();
        return;
    }
    return error.ServerDidNotStart;
}

const ServerProcess = struct {
    process: std.process.Child,
    port: u16,
    tmp_dir: testing.TmpDir,
    tmp_path: []const u8,
    allocator: std.mem.Allocator,
    config: TestConfig,

    fn init(allocator: std.mem.Allocator, config: TestConfig) !ServerProcess {
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();

        const port = try findFreePort();
        const port_str = try std.fmt.allocPrint(allocator, "{d}", .{port});
        defer allocator.free(port_str);

        const tmp_path = try tmp.dir.realpathAlloc(allocator, ".");
        errdefer allocator.free(tmp_path);

        const args = [_][]const u8{ "zig-out/bin/zue-server", port_str, tmp_path };

        var server_process = std.process.Child.init(&args, allocator);

        // Configure output based on test config
        if (config.capture_server_output) {
            server_process.stdout_behavior = .Pipe;
            server_process.stderr_behavior = .Pipe;
        } else {
            server_process.stdout_behavior = .Ignore;
            server_process.stderr_behavior = .Ignore;
        }

        try server_process.spawn();
        errdefer {
            _ = server_process.kill() catch {};
            _ = server_process.wait() catch {};
        }

        // Wait for server to be ready
        waitForServer(port) catch |err| {
            std.debug.print("Server failed to start on port {d}: {}\n", .{ port, err });
            return err;
        };

        return ServerProcess{
            .process = server_process,
            .port = port,
            .tmp_dir = tmp,
            .tmp_path = tmp_path,
            .allocator = allocator,
            .config = config,
        };
    }

    fn deinit(self: *ServerProcess) void {
        _ = self.process.kill() catch {};
        _ = self.process.wait() catch {};
        self.allocator.free(self.tmp_path);
        self.tmp_dir.cleanup();
    }

    /// Helper to get a client connection (ensures single client pattern)
    fn getClient(self: *ServerProcess) !Client {
        return try Client.connect("127.0.0.1", self.port, self.allocator);
    }
};

// ============================================================================
// Integration Tests
// ============================================================================

test "integration: basic append and read over TCP" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // Single client connection for this test
    var client = try server.getClient();
    defer client.disconnect();

    const record = Record{
        .key = "test-key",
        .value = "test-value",
    };
    const offset = try client.append(record);
    try testing.expect(offset == 0);

    const read_record = try client.read(offset);
    defer {
        if (read_record.key) |k| allocator.free(k);
        allocator.free(read_record.value);
    }

    try testing.expectEqualStrings("test-key", read_record.key.?);
    try testing.expectEqualStrings("test-value", read_record.value);
}

test "integration: append record without key" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    var client = try server.getClient();
    defer client.disconnect();

    const record = Record{
        .key = null,
        .value = "value-only",
    };
    const offset = try client.append(record);

    const read_record = try client.read(offset);
    defer allocator.free(read_record.value);

    try testing.expect(read_record.key == null);
    try testing.expectEqualStrings("value-only", read_record.value);
}

test "integration: large record over TCP (10KB)" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    var client = try server.getClient();
    defer client.disconnect();

    // Create a large value (10KB)
    const large_value = try allocator.alloc(u8, 10 * 1024);
    defer allocator.free(large_value);
    @memset(large_value, 'X');

    const record = Record{
        .key = "large-key",
        .value = large_value,
    };
    const offset = try client.append(record);

    const read_record = try client.read(offset);
    defer {
        if (read_record.key) |k| allocator.free(k);
        allocator.free(read_record.value);
    }

    try testing.expectEqualStrings("large-key", read_record.key.?);
    try testing.expectEqual(large_value.len, read_record.value.len);
    try testing.expectEqualSlices(u8, large_value, read_record.value);
}

test "integration: buffer boundary - 50KB record" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    var client = try server.getClient();
    defer client.disconnect();

    // Test near the 64KB buffer limit (50KB should work comfortably)
    const large_value = try allocator.alloc(u8, 50 * 1024);
    defer allocator.free(large_value);
    @memset(large_value, 'Y');

    const record = Record{
        .key = "boundary-test",
        .value = large_value,
    };
    const offset = try client.append(record);

    const read_record = try client.read(offset);
    defer {
        if (read_record.key) |k| allocator.free(k);
        allocator.free(read_record.value);
    }

    try testing.expectEqualStrings("boundary-test", read_record.key.?);
    try testing.expectEqual(large_value.len, read_record.value.len);
    try testing.expectEqualSlices(u8, large_value, read_record.value);
}

test "integration: buffer boundary - exactly 60KB record" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    var client = try server.getClient();
    defer client.disconnect();

    // Test closer to the 64KB limit (60KB with overhead should still fit)
    const large_value = try allocator.alloc(u8, 60 * 1024);
    defer allocator.free(large_value);
    @memset(large_value, 'Z');

    const record = Record{
        .key = "limit-test",
        .value = large_value,
    };
    const offset = try client.append(record);

    const read_record = try client.read(offset);
    defer {
        if (read_record.key) |k| allocator.free(k);
        allocator.free(read_record.value);
    }

    try testing.expectEqualStrings("limit-test", read_record.key.?);
    try testing.expectEqual(large_value.len, read_record.value.len);
    try testing.expectEqualSlices(u8, large_value, read_record.value);
}

test "integration: stateful sequence - multiple appends and reads (single connection)" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // Single client performing multiple operations
    var client = try server.getClient();
    defer client.disconnect();

    // Append three records
    const record1 = Record{ .key = "key1", .value = "value1" };
    const offset1 = try client.append(record1);

    const record2 = Record{ .key = "key2", .value = "value2" };
    const offset2 = try client.append(record2);

    const record3 = Record{ .key = "key3", .value = "value3" };
    const offset3 = try client.append(record3);

    // Read them back in non-sequential order
    {
        const read1 = try client.read(offset1);
        defer {
            if (read1.key) |k| allocator.free(k);
            allocator.free(read1.value);
        }
        try testing.expectEqualStrings("key1", read1.key.?);
        try testing.expectEqualStrings("value1", read1.value);
    }

    {
        const read3 = try client.read(offset3);
        defer {
            if (read3.key) |k| allocator.free(k);
            allocator.free(read3.value);
        }
        try testing.expectEqualStrings("key3", read3.key.?);
        try testing.expectEqualStrings("value3", read3.value);
    }

    {
        const read2 = try client.read(offset2);
        defer {
            if (read2.key) |k| allocator.free(k);
            allocator.free(read2.value);
        }
        try testing.expectEqualStrings("key2", read2.key.?);
        try testing.expectEqualStrings("value2", read2.value);
    }
}

test "integration: read invalid offset returns error" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    var client = try server.getClient();
    defer client.disconnect();

    // Try to read from offset that doesn't exist
    const result = client.read(999999);
    try testing.expectError(error.ServerError, result);
}

test "integration: multiple sequential clients (single-threaded server pattern)" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // CRITICAL: Single-threaded server means ONE client at a time.
    // Each client MUST disconnect before the next can connect.

    // Client 1: connects, writes, DISCONNECTS
    var offset1: u64 = undefined;
    {
        var client1 = try server.getClient();
        defer client1.disconnect();

        const record1 = Record{ .key = "client1", .value = "data1" };
        offset1 = try client1.append(record1);
        // client1.disconnect() called here by defer
    }

    // Client 2: connects AFTER client1 disconnected, writes and reads
    var offset2: u64 = undefined;
    {
        var client2 = try server.getClient();
        defer client2.disconnect();

        const record2 = Record{ .key = "client2", .value = "data2" };
        offset2 = try client2.append(record2);

        // Verify persistence: client2 can read client1's data
        const read1 = try client2.read(offset1);
        defer {
            if (read1.key) |k| allocator.free(k);
            allocator.free(read1.value);
        }
        try testing.expectEqualStrings("client1", read1.key.?);
        try testing.expectEqualStrings("data1", read1.value);
        // client2.disconnect() called here by defer
    }

    // Client 3: connects and reads data from both previous clients
    {
        var client3 = try server.getClient();
        defer client3.disconnect();

        // Read client1's data
        {
            const read1 = try client3.read(offset1);
            defer {
                if (read1.key) |k| allocator.free(k);
                allocator.free(read1.value);
            }
            try testing.expectEqualStrings("client1", read1.key.?);
            try testing.expectEqualStrings("data1", read1.value);
        }

        // Read client2's data
        {
            const read2 = try client3.read(offset2);
            defer {
                if (read2.key) |k| allocator.free(k);
                allocator.free(read2.value);
            }
            try testing.expectEqualStrings("client2", read2.key.?);
            try testing.expectEqualStrings("data2", read2.value);
        }
    }
}

test "integration: client disconnect and reconnect" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // First connection: write and disconnect
    var offset1: u64 = undefined;
    {
        var client = try server.getClient();
        defer client.disconnect();

        const record1 = Record{ .key = "before", .value = "disconnect" };
        offset1 = try client.append(record1);
    }

    // Reconnect: read old data and write new data
    {
        var client = try server.getClient();
        defer client.disconnect();

        // Should be able to read old data
        const read_record = try client.read(offset1);
        defer {
            if (read_record.key) |k| allocator.free(k);
            allocator.free(read_record.value);
        }

        try testing.expectEqualStrings("before", read_record.key.?);
        try testing.expectEqualStrings("disconnect", read_record.value);

        // And write new data
        const record2 = Record{ .key = "after", .value = "reconnect" };
        const offset2 = try client.append(record2);
        try testing.expect(offset2 > offset1);
    }
}

// ============================================================================
// Phase 1.5: Event Loop Tests
// ============================================================================

test "integration: concurrent clients - event-driven server" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // Connect multiple clients simultaneously
    var client1 = try server.getClient();
    defer client1.disconnect();

    var client2 = try server.getClient();
    defer client2.disconnect();

    var client3 = try server.getClient();
    defer client3.disconnect();

    // All three clients can write concurrently
    const record1 = Record{ .key = "client1", .value = "concurrent-1" };
    const offset1 = try client1.append(record1);

    const record2 = Record{ .key = "client2", .value = "concurrent-2" };
    const offset2 = try client2.append(record2);

    const record3 = Record{ .key = "client3", .value = "concurrent-3" };
    const offset3 = try client3.append(record3);

    // Each client can read any offset
    {
        const read = try client1.read(offset2);
        defer {
            if (read.key) |k| allocator.free(k);
            allocator.free(read.value);
        }
        try testing.expectEqualStrings("client2", read.key.?);
    }

    {
        const read = try client2.read(offset3);
        defer {
            if (read.key) |k| allocator.free(k);
            allocator.free(read.value);
        }
        try testing.expectEqualStrings("client3", read.key.?);
    }

    {
        const read = try client3.read(offset1);
        defer {
            if (read.key) |k| allocator.free(k);
            allocator.free(read.value);
        }
        try testing.expectEqualStrings("client1", read.key.?);
    }
}

test "integration: interleaved operations from multiple clients" {
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // Connect two clients
    var client1 = try server.getClient();
    defer client1.disconnect();

    var client2 = try server.getClient();
    defer client2.disconnect();

    // Interleave operations: client1 write, client2 write, client1 read, client2 read
    const record1 = Record{ .key = "interleaved1", .value = "data1" };
    const offset1 = try client1.append(record1);

    const record2 = Record{ .key = "interleaved2", .value = "data2" };
    const offset2 = try client2.append(record2);

    // Client1 reads client2's data
    {
        const read = try client1.read(offset2);
        defer {
            if (read.key) |k| allocator.free(k);
            allocator.free(read.value);
        }
        try testing.expectEqualStrings("interleaved2", read.key.?);
        try testing.expectEqualStrings("data2", read.value);
    }

    // Client2 reads client1's data
    {
        const read = try client2.read(offset1);
        defer {
            if (read.key) |k| allocator.free(k);
            allocator.free(read.value);
        }
        try testing.expectEqualStrings("interleaved1", read.key.?);
        try testing.expectEqualStrings("data1", read.value);
    }
}

test "integration: rapid client connect/disconnect - regression test for collection modification bug" {
    // This test specifically validates the fix for the critical bug where
    // unregisterSocket was called during iteration, causing undefined behavior.
    const allocator = testing.allocator;
    const config = TestConfig{};

    var server = try ServerProcess.init(allocator, config);
    defer server.deinit();

    // Rapidly connect and disconnect multiple clients
    // This would trigger the bug if unregisterSocket modifies the list during iteration
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var client = try server.getClient();
        const record = Record{ .key = "rapid", .value = "test" };
        _ = try client.append(record);
        client.disconnect(); // This triggers unregisterSocket
    }

    // Verify server is still healthy by connecting a new client
    var final_client = try server.getClient();
    defer final_client.disconnect();

    const record = Record{ .key = "final", .value = "healthy" };
    const offset = try final_client.append(record);

    const read = try final_client.read(offset);
    defer {
        if (read.key) |k| allocator.free(k);
        allocator.free(read.value);
    }
    try testing.expectEqualStrings("final", read.key.?);
    try testing.expectEqualStrings("healthy", read.value);
}
