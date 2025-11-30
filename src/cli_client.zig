const std = @import("std");
const Client = @import("client.zig").Client;
const Record = @import("log/record.zig").Record;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        printUsage();
        std.process.exit(1);
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "append")) {
        try handleAppend(args, allocator);
    } else if (std.mem.eql(u8, command, "read")) {
        try handleRead(args, allocator);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        printUsage();
        std.process.exit(1);
    }
}

fn handleAppend(args: [][:0]u8, allocator: std.mem.Allocator) !void {
    if (args.len < 6) {
        std.debug.print("Usage: {s} append <host> <port> <key> <value>\n", .{args[0]});
        std.debug.print("   Or: {s} append <host> <port> - <value>  (no key)\n", .{args[0]});
        std.process.exit(1);
    }

    const host = args[2];
    const port = try std.fmt.parseInt(u16, args[3], 10);
    const key = if (std.mem.eql(u8, args[4], "-")) null else args[4];
    const value = args[5];

    var client = try Client.connect(host, port, allocator);
    defer client.disconnect();

    const record = Record{
        .key = key,
        .value = value,
    };
    const offset = try client.append(record);
    std.debug.print("{}\n", .{offset});
}

fn handleRead(args: [][:0]u8, allocator: std.mem.Allocator) !void {
    if (args.len < 5) {
        std.debug.print("Usage: {s} read <host> <port> <offset>\n", .{args[0]});
        std.process.exit(1);
    }

    const host = args[2];
    const port = try std.fmt.parseInt(u16, args[3], 10);
    const offset = try std.fmt.parseInt(u64, args[4], 10);

    var client = try Client.connect(host, port, allocator);
    defer client.disconnect();

    const record = try client.read(offset);
    defer {
        if (record.key) |k| allocator.free(k);
        allocator.free(record.value);
    }
    std.debug.print("{?s}|{s}\n", .{ record.key, record.value });
}

fn printUsage() void {
    std.debug.print("Zue Client CLI\n", .{});
    std.debug.print("\nUsage:\n", .{});
    std.debug.print("  zue-client append <host> <port> <key> <value>\n", .{});
    std.debug.print("  zue-client append <host> <port> - <value>       (no key)\n", .{});
    std.debug.print("  zue-client read <host> <port> <offset>\n", .{});
    std.debug.print("\nExamples:\n", .{});
    std.debug.print("  zue-client append 127.0.0.1 9001 user:123 Alice\n", .{});
    std.debug.print("  zue-client read 127.0.0.1 9001 0\n", .{});
}
