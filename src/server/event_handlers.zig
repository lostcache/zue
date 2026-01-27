const std = @import("std");
const Server = @import("main.zig").Server;
const ClientState = @import("main.zig").ClientState;
const request_handlers = @import("request_handlers.zig");

pub fn onListenReady(ctx: *anyopaque) !void {
    const self: *Server = @ptrCast(@alignCast(ctx));

    const connection = self.listener.?.accept() catch |err| {
        std.debug.print("Error accepting connection: {}\n", .{err});
        return;
    };

    const client_state = try self.allocator.create(ClientState);
    client_state.* = .{
        .fd = connection.stream.handle,
        .read_buffer = undefined,
    };

    try self.clients.put(connection.stream.handle, client_state);
    try self.event_loop.registerSocket(connection.stream.handle, .client);
}

pub fn onClientReady(ctx: *anyopaque, fd: std.posix.fd_t) !void {
    const self: *Server = @ptrCast(@alignCast(ctx));

    const client_state = self.clients.get(fd) orelse {
        std.debug.print("Unknown client fd: {}\n", .{fd});
        return;
    };

    request_handlers.handleClientMessage(self, fd, &client_state.read_buffer) catch |err| {
        switch (err) {
            error.EndOfStream, error.ConnectionResetByPeer => {},
            else => std.debug.print("Error handling client message: {}\n", .{err}),
        }
        std.posix.close(fd);
        _ = self.clients.remove(fd);
        self.allocator.destroy(client_state);
        return err;
    };
}

pub fn onTimer(ctx: *anyopaque) !void {
    const self: *Server = @ptrCast(@alignCast(ctx));

    if (self.leader) |leader| {
        leader.sendHeartbeats();
        _ = leader.tickRepair();
    }
}
