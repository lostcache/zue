/// Uncommitted Write Buffer
///
/// Manages writes that have been appended to the leader's local log but not yet
/// replicated to a quorum of followers. This is part of Phase 2's simplified
/// replication approach.
///
/// Key behaviors:
/// - Tracks writes awaiting quorum acknowledgment
/// - Provides timeout-based cleanup for stale writes
/// - Does NOT rollback the log (deferred to Phase 3)
/// - Lost writes on leader crash are acceptable for Phase 2
const std = @import("std");
const Record = @import("../log/record.zig").Record;

/// An uncommitted write entry
/// IMPORTANT: This struct OWNS the record data to avoid use-after-free bugs
pub const UncommittedWrite = struct {
    offset: u64,
    /// Owned copy of key data (allocated)
    key: ?[]u8,
    /// Owned copy of value data (allocated)
    value: []u8,
    ack_count: u32,        // How many acks received (starts at 1 for leader)
    timestamp_ms: i64,     // When was this write initiated
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, offset: u64, record: Record, now_ms: i64) !UncommittedWrite {
        // Copy key data
        const key_copy = if (record.key) |k|
            try allocator.dupe(u8, k)
        else
            null;

        // Copy value data
        const value_copy = try allocator.dupe(u8, record.value);

        return UncommittedWrite{
            .offset = offset,
            .key = key_copy,
            .value = value_copy,
            .ack_count = 1,  // Leader counts as first ack
            .timestamp_ms = now_ms,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *UncommittedWrite) void {
        if (self.key) |k| {
            self.allocator.free(k);
        }
        self.allocator.free(self.value);
    }

    /// Convert owned data back to a Record struct (for reading)
    pub fn toRecord(self: *const UncommittedWrite) Record {
        return Record{
            .key = self.key,
            .value = self.value,
        };
    }

    pub fn isTimedOut(self: *const UncommittedWrite, now_ms: i64, timeout_ms: u64) bool {
        const elapsed = now_ms - self.timestamp_ms;
        return elapsed >= @as(i64, @intCast(timeout_ms));
    }
};

/// Buffer for managing uncommitted writes
/// Uses HashMap for O(1) lookup and removal by offset
pub const UncommittedBuffer = struct {
    writes: std.AutoHashMap(u64, UncommittedWrite),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) UncommittedBuffer {
        return UncommittedBuffer{
            .writes = std.AutoHashMap(u64, UncommittedWrite).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *UncommittedBuffer) void {
        // Free all owned record data first
        var it = self.writes.valueIterator();
        while (it.next()) |write| {
            write.deinit();
        }
        self.writes.deinit();
    }

    /// Add a new uncommitted write (starts with ack_count = 1 for self)
    /// The record data is COPIED, so caller maintains ownership of the input
    /// O(1) operation
    pub fn add(
        self: *UncommittedBuffer,
        offset: u64,
        record: Record,
        now_ms: i64,
    ) !void {
        var write = try UncommittedWrite.init(self.allocator, offset, record, now_ms);
        errdefer write.deinit();
        try self.writes.put(offset, write);
    }

    /// Increment ack count for a specific offset
    /// Returns true if the write now has enough acks to commit
    /// O(1) operation
    pub fn acknowledgeWrite(
        self: *UncommittedBuffer,
        offset: u64,
        quorum_size: u32,
    ) !bool {
        const write_ptr = self.writes.getPtr(offset) orelse return error.WriteNotFound;
        write_ptr.ack_count += 1;
        return write_ptr.ack_count >= quorum_size;
    }

    /// Remove a write from the buffer (called after commit or timeout)
    /// IMPORTANT: This frees the owned record data
    /// O(1) operation
    pub fn remove(self: *UncommittedBuffer, offset: u64) !void {
        var kv = self.writes.fetchRemove(offset) orelse return error.WriteNotFound;
        kv.value.deinit();  // Free owned data
    }

    /// Get a write by offset
    /// O(1) operation
    pub fn get(self: *UncommittedBuffer, offset: u64) ?*UncommittedWrite {
        return self.writes.getPtr(offset);
    }

    /// Clean up writes that have exceeded the timeout
    /// Returns the number of writes removed
    /// IMPORTANT: This frees the owned record data for timed-out writes
    /// O(n) operation
    pub fn cleanupTimedOut(
        self: *UncommittedBuffer,
        now_ms: i64,
        timeout_ms: u64,
    ) !usize {
        var removed: usize = 0;
        var to_remove: std.ArrayList(u64) = .{};
        defer to_remove.deinit(self.allocator);

        var it = self.writes.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.isTimedOut(now_ms, timeout_ms)) {
                try to_remove.append(self.allocator, entry.key_ptr.*);
            }
        }

        for (to_remove.items) |offset| {
            try self.remove(offset);
            removed += 1;
        }

        return removed;
    }

    /// Get the oldest uncommitted write (if any)
    /// O(n) operation - only used for monitoring/debugging
    pub fn getOldest(self: *UncommittedBuffer) ?*UncommittedWrite {
        if (self.writes.count() == 0) return null;

        var oldest_offset: u64 = std.math.maxInt(u64);
        var oldest_time: i64 = std.math.maxInt(i64);

        var it = self.writes.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.timestamp_ms < oldest_time) {
                oldest_time = entry.value_ptr.timestamp_ms;
                oldest_offset = entry.key_ptr.*;
            }
        }

        return self.writes.getPtr(oldest_offset);
    }

    /// Get the number of uncommitted writes
    /// O(1) operation
    pub fn count(self: *const UncommittedBuffer) usize {
        return self.writes.count();
    }

    /// Clear all uncommitted writes (useful for testing or reset)
    /// IMPORTANT: This frees all owned record data
    pub fn clear(self: *UncommittedBuffer) void {
        var it = self.writes.valueIterator();
        while (it.next()) |write| {
            write.deinit();
        }
        self.writes.clearRetainingCapacity();
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "UncommittedBuffer: init and deinit" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    try std.testing.expectEqual(@as(usize, 0), buffer.count());
}

test "UncommittedBuffer: add and get" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    const record = Record{ .key = "test", .value = "value" };
    const now = std.time.milliTimestamp();
    try buffer.add(100, record, now);

    try std.testing.expectEqual(@as(usize, 1), buffer.count());

    const write = buffer.get(100);
    try std.testing.expect(write != null);
    try std.testing.expectEqual(@as(u64, 100), write.?.offset);
    try std.testing.expectEqual(@as(u32, 1), write.?.ack_count);
}

test "UncommittedBuffer: acknowledgeWrite increments ack count" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    const record = Record{ .key = "test", .value = "value" };
    const now = std.time.milliTimestamp();
    try buffer.add(100, record, now);

    // Initially has 1 ack (leader)
    const write1 = buffer.get(100);
    try std.testing.expectEqual(@as(u32, 1), write1.?.ack_count);

    // Acknowledge from follower 1 (quorum = 3, so not ready yet)
    const ready1 = try buffer.acknowledgeWrite(100, 3);
    try std.testing.expectEqual(false, ready1);
    try std.testing.expectEqual(@as(u32, 2), write1.?.ack_count);

    // Acknowledge from follower 2 (now we have quorum)
    const ready2 = try buffer.acknowledgeWrite(100, 3);
    try std.testing.expectEqual(true, ready2);
    try std.testing.expectEqual(@as(u32, 3), write1.?.ack_count);
}

test "UncommittedBuffer: acknowledgeWrite on non-existent offset returns error" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    try std.testing.expectError(error.WriteNotFound, buffer.acknowledgeWrite(999, 2));
}

test "UncommittedBuffer: remove" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    const record = Record{ .key = "test", .value = "value" };
    const now = std.time.milliTimestamp();
    try buffer.add(100, record, now);
    try buffer.add(101, record, now);

    try std.testing.expectEqual(@as(usize, 2), buffer.count());

    try buffer.remove(100);
    try std.testing.expectEqual(@as(usize, 1), buffer.count());
    try std.testing.expect(buffer.get(100) == null);
    try std.testing.expect(buffer.get(101) != null);
}

test "UncommittedBuffer: remove non-existent offset returns error" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    try std.testing.expectError(error.WriteNotFound, buffer.remove(999));
}

test "UncommittedBuffer: cleanupTimedOut removes old writes" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    const record = Record{ .key = "test", .value = "value" };

    // Add write at time 1000ms
    try buffer.add(100, record, 1000);
    // Add write at time 2000ms
    try buffer.add(101, record, 2000);
    // Add write at time 3000ms
    try buffer.add(102, record, 3000);

    try std.testing.expectEqual(@as(usize, 3), buffer.count());

    // Clean up with current time = 4000ms, timeout = 1500ms
    // Should remove writes older than 2500ms (offsets 100 and 101)
    const removed = try buffer.cleanupTimedOut(4000, 1500);
    try std.testing.expectEqual(@as(usize, 2), removed);
    try std.testing.expectEqual(@as(usize, 1), buffer.count());
    try std.testing.expect(buffer.get(100) == null);
    try std.testing.expect(buffer.get(101) == null);
    try std.testing.expect(buffer.get(102) != null);
}

test "UncommittedBuffer: cleanupTimedOut with no timeouts" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    const record = Record{ .key = "test", .value = "value" };
    const now = 1000;

    try buffer.add(100, record, now);
    try buffer.add(101, record, now);

    // Clean up with short elapsed time
    const removed = try buffer.cleanupTimedOut(now + 100, 1000);
    try std.testing.expectEqual(@as(usize, 0), removed);
    try std.testing.expectEqual(@as(usize, 2), buffer.count());
}

test "UncommittedBuffer: getOldest" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    // Empty buffer
    try std.testing.expect(buffer.getOldest() == null);

    const record = Record{ .key = "test", .value = "value" };
    try buffer.add(100, record, 1000);
    try buffer.add(101, record, 2000);
    try buffer.add(102, record, 3000);

    const oldest = buffer.getOldest();
    try std.testing.expect(oldest != null);
    try std.testing.expectEqual(@as(u64, 100), oldest.?.offset);
}

test "UncommittedBuffer: clear" {
    var buffer = UncommittedBuffer.init(std.testing.allocator);
    defer buffer.deinit();

    const record = Record{ .key = "test", .value = "value" };
    const now = std.time.milliTimestamp();
    try buffer.add(100, record, now);
    try buffer.add(101, record, now);

    try std.testing.expectEqual(@as(usize, 2), buffer.count());

    buffer.clear();
    try std.testing.expectEqual(@as(usize, 0), buffer.count());
}

test "UncommittedWrite: isTimedOut" {
    const record = Record{ .key = "test", .value = "value" };
    var write = try UncommittedWrite.init(std.testing.allocator, 100, record, 1000);
    defer write.deinit();

    // Not timed out: elapsed 500ms, timeout 1000ms
    try std.testing.expectEqual(false, write.isTimedOut(1500, 1000));

    // Exactly at timeout: elapsed 1000ms, timeout 1000ms
    try std.testing.expectEqual(true, write.isTimedOut(2000, 1000));

    // Timed out: elapsed 1500ms, timeout 1000ms
    try std.testing.expectEqual(true, write.isTimedOut(2500, 1000));
}
