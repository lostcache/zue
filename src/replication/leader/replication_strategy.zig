const std = @import("std");
const Leader = @import("main.zig").Leader;
const FollowerConnection = @import("follower_connection.zig").FollowerConnection;

/// Strategy for handling a follower during replication
pub const ReplicationStrategy = enum {
    send_inline, // Send catch-up entries inline during this write
    defer_to_background, // Too far behind, defer to background repair
    skip, // Connection failed or other error, skip this follower
};

/// Determine replication strategy for a follower
/// Handles reconnection attempts and lag calculation
pub fn determineFollowerStrategy(
    self: *Leader,
    follower: *FollowerConnection,
    current_offset: u64,
) ReplicationStrategy {
    // Try to reconnect if follower is out-of-sync and disconnected
    if (!follower.in_sync) {
        if (follower.stream == null) {
            follower.connect() catch {
                return .skip;
            };
        }

        // Successfully connected, calculate lag
        const lag = if (follower.next_index <= current_offset)
            current_offset - follower.next_index + 1
        else
            0;

        // If lag exceeds threshold, defer to background
        if (lag > self.inline_catchup_threshold) {
            return .defer_to_background;
        }
        // Fall through to send inline
    }

    // Calculate lag for in-sync followers
    const lag = if (follower.next_index <= current_offset)
        current_offset - follower.next_index + 1
    else
        0;

    // Decide strategy based on lag
    if (lag > 0 and lag <= self.inline_catchup_threshold) {
        return .send_inline;
    } else if (lag > self.inline_catchup_threshold) {
        return .defer_to_background;
    }

    // Follower is caught up (lag == 0)
    return .skip;
}
