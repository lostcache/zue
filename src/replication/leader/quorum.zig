const std = @import("std");
const Leader = @import("main.zig").Leader;

/// Calculate commit_index based on quorum of match_index values (Raft algorithm)
/// Returns the highest offset that has been replicated on a quorum of replicas
pub fn calculateCommitIndex(self: *const Leader) ?u64 {
    // Special case: empty log
    if (self.log.next_offset == 0) {
        return null; // No entries to commit
    }

    // Collect match_index values for in-sync replicas only (including leader)
    const max_nodes = self.followers.len + 1; // +1 for leader
    var match_indices = self.allocator.alloc(u64, max_nodes) catch {
        // Fallback: return current commit_index on allocation failure
        return self.commit_index;
    };
    defer self.allocator.free(match_indices);

    // Leader's match_index is its own log's last offset
    const leader_offset = self.log.next_offset - 1;
    match_indices[0] = leader_offset;
    var count: usize = 1;

    // Collect follower match_indices (ONLY for in-sync followers)
    for (self.followers) |follower| {
        if (follower.in_sync) {
            match_indices[count] = follower.match_index orelse 0;
            count += 1;
        }
    }

    // Check if we have enough in-sync replicas for quorum
    if (count < self.quorum_size) {
        // Not enough replicas in sync - no progress
        return self.commit_index;
    }

    // Sort in descending order (only the valid entries)
    std.mem.sort(u64, match_indices[0..count], {}, comptime std.sort.desc(u64));

    // The commit_index is the value at index (quorum_size - 1)
    // This ensures at least quorum_size replicas have this value or higher
    const commit_idx_position = @min(self.quorum_size - 1, count - 1);
    const new_commit = match_indices[commit_idx_position];

    // Never decrease commit_index
    if (self.commit_index) |old| {
        return @max(new_commit, old);
    } else {
        return new_commit;
    }
}

/// Count how many followers are currently in-sync
pub fn countInSync(self: *const Leader) u32 {
    var count: u32 = 0;
    for (self.followers) |follower| {
        if (follower.in_sync) {
            count += 1;
        }
    }
    return count;
}

/// Check if we have enough in-sync replicas to accept writes
pub fn hasQuorum(self: *const Leader) bool {
    const isr_count = self.countInSync() + 1; // +1 for leader
    return isr_count >= self.quorum_size;
}

/// Get the current commit index
pub fn getCommitIndex(self: *const Leader) ?u64 {
    return self.commit_index;
}
