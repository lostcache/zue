const std = @import("std");
const Segment = @import("segment.zig");

pub const LogConfig = struct {
    segment_config: Segment.SegmentConfig,
    initial_offset: u64 = 0,

    pub fn default() LogConfig {
        return LogConfig{
            .segment_config = Segment.SegmentConfig.default(),
            .initial_offset = 0,
        };
    }

    pub fn init(log_file_max_bytes: u64, index_file_max_bytes: u64) LogConfig {
        return LogConfig{
            .segment_config = Segment.SegmentConfig.init(log_file_max_bytes, index_file_max_bytes),
            .initial_offset = 0,
        };
    }

    pub fn withInitialOffset(segment_config: Segment.SegmentConfig, initial_offset: u64) LogConfig {
        return LogConfig{
            .segment_config = segment_config,
            .initial_offset = initial_offset,
        };
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "LogConfig: default configuration" {
    const config = LogConfig.default();
    try std.testing.expectEqual(@as(u64, 0), config.initial_offset);
    try std.testing.expectEqual(@as(u64, 1024 * 1024 * 1024), config.segment_config.log_config.log_file_max_size_bytes);
}

test "LogConfig: init with custom sizes" {
    const config = LogConfig.init(512 * 1024 * 1024, 5 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 512 * 1024 * 1024), config.segment_config.log_config.log_file_max_size_bytes);
    try std.testing.expectEqual(@as(u64, 5 * 1024 * 1024), config.segment_config.index_config.index_file_max_size_bytes);
}

test "LogConfig: withInitialOffset" {
    const seg_config = Segment.SegmentConfig.default();
    const config = LogConfig.withInitialOffset(seg_config, 1000);
    try std.testing.expectEqual(@as(u64, 1000), config.initial_offset);
}
