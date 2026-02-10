# Implementation Plan - Refactor: Production Readiness

## Strategy: Two-Phase Refactor
1. **Phase 1: Modularization** - Focus on splitting bloated files and functions into smaller, single-responsibility modules.
2. **Phase 2: Code Quality & Performance** - Focus on assertions (min 2 per function), comprehensive comments, and performance optimizations.

---

## Phase 1: Modularization (CURRENT)

### Structural Analysis
- [x] Task: Analyze existing codebase structure and dependencies
- [x] Task: Create a detailed refactoring map

### Replication & Consensus
- [x] Task: Modularize `src/replication/leader.zig`
    - [x] Extract `FollowerConnection` to `src/replication/leader/follower_connection.zig`
    - [x] Extract Quorum logic to `src/replication/leader/quorum.zig`
    - [x] Extract Strategy logic to `src/replication/leader/replication_strategy.zig`
    - [x] Extract Request Handling to `src/replication/leader/request_handling.zig`
    - [x] Extract Background Repair to `src/replication/leader/background_repair.zig`
    - [x] Extract Shared Utilities to `src/replication/leader/utils.zig`
    - [x] Update `src/replication/leader.zig` as a facade
    - [x] Verify with `zig build test`
    - [x] Verify with `zig build test-replication`
- [x] Task: Modularize `src/replication/follower.zig` [commit: e1fd0ca]
    - [x] Isolate sync logic and log appends
    - [x] Verify with `zig build test-replication`

### Server & Main
- [x] Task: Refactor `src/server.zig` & `src/main.zig`
    - [x] Extract configuration loading and CLI argument parsing to `src/config_loader.zig` or `src/cli_args.zig`
    - [x] Modularize `processRequest` logic into separate handlers
    - [x] Simplify the main event loop
    - [x] Verify with `zig build test-integration` [commit: ef7fde3]

### Core Components
- [x] Task: Modularize `src/log/` (Storage Engine)
    - [x] Split `mmap_log.zig` if necessary; extract segment management [commit: e98cd24]
- [x] Task: Modularize `src/network/` (Networking Layer)
    - [x] Separate protocol definitions from transport/socket handling [commit: d121601]

---

## Phase 2: Code Quality & Performance (PENDING)

### Testing & Debugging
- [ ] Task: Enhance Test Logging
    - [ ] Improve logging visibility in integration and replication tests

### Assertions & Safety
- [ ] Task: Global Assertion Injection
    - [ ] Ensure minimum 2 assertions per function across the codebase
- [ ] Task: Error Handling Review
    - [ ] Standardize error handling and remove any remaining panics

### Documentation
- [ ] Task: Comprehensive Commenting
    - [ ] Add "why" comments to complex logic
    - [ ] Ensure all public APIs have doc comments (`///`)

### Performance & Final Polish
- [ ] Task: Performance Optimization
    - [ ] Review critical paths for unnecessary allocations or syscalls
- [ ] Task: Final Suite Verification
    - [ ] Run all test suites: `test`, `test-integration`, `test-replication`