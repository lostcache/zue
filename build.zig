const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // zue-server
    const zue_server = b.addExecutable(.{
        .name = "zue-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(zue_server);

    // zue-client
    const zue_client = b.addExecutable(.{
        .name = "zue-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli_client.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(zue_client);

    // zig build and run unit tests (main.zig has refAllDecls for all modules)
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const unit_test_run_artifact = b.addRunArtifact(unit_tests);

    const unit_tests_step = b.step("test", "Run unit tests");
    unit_tests_step.dependOn(&unit_test_run_artifact.step);

    // zig build and run test-integration
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const integration_tests_run_artifact = b.addRunArtifact(integration_tests);
    integration_tests_run_artifact.step.dependOn(b.getInstallStep());

    const integration_tests_step = b.step("test-integration", "Run integration tests");
    integration_tests_step.dependOn(&integration_tests_run_artifact.step);

    // zig build and run test-replication
    const replication_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/replication_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const replication_tests_run_artifact = b.addRunArtifact(replication_tests);
    replication_tests_run_artifact.step.dependOn(b.getInstallStep());

    const replication_tests_step = b.step("test-replication", "Run replication tests");
    replication_tests_step.dependOn(&replication_tests_run_artifact.step);
}
