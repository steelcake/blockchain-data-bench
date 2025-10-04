const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const olive = b.dependency("olive", .{
        .target = target,
        .optimize = optimize,
    });

    const arrow = b.dependency("arrow", .{
        .target = target,
        .optimize = optimize,
    });

    const borsh = b.dependency("borsh", .{
        .target = target,
        .optimize = optimize,
    });

    const mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("olive", olive.module("olive"));
    mod.addImport("arrow", arrow.module("arrow"));
    mod.addImport("borsh", borsh.module("borsh"));

    const lib = b.addLibrary(.{
        .name = "olivers",
        .root_module = mod,
        .linkage = .dynamic,
        .use_llvm = true,
    });

    b.installArtifact(lib);
}
