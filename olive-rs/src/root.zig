const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const ArenaAllocator = std.heap.ArenaAllocator;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;

const olive = @import("olive");

const borsh = @import("borsh");

const arrow = @import("arrow");
const FFI_ArrowArray = arrow.ffi.abi.ArrowArray;
const FFI_ArrowSchema = arrow.ffi.abi.ArrowSchema;

const SerializeOutput = extern struct {
    len: u32,
    ptr: [*]u8,
};

const ALLOC_SIZE = 1 << 23;

fn timer() std.time.Timer {
    return std.time.Timer.start() catch unreachable;
}

export fn olivers_ffi_serialize(array: *FFI_ArrowArray, schema: *FFI_ArrowSchema) callconv(.c) SerializeOutput {
    var t = timer();

    const mem = alloc_thp(ALLOC_SIZE) orelse unreachable;
    std.debug.assert(mem.len == ALLOC_SIZE);
    defer posix.munmap(mem);
    var fb_alloc = FixedBufferAllocator.init(mem);
    const alloc = fb_alloc.allocator();

    std.debug.print("olive alloc in {}us\n", .{t.lap() / 1000});

    var ffi_array = arrow.ffi.FFI_Array{
        .array = array.*,
        .schema = schema.*,
    };
    defer ffi_array.release();

    const imported_array = arrow.ffi.import_array(&ffi_array, alloc) catch unreachable;
    const imported_s_array = imported_array.struct_;

    const output = alloc_thp(ALLOC_SIZE) orelse unreachable;
    std.debug.assert(output.len == ALLOC_SIZE);
    var output_len: usize = 0;

    const imported_dt = arrow.data_type.get_data_type(&imported_array, alloc) catch unreachable;
    const imported_s_dt = imported_dt.struct_;

    std.debug.print("olive alloc in {}us\n", .{t.lap() / 1000});

    const has_minmax_index = alloc.alloc(bool, imported_s_dt.field_names.len) catch unreachable;
    for (0..has_minmax_index.len) |idx| {
        has_minmax_index[idx] = false;
    }

    const olive_schema = olive.schema.Schema{
        .table_names = &.{"transactions"},
        .tables = &.{olive.schema.TableSchema{
            .has_minmax_index = has_minmax_index,
            .data_types = imported_s_dt.field_types,
            .field_names = imported_s_array.field_names,
        }},
        .dicts = &.{
            olive.schema.DictSchema{
                .has_filter = false,
                .byte_width = 20,
                .members = &.{
                    olive.schema.DictMember{
                        .table_index = 0,
                        .field_index = 2,
                    },
                    olive.schema.DictMember{
                        .table_index = 0,
                        .field_index = 5,
                    },
                },
            },
        },
    };

    const chunk = olive.chunk.Chunk.from_arrow(&olive_schema, &.{imported_s_array.field_values}, alloc, alloc) catch unreachable;

    std.debug.print("olive from_arrow in {}us\n", .{t.lap() / 1000});

    const header = olive.write.write(.{
        .chunk = &chunk,
        .compression = &olive.write.ChunkCompression{
            .dicts = &.{.no_compression},
            .tables = &.{olive.write.TableCompression{
                .fields = &.{
                    .{ .flat = .lz4 },
                    .{ .flat = .lz4 },
                    .{ .flat = .no_compression },
                    .{ .flat = .no_compression },
                    .{ .flat = .{ .zstd = 1 } },
                    .{ .flat = .no_compression },
                    .{ .flat = .lz4 },
                },
            }},
        },
        .header_alloc = alloc,
        .filter_alloc = null,
        .data_section = output[output_len..],
        .page_size = null,
        .scratch_alloc = alloc,
    }) catch unreachable;
    output_len += header.data_section_size;

    std.debug.print("olive write in {}us\n", .{t.lap() / 1000});

    const header_len = borsh.serde.serialize(olive.header.Header, &header, output[output_len..], 40) catch unreachable;
    output_len += header_len;
    const header_size = @as(u32, @intCast(header_len));
    @memcpy(output[output_len .. output_len + 4], std.mem.asBytes(&header_size));
    output_len += 4;

    std.debug.print("olive header write in {}us\n", .{t.lap() / 1000});

    const schema_len = borsh.serde.serialize(olive.schema.Schema, &olive_schema, output[output_len..], 40) catch unreachable;
    output_len += schema_len;
    const schema_size = @as(u32, @intCast(schema_len));
    @memcpy(output[output_len .. output_len + 4], std.mem.asBytes(&schema_size));
    output_len += 4;

    std.debug.print("olive schema write in {}us\n", .{t.lap() / 1000});

    return .{
        .len = @intCast(output_len),
        .ptr = output.ptr,
    };
}

export fn olivers_ffi_deserialize(data: SerializeOutput, array_out: *FFI_ArrowArray, schema_out: *FFI_ArrowSchema) callconv(.c) void {
    var t = timer();

    defer posix.munmap(@alignCast(data.ptr[0..ALLOC_SIZE]));

    var arena = ArenaAllocator.init(std.heap.page_allocator);
    const alloc = arena.allocator();

    var data_end = data.len;

    const schema_len = std.mem.readVarInt(u32, data.ptr[data_end - 4 .. data_end], .little);
    data_end -= 4;
    const schema_bytes = data.ptr[data_end - schema_len .. data_end];
    data_end -= @intCast(schema_bytes.len);
    const schema = borsh.serde.deserialize(olive.schema.Schema, schema_bytes, alloc, 40) catch unreachable;

    std.debug.print("olive schema read in {}us\n", .{t.lap() / 1000});

    const header_len = std.mem.readVarInt(u32, data.ptr[data_end - 4 .. data_end], .little);
    data_end -= 4;
    const header_bytes = data.ptr[data_end - header_len .. data_end];
    data_end -= @intCast(header_bytes.len);
    const header = borsh.serde.deserialize(olive.header.Header, header_bytes, alloc, 40) catch unreachable;

    std.debug.print("olive header read in {}us\n", .{t.lap() / 1000});

    const chunk = olive.read.read(.{
        .schema = &schema,
        .scratch_alloc = alloc,
        .data_section = data.ptr[0..data_end],
        .alloc = alloc,
        .header = &header,
    }) catch unreachable;

    std.debug.print("olive read in {}us\n", .{t.lap() / 1000});

    const arrow_tables = chunk.to_arrow(alloc) catch unreachable;
    const fields = arrow_tables[0];

    std.debug.print("olive to_arrow in {}us\n", .{t.lap() / 1000});

    const table_s = schema.tables[0];

    const field_names = alloc.alloc([:0]const u8, table_s.field_names.len) catch unreachable;
    for (0..field_names.len) |idx| {
        const fname = alloc.allocSentinel(u8, table_s.field_names[idx].len, 0) catch unreachable;
        @memcpy(fname, table_s.field_names[idx]);
        field_names[idx] = fname;
    }

    const s_array = arrow.array.StructArray{
        .field_values = fields,
        .field_names = field_names,
        .len = chunk.tables[0].num_rows,
        .offset = 0,
        .validity = null,
        .null_count = 0,
    };

    const array = arrow.array.Array{ .struct_ = s_array };

    const ffi_arr = arrow.ffi.export_array(.{
        .array = &array,
        .arena = arena,
    }) catch unreachable;

    array_out.* = ffi_arr.array;
    schema_out.* = ffi_arr.schema;

    std.debug.print("olive read finish in {}us\n", .{t.lap() / 1000});
}

fn alloc_thp(size: usize) ?[]align(1 << 12) u8 {
    if (size == 0) {
        return null;
    }
    const alloc_size = align_forward(size, 1 << 21);
    const page = mmap_wrapper(alloc_size, 0) orelse return null;
    // posix.madvise(page.ptr, page.len, posix.MADV.HUGEPAGE) catch {
    //     posix.munmap(page);
    //     return null;
    // };
    return page;
}

fn mmap_wrapper(size: usize, huge_page_flag: u32) ?[]align(1 << 12) u8 {
    if (size == 0) {
        return null;
    }
    const flags = linux.MAP{ .TYPE = .PRIVATE, .ANONYMOUS = true, .HUGETLB = huge_page_flag != 0, .POPULATE = true, .LOCKED = false };
    const flags_int: u32 = @bitCast(flags);
    const flags_f: linux.MAP = @bitCast(flags_int | huge_page_flag);
    const page = posix.mmap(null, size, posix.PROT.READ | posix.PROT.WRITE, flags_f, -1, 0) catch return null;
    return page;
}

pub fn align_forward(addr: usize, alignment: usize) usize {
    return (addr +% alignment -% 1) & ~(alignment -% 1);
}
