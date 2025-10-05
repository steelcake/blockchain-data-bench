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

const ALLOC_SIZE = 1 << 25;

fn timer() std.time.Timer {
    return std.time.Timer.start() catch unreachable;
}

export fn olivers_ffi_serialize(array: *FFI_ArrowArray, schema: *FFI_ArrowSchema) callconv(.c) SerializeOutput {
    var arena = ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var ffi_array = arrow.ffi.FFI_Array{
        .array = array.*,
        .schema = schema.*,
    };
    defer ffi_array.release();

    const imported_array = arrow.ffi.import_array(&ffi_array, alloc) catch unreachable;
    const imported_s_array = imported_array.struct_;

    const output = std.heap.page_allocator.alloc(u8, ALLOC_SIZE) catch unreachable;
    var output_len: usize = 0;

    const imported_dt = arrow.data_type.get_data_type(&imported_array, alloc) catch unreachable;
    const imported_s_dt = imported_dt.struct_;

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
            olive.schema.DictSchema {
                .has_filter = false,
                .byte_width = 20,
                .members = &.{
                    olive.schema.DictMember {
                        .table_index = 0,
                        .field_index = 2,
                    },
                    olive.schema.DictMember {
                        .table_index = 0,
                        .field_index = 5,
                    },
                },
            },
        },
    };

    var t = timer();

    const chunk = olive.chunk.Chunk.from_arrow(&olive_schema, &.{imported_s_array.field_values}, alloc, alloc) catch unreachable;

    std.debug.print("olive from_arrow in {}ms\n", .{t.lap() / 1000 / 1000});

    const header = olive.write.write(.{
        .chunk = &chunk,
        .compression = .{ .zstd = 1 },
        .header_alloc = alloc,
        .filter_alloc = null,
        .data_section = output[output_len..],
        .page_size_kb = null,
        .scratch_alloc = alloc,
    }) catch unreachable;
    output_len += header.data_section_size;

    std.debug.print("olive write in {}ms\n", .{t.lap() / 1000 / 1000});

    const header_len = borsh.serde.serialize(olive.header.Header, &header, output[output_len..], 40) catch unreachable;
    output_len += header_len;
    const header_size = @as(u32, @intCast(header_len));
    @memcpy(output[output_len .. output_len + 4], std.mem.asBytes(&header_size));
    output_len += 4;

    std.debug.print("olive header write in {}ms\n", .{t.lap() / 1000 / 1000});

    const schema_len = borsh.serde.serialize(olive.schema.Schema, &olive_schema, output[output_len..], 40) catch unreachable;
    output_len += schema_len;
    const schema_size = @as(u32, @intCast(schema_len));
    @memcpy(output[output_len .. output_len + 4], std.mem.asBytes(&schema_size));
    output_len += 4;
    
    std.debug.print("olive schema write in {}ms\n", .{t.lap() / 1000 / 1000});

    return .{
        .len = @intCast(output_len),
        .ptr = output.ptr,
    };
}

export fn olivers_ffi_deserialize(data: SerializeOutput, array_out: *FFI_ArrowArray, schema_out: *FFI_ArrowSchema) callconv(.c) void {
    defer std.heap.page_allocator.free(data.ptr[0..ALLOC_SIZE]);

    var arena = ArenaAllocator.init(std.heap.page_allocator);
    const alloc = arena.allocator();

    var data_end = data.len;

    var t = timer();

    const schema_len = std.mem.readVarInt(u32, data.ptr[data_end - 4 .. data_end], .little);
    data_end -= 4;
    const schema_bytes = data.ptr[data_end - schema_len .. data_end];
    data_end -= @intCast(schema_bytes.len);
    const schema = borsh.serde.deserialize(olive.schema.Schema, schema_bytes, alloc, 40) catch unreachable;

    std.debug.print("olive schema read in {}ms\n", .{t.lap() / 1000 / 1000});

    const header_len = std.mem.readVarInt(u32, data.ptr[data_end - 4 .. data_end], .little);
    data_end -= 4;
    const header_bytes = data.ptr[data_end - header_len .. data_end];
    data_end -= @intCast(header_bytes.len);
    const header = borsh.serde.deserialize(olive.header.Header, header_bytes, alloc, 40) catch unreachable;

    std.debug.print("olive header read in {}ms\n", .{t.lap() / 1000 / 1000});

    const chunk = olive.read.read(.{
        .schema = &schema,
        .scratch_alloc = alloc,
        .data_section = data.ptr[0..data_end],
        .alloc = alloc,
        .header = &header,
    }) catch unreachable;

    std.debug.print("olive read in {}ms\n", .{t.lap() / 1000 / 1000});

    const arrow_tables = chunk.to_arrow(alloc) catch unreachable;
    const fields = arrow_tables[0];

    std.debug.print("olive to_arrow in {}ms\n", .{t.lap() / 1000 / 1000});

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

    std.debug.print("olive read finish in {}ms\n", .{t.lap() / 1000 / 1000});
}
