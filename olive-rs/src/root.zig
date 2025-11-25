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

const ALLOC_SIZE = 1 << 30;

fn timer() std.time.Timer {
    return std.time.Timer.start() catch unreach();
}

const Context = extern struct {
    serialize_mem: [*]u8,
    deserialize_mem: [*]u8,
    out_mem: [*]u8,
};

export fn olivers_init_ctx() callconv(.c) Context {
    std.debug.print("olive init_ctx\n", .{});
    return .{
        .serialize_mem = (alloc_thp(ALLOC_SIZE) orelse unreach()).ptr,
        .deserialize_mem = (alloc_thp(ALLOC_SIZE) orelse unreach()).ptr,
        .out_mem = (alloc_thp(ALLOC_SIZE) orelse unreach()).ptr,
    };
}

export fn olivers_deinit_ctx(ctx: *Context) void {
    posix.munmap(@alignCast(ctx.out_mem[0..ALLOC_SIZE]));
    posix.munmap(@alignCast(ctx.deserialize_mem[0..ALLOC_SIZE]));
    posix.munmap(@alignCast(ctx.serialize_mem[0..ALLOC_SIZE]));
    ctx.out_mem = undefined;
    ctx.deserialize_mem = undefined;
    ctx.serialize_mem = undefined;
}

fn unreach() noreturn {
    @panic("unreach()");
}

export fn olivers_ffi_serialize(
    ctx: *const Context,
    table_names_raw: [*]const [*:0]const u8,
    arrays: [*]const FFI_ArrowArray,
    schemas: [*]const FFI_ArrowSchema,
    n_tables: usize,
) callconv(.c) SerializeOutput {
    std.debug.print("olive serialize\n", .{});

    var t = timer();

    var fb_alloc = std.heap.page_allocator.create(FixedBufferAllocator) catch unreach();
    fb_alloc.* = FixedBufferAllocator.init(ctx.serialize_mem[0..ALLOC_SIZE]);
    const alloc = fb_alloc.allocator();

    std.debug.print("olive alloc in {}us\n", .{t.lap() / 1000});

    const table_names = alloc.alloc([:0]const u8, n_tables) catch unreach();
    for (0..n_tables) |idx| {
        table_names[idx] = std.mem.span(table_names_raw[idx]);
    }

    const ffi_arrays = alloc.alloc(arrow.ffi.FFI_Array, n_tables) catch unreach();
    for (0..n_tables) |idx| {
        ffi_arrays[idx] = arrow.ffi.FFI_Array{
            .array = arrays[idx],
            .schema = schemas[idx],
        };
    }
    // defer for (ffi_arrays) |*a| {
    //     a.release();
    // };

    const field_names = alloc.alloc([]const [:0]const u8, n_tables) catch unreach();
    const tables = alloc.alloc([]const arrow.array.Array, n_tables) catch unreach();
    for (0..n_tables) |idx| {
        const table = (arrow.ffi.import_array(&ffi_arrays[idx], alloc) catch unreach()).struct_;
        tables[idx] = table.field_values;
        field_names[idx] = table.field_names;
    }

    const output = ctx.out_mem[0..ALLOC_SIZE];
    var output_len: usize = 0;

    const table_schemas = alloc.alloc(olive.schema.TableSchema, n_tables) catch unreach();
    for (0..n_tables) |idx| {
        const table_fields = tables[idx];

        const field_types = alloc.alloc(arrow.data_type.DataType, table_fields.len) catch unreach();
        for (0..field_types.len) |field_idx| {
            field_types[field_idx] = arrow.data_type.get_data_type(&table_fields[field_idx], alloc) catch unreach();
        }

        table_schemas[idx] = olive.schema.TableSchema{
            .field_names = field_names[idx],
            .field_types = field_types,
        };
    }

    std.debug.print("olive alloc in {}us\n", .{t.lap() / 1000});

    const olive_schema = olive.schema.Schema{
        .table_names = table_names,
        .table_schemas = table_schemas,
    };

    const chunk = olive.chunk.Chunk.from_arrow(&olive_schema, tables, alloc, alloc) catch unreach();

    std.debug.print("olive from_arrow in {}us\n", .{t.lap() / 1000});

    const header = olive.write.write(.{
        .chunk = &chunk,
        .header_alloc = alloc,
        .data_section = output[output_len..],
        .page_size = null,
        .scratch_alloc = alloc,
    }) catch unreach();
    output_len += header.data_section_size;

    std.debug.print("olive write in {}us\n", .{t.lap() / 1000});

    const header_len = borsh.serialize(*const olive.header.Header, &header, output[output_len..], 40) catch unreach();
    output_len += header_len;
    const header_size = @as(u32, @intCast(header_len));
    @memcpy(output[output_len .. output_len + 4], std.mem.asBytes(&header_size));
    output_len += 4;

    std.debug.print("olive header write in {}us\n", .{t.lap() / 1000});

    const schema_len = borsh.serialize(*const olive.schema.Schema, &olive_schema, output[output_len..], 40) catch unreach();
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

export fn olivers_ffi_deserialize(
    ctx: *const Context,
    data_raw: SerializeOutput,
    table_names_out: *[*]const [*:0]const u8,
    arrays_out: *[*]const FFI_ArrowArray,
    schemas_out: *[*]const FFI_ArrowSchema,
    n_tables_out: *usize,
) callconv(.c) void {
    var t = timer();

    var fb_alloc = std.heap.page_allocator.create(FixedBufferAllocator) catch unreach();
    fb_alloc.* = FixedBufferAllocator.init(ctx.deserialize_mem[0..ALLOC_SIZE]);
    var arena = ArenaAllocator.init(fb_alloc.allocator());
    const alloc = arena.allocator();

    const data = data_raw.ptr[0..data_raw.len];

    var x: u8 = 0;
    for (data) |xq| {
        x +%= xq;
    }

    var data_end = data.len;

    const schema_len = std.mem.readVarInt(u32, data[data_end - 4 .. data_end], .little);
    data_end -= 4;
    const schema_bytes = data[data_end - schema_len .. data_end];
    data_end -= @intCast(schema_bytes.len);
    const schema = borsh.deserialize(olive.schema.Schema, schema_bytes, alloc, 40) catch unreach();

    std.debug.print("olive schema read in {}us\n", .{t.lap() / 1000});

    const header_len = std.mem.readVarInt(u32, data[data_end - 4 .. data_end], .little);
    data_end -= 4;
    const header_bytes = data[data_end - header_len .. data_end];
    data_end -= @intCast(header_bytes.len);
    const header = borsh.deserialize(olive.header.Header, header_bytes, alloc, 40) catch unreach();

    std.debug.print("olive header read in {}us\n", .{t.lap() / 1000});

    const chunk = olive.read.read(.{
        .schema = &schema,
        .scratch_alloc = alloc,
        .data_section = data[0..data_end],
        .alloc = alloc,
        .header = &header,
    }) catch unreach();

    std.debug.print("olive read in {}us\n", .{t.lap() / 1000});

    const arrow_tables = chunk.to_arrow(alloc) catch unreach();

    std.debug.print("olive to_arrow in {}us\n", .{t.lap() / 1000});

    const n_tables = arrow_tables.len;
    const table_names_o = alloc.alloc([*:0]const u8, n_tables) catch unreach();
    const arrays_o = alloc.alloc(FFI_ArrowArray, n_tables) catch unreach();
    const schemas_o = alloc.alloc(FFI_ArrowSchema, n_tables) catch unreach();

    for (0..n_tables) |idx| {
        const fields = arrow_tables[idx];
        const table_s = schema.table_schemas[idx];

        const s_array = arrow.array.StructArray{
            .field_values = fields,
            .field_names = table_s.field_names,
            .len = arrow.length.length(&fields[0]),
            .offset = 0,
            .validity = null,
            .null_count = 0,
        };

        const array = arrow.array.Array{ .struct_ = s_array };

        const ffi_arr = arrow.ffi.export_array(.{
            .array = &array,
            .arena = arena,
        }) catch unreach();

        arrays_o[idx] = ffi_arr.array;
        schemas_o[idx] = ffi_arr.schema;
        table_names_o[idx] = schema.table_names[idx].ptr;

        std.debug.print("olive per_array in {}us\n", .{t.lap() / 1000});
    }

    n_tables_out.* = n_tables;
    table_names_out.* = table_names_o.ptr;
    arrays_out.* = arrays_o.ptr;
    schemas_out.* = schemas_o.ptr;
}

fn alloc_thp(size: usize) ?[]align(1 << 12) u8 {
    if (size == 0) {
        return null;
    }
    const alloc_size = align_forward(size, 1 << 21);
    const page = mmap_wrapper(alloc_size, 0) orelse return null;
    posix.madvise(page.ptr, page.len, posix.MADV.HUGEPAGE) catch {
        posix.munmap(page);
        return null;
    };
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
