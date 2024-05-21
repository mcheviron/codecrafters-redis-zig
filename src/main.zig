const std = @import("std");
const log = std.log;
const net = std.net;
const Thread = std.Thread;
const Cache = @import("cache.zig").Cache;

const DEFAULT_PORT = 6379;

pub fn main() !void {
    log.info("Logs from your program will appear here!\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak occured");

    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const port = blk: {
        for (args[1..], 1..) |arg, i| {
            if (std.mem.eql(u8, arg, "--port")) {
                if (i + 1 < args.len) {
                    break :blk try std.fmt.parseUnsigned(u16, args[i + 1], 10);
                }
            }
        }
        break :blk DEFAULT_PORT;
    };

    const address = try net.Address.resolveIp("127.0.0.1", port);
    log.info("Server started on 127.0.0.1:{d}", .{port});

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var cache = Cache.init(allocator);
    defer cache.deinit();

    while (true) {
        const connection = try listener.accept();

        const t = try Thread.spawn(.{}, handleConnection, .{ connection, &cache });
        t.detach();
    }
}

fn handleConnection(connection: net.Server.Connection, cache: *Cache) !void {
    try cache.handleConnection(connection);
}
