const std = @import("std");
const log = std.log;
const net = std.net;
const Thread = std.Thread;
const Cache = @import("cache.zig").Cache;
const Role = @import("cache.zig").Cache.Role;

const DEFAULT_PORT = 6379;

pub fn main() !void {
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

    const role = blk: {
        for (args[1..], 1..) |arg, i| {
            if (std.mem.eql(u8, arg, "--replicaof")) {
                if (i + 1 < args.len) {
                    break :blk Role{ .Slave = args[i + 1] };
                }
            }
        }
        break :blk Role{
            .Master = .{
                .master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                .master_repl_offset = 0,
            },
        };
    };

    const address = try net.Address.resolveIp("127.0.0.1", port);
    log.info("Server started on 127.0.0.1:{d}", .{port});

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var cache = Cache.init(allocator, role);
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
