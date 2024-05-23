const std = @import("std");
const log = std.log;
const net = std.net;
const mem = std.mem;
const Thread = std.Thread;
const Cache = @import("cache.zig").Cache;
const Role = @import("cache.zig").Cache.Role;
const RESP = @import("resp.zig");

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
                if (i + 2 < args.len) {
                    var buf: [128]u8 = undefined;
                    const addr = try std.fmt.bufPrint(&buf, "{s} {s}", .{ args[i + 1], args[i + 2] });
                    break :blk Role{ .Slave = addr };
                } else {
                    std.log.err("Invalid arguments. Usage: {s} --replicaof <host> <port>", .{args[0]});
                    return;
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

    if (role == .Slave) {
        const slave_thread = try Thread.spawn(.{}, startClient, .{ allocator, role.Slave });
        slave_thread.detach();
        // var parts = std.mem.splitSequence(u8, role.Slave, " ");
        // while (parts.next()) |part| {
        //     log.info("{s}", .{part});
        // }
    }

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

        const t = try Thread.spawn(.{}, startServer, .{ connection, &cache });
        t.detach();
    }
}

fn startServer(connection: net.Server.Connection, cache: *Cache) !void {
    try cache.handleConnection(connection);
}

fn startClient(allocator: mem.Allocator, master_addr: []const u8) !void {
    var iter = std.mem.splitSequence(u8, master_addr, " ");
    const host = iter.next().?;
    const port = try std.fmt.parseUnsigned(u16, iter.next().?, 10);

    const stream = try net.tcpConnectToHost(allocator, host, port);
    defer stream.close();

    const responses = [_]RESP.Response{RESP.Response.Ping};

    const ping_encoded = try RESP.encode(allocator, &responses);
    defer allocator.free(ping_encoded);
    try stream.writeAll(ping_encoded);
}
