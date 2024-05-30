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
                    const host = args[i + 1];
                    const master_port = try std.fmt.parseUnsigned(u16, args[i + 2], 10);
                    break :blk Role{ .Slave = .{
                        .master_address = host,
                        .master_port = master_port,
                        .own_port = port,
                    } };
                } else {
                    std.log.err("Invalid arguments.\nUsage: {s} --replicaof <host> <port>", .{args[0]});
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

    var cache = Cache.init(allocator, role);
    defer cache.deinit();

    if (role == .Slave) {
        const slave_thread = try Thread.spawn(.{}, startClient, .{ allocator, role.Slave, &cache });
        slave_thread.detach();
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);
    log.info("Server started on 127.0.0.1:{d}", .{port});

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        const t = try Thread.spawn(.{}, startServer, .{ connection, &cache });
        t.detach();
    }
}

fn startServer(connection: net.Server.Connection, cache: *Cache) !void {
    try cache.handleConnection(connection);
}

fn startClient(allocator: mem.Allocator, slave_info: Cache.SlaveInfo, cache: *Cache) !void {
    const stream = try net.tcpConnectToHost(allocator, slave_info.master_address, slave_info.master_port);
    defer stream.close();

    const responses = [_]RESP.Response{
        RESP.Response.Ping,
        RESP.Response{ .ReplConf = .{ .listening_port = slave_info.own_port } },
        RESP.Response{ .ReplConf = .{ .capability = "psync2" } },
        RESP.Response{ .Psync = .{ .init = true } },
    };

    const ping = try RESP.encode(allocator, responses[0..1]);
    defer allocator.free(ping);

    try stream.writeAll(ping);

    var buffer: [1024]u8 = undefined;
    var bytes_read = try stream.read(&buffer);
    var response = buffer[0..bytes_read];

    if (!mem.eql(u8, response, "+PONG\r\n")) {
        log.err("Unexpected response from server: {s}", .{response});
        return;
    }

    const replconf1 = try RESP.encode(allocator, responses[1..2]);
    defer allocator.free(replconf1);

    const replconf2 = try RESP.encode(allocator, responses[2..3]);
    defer allocator.free(replconf2);

    try stream.writeAll(replconf1);
    try stream.writeAll(replconf2);

    buffer = undefined;
    const ok_response = "+OK\r\n";
    bytes_read = try stream.read(&buffer);
    response = buffer[0..bytes_read];

    if (!mem.eql(u8, response, ok_response)) {
        log.err("Unexpected response from master: {s}", .{response});
        return;
    }

    const psync = try RESP.encode(allocator, responses[3..]);
    defer allocator.free(psync);

    try stream.writeAll(psync);

    buffer = undefined;
    bytes_read = try stream.read(&buffer);
    response = buffer[0..bytes_read];
    if (mem.startsWith(u8, response, "+FULLRESYNC")) {
        var parts = mem.splitSequence(u8, response, " ");
        _ = parts.next(); // skip "+FULLRESYNC"
        const repl_id = mem.trim(u8, parts.next().?, " \r\n");
        cache.replication_id = repl_id;
    } else {
        log.err("Unexpected response from master: {s}", .{response});
        return;
    }
}
