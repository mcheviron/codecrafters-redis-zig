const std = @import("std");
const net = std.net;
const Thread = std.Thread;
const Parser = @import("parser.zig").Parser;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("Logs from your program will appear here!\n", .{});

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    while (true) {
        const connection = try listener.accept();

        const t = try Thread.spawn(.{}, handleConnection, .{ connection, &parser });
        t.detach();
    }
}

fn handleConnection(connection: net.Server.Connection, parser: *Parser) !void {
    try parser.handleConnection(connection);
}
