const std = @import("std");
const net = std.net;
const thread = std.Thread;

const ping = "*1\r\n$4\r\nPING\r\n";
const pong = "+PONG\r\n";

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("Logs from your program will appear here!", .{});

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection", .{});

        var t = try thread.spawn(.{}, handleConnection, .{connection});
        t.detach();
    }
}

fn handleConnection(connection: net.Server.Connection) !void {
    var buffer: [1024]u8 = undefined;
    defer connection.stream.close();

    const conReader = connection.stream.reader();

    while (try conReader.read(&buffer) > 0) {
        try connection.stream.writeAll(pong);
    }
}
