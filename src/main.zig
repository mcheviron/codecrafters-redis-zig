const std = @import("std");
const net = std.net;
const thread = std.Thread;
const parser = @import("parser.zig");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("Logs from your program will appear here!\n", .{});

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        var t = try thread.spawn(.{}, handleConnection, .{connection});
        t.detach();
    }
}

fn handleConnection(connection: net.Server.Connection) !void {
    var buffer: [1024]u8 = undefined;
    defer connection.stream.close();

    const conReader = connection.stream.reader();

    while (true) {
        const bytesRead = try conReader.read(&buffer);
        if (bytesRead == 0) break;

        const data = buffer[0..bytesRead];

        const command = parser.parseCommand(data);
        const response = try parser.handleCommand(command, data);

        try connection.stream.writeAll(response);
    }
}
