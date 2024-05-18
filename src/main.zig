const std = @import("std");
// Uncomment this block to pass the first stage
const net = std.net;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!", .{});

    // Uncomment this block to pass the first stage

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection", .{});

        var buffer: [1024]u8 = undefined;
        const read_bytes = try connection.stream.read(&buffer);
        const message = buffer[0..read_bytes];

        if (std.mem.startsWith(u8, message, "PING")) {
            _ = try connection.stream.write("+PONG\r\n");
        }

        connection.stream.close();
    }
}
