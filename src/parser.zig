const std = @import("std");

pub const Command = enum {
    Ping,
    Echo,
    Unknown,
};

pub fn parseCommand(input: []const u8) Command {
    if (std.mem.indexOf(u8, input, "PING") != null or std.mem.indexOf(u8, input, "ping") != null) {
        return Command.Ping;
    } else if (std.mem.indexOf(u8, input, "ECHO") != null or std.mem.indexOf(u8, input, "echo") != null) {
        return Command.Echo;
    } else {
        return Command.Unknown;
    }
}

pub fn handleCommand(command: Command, input: []const u8) ![]const u8 {
    std.debug.print("Handling command: {}, with input: {s}\n", .{ command, input });
    switch (command) {
        Command.Ping => {
            std.debug.print("Command is Ping\n", .{});
            return "+PONG\r\n";
        },
        Command.Echo => {
            std.debug.print("Command is Echo\n", .{});
            const message = input[14..];
            std.debug.print("Echo message: {s}\n", .{message});
            return message;
        },
        Command.Unknown => {
            std.debug.print("Command is Unknown\n", .{});
            return "-ERR unknown command\r\n";
        },
    }
}
