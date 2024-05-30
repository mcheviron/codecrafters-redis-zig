const std = @import("std");
const mem = std.mem;
const log = std.log;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Role = @import("cache.zig").Cache.Role;
const eqlIgnoreCase = std.ascii.eqlIgnoreCase;

pub const Command = union(enum) {
    Ping,
    Unknown,
    Info,
    Echo: []const u8,
    Get: []const u8,
    Set: SetCommand,
    ReplConf,
};

pub const Response = union(enum) {
    Pong,
    Echo: []const u8,
    Get: ?[]const u8,
    Set,
    Info: InfoResponse,
    Unknown,
    Ping,
    ReplConf: ?ReplConfig,
    Psync: Psync,

    pub const InfoResponse = union(enum) {
        Master: struct {
            master_replid: []const u8,
            master_repl_offset: u64,
        },
        Slave,
    };

    pub const ReplConfig = union(enum) {
        listening_port: u16,
        capability: []const u8,
    };

    pub const Psync = union(enum) {
        init: bool,
        other,
    };
};

pub const SetCommand = struct {
    key: []const u8,
    value: []const u8,
    expiration: ?u64,
};

pub const ParseError = error{
    InvalidNumArgs,
    InvalidFormat,
    InvalidArgLen,
    ArgLenMismatch,
    InvalidCommand,
};

pub fn decode(allocator: Allocator, input: []const u8) ![]Command {
    var commands = ArrayList(Command).init(allocator);
    errdefer commands.deinit();

    // Split the input string by "\r\n"
    // Example input: "*2\r\n$4\r\nECHO\r\n$13\r\nHello, World!\r\n*2\r\n$3\r\nGET\r\n$7\r\nmykey\r\n"
    // Example output after split:
    // ["*2", "$4", "ECHO", "$13", "Hello, World!", "*2", "$3", "GET", "$7", "mykey", ""]
    var it = std.mem.splitSequence(u8, input, "\r\n");
    while (it.next()) |line| {
        if (line.len <= 1) continue;

        switch (line[0]) {
            '*' => {
                // Parse the number of arguments
                // Example: "*2" => numArgs = 2
                const numArgs = try std.fmt.parseInt(usize, line[1..], 10);
                var args = ArrayList([]const u8).init(allocator);
                defer args.deinit();

                var i: usize = 0;
                while (i < numArgs) : (i += 1) {
                    // Parse the argument length
                    // Example: "$4" => len = 4
                    const argLen = it.next() orelse return ParseError.InvalidCommand;
                    if (!std.mem.startsWith(u8, argLen, "$")) return ParseError.InvalidCommand;
                    const len = try std.fmt.parseInt(usize, argLen[1..], 10);

                    // Parse the argument value
                    // Example: "ECHO" => arg = "ECHO"
                    const arg = it.next() orelse return ParseError.InvalidCommand;
                    // log.info("arg.len: {d}, len: {d}\n", .{ arg.len, len });
                    if (arg.len != len) return ParseError.ArgLenMismatch;
                    try args.append(arg);
                }

                // Parse the command and append it to the commands list
                // Example: ["ECHO", "Hello, World!"] => Command{.Echo = "Hello, World!"}
                const command = try parseCommand(args.items);
                try commands.append(command);
            },
            else => return ParseError.InvalidCommand,
        }
    }

    // Return the parsed commands as an owned slice
    // Example: [_][]const u8{
    //     "ECHO", "Hello, World!",  => Command{.Echo = "Hello, World!"},
    //     "GET",  "mykey",          => Command{.Get  = "mykey"},
    // }
    return commands.toOwnedSlice();
}

pub fn encode(allocator: Allocator, responses: []const Response) ![]const u8 {
    var result = ArrayList(u8).init(allocator);
    errdefer result.deinit();

    const writer = result.writer();
    for (responses) |response| {
        const encoded_response = switch (response) {
            .Pong => try encodePong(allocator),
            .Echo => |message| try encodeEcho(allocator, message),
            .Get => |value| try encodeGet(allocator, value),
            .Set => try encodeSet(allocator),
            .Info => |info| try encodeInfo(allocator, info),
            .Unknown => try encodeUnknownCommand(allocator),
            .Ping => try encodePing(allocator),
            .ReplConf => |replconf| try encodeReplConf(allocator, replconf),
            .Psync => |psync| try encodePsync(allocator, psync),
        };
        try writer.print("{s}", .{encoded_response});
    }

    return result.toOwnedSlice();
}

fn parseCommand(args: [][]const u8) !Command {
    if (args.len == 0) return ParseError.InvalidNumArgs;

    if (eqlIgnoreCase(args[0], "ping")) {
        return Command.Ping;
    } else if (eqlIgnoreCase(args[0], "echo")) {
        if (args.len != 2) return ParseError.InvalidNumArgs;
        return Command{ .Echo = args[1] };
    } else if (eqlIgnoreCase(args[0], "get")) {
        if (args.len != 2) return ParseError.InvalidNumArgs;
        return Command{ .Get = args[1] };
    } else if (eqlIgnoreCase(args[0], "set")) {
        if (args.len != 3 and args.len != 5) return ParseError.InvalidNumArgs;
        const key = args[1];
        const value = args[2];
        var expiration: ?u64 = null;
        if (args.len == 5) {
            if (!eqlIgnoreCase(args[3], "PX")) return ParseError.InvalidFormat;
            expiration = try std.fmt.parseInt(u64, args[4], 10);
        }
        return Command{ .Set = .{ .key = key, .value = value, .expiration = expiration } };
    } else if (eqlIgnoreCase(args[0], "info")) {
        return Command.Info;
    } else if (eqlIgnoreCase(args[0], "replconf")) {
        if (args.len != 3 and args.len != 5) return ParseError.InvalidNumArgs;
        return Command.ReplConf;
    }

    return Command.Unknown;
}

fn encodePong(allocator: Allocator) ![]const u8 {
    return encodeSimpleString(allocator, "+PONG\r\n");
}

fn encodeEcho(allocator: Allocator, message: []const u8) ![]const u8 {
    return encodeBulkString(allocator, message);
}

fn encodeGet(allocator: Allocator, value: ?[]const u8) ![]const u8 {
    if (value) |v| {
        return encodeBulkString(allocator, v);
    } else {
        return encodeNull(allocator);
    }
}

fn encodeSet(allocator: Allocator) ![]const u8 {
    return encodeSimpleString(allocator, "+OK\r\n");
}

fn encodeInfo(allocator: Allocator, info: Response.InfoResponse) ![]const u8 {
    var result = ArrayList(u8).init(allocator);
    errdefer result.deinit();

    const writer = result.writer();

    switch (info) {
        .Master => |master_info| {
            var buf: [1024]u8 = undefined;
            const full_info = try std.fmt.bufPrint(&buf, "role:master\nmaster_replid:{s}\nmaster_repl_offset:{d}\n", .{ master_info.master_replid, master_info.master_repl_offset });

            const encoded = try encodeBulkString(allocator, full_info);
            try writer.print("{s}", .{encoded});
        },
        .Slave => {
            const role_slave = try encodeBulkString(allocator, "role:slave");
            try writer.print("{s}", .{role_slave});
        },
    }

    return result.toOwnedSlice();
}

fn encodeUnknownCommand(allocator: Allocator) ![]const u8 {
    return encodeError(allocator, "unknown command");
}

fn encodePing(allocator: Allocator) ![]const u8 {
    var strs = [_][]const u8{"PING"};
    return encodeBulkStrings(allocator, strs[0..]);
}

fn encodeReplConf(allocator: Allocator, replconf: ?Response.ReplConfig) ![]const u8 {
    if (replconf) |conf| {
        var result = ArrayList(u8).init(allocator);
        errdefer result.deinit();

        const writer = result.writer();
        var buf: [20]u8 = undefined;

        switch (conf) {
            .listening_port => |port| {
                var strs = [_][]const u8{ "REPLCONF", "listening-port", try std.fmt.bufPrint(&buf, "{d}", .{port}) };
                const encoded = try encodeBulkStrings(allocator, strs[0..]);
                try writer.print("{s}", .{encoded});
            },
            .capability => |capa| {
                var strs = [_][]const u8{ "REPLCONF", "capa", capa };
                const encoded = try encodeBulkStrings(allocator, strs[0..]);
                try writer.print("{s}", .{encoded});
            },
        }

        return result.toOwnedSlice();
    } else {
        return encodeSimpleString(allocator, "+OK\r\n");
    }
}

fn encodePsync(allocator: Allocator, psync: Response.Psync) ![]const u8 {
    return switch (psync) {
        .init => blk: {
            var strs = [_][]const u8{ "PSYNC", "?", "-1" };
            break :blk encodeBulkStrings(allocator, strs[0..]);
        },
        .other => encodeError(allocator, "unsupported psync command"),
    };
}

fn encodeSimpleString(allocator: Allocator, str: []const u8) ![]const u8 {
    var result = ArrayList(u8).init(allocator);
    errdefer result.deinit();
    try result.writer().print("{s}", .{str});
    return result.toOwnedSlice();
}

fn encodeBulkString(allocator: Allocator, str: []const u8) ![]const u8 {
    var result = ArrayList(u8).init(allocator);
    errdefer result.deinit();
    try result.writer().print("${d}\r\n{s}\r\n", .{ str.len, str });
    return result.toOwnedSlice();
}

fn encodeBulkStrings(allocator: Allocator, strings: []const []const u8) ![]const u8 {
    var result = ArrayList(u8).init(allocator);
    errdefer result.deinit();

    try result.writer().print("*{d}\r\n", .{strings.len});

    for (strings) |str| {
        const encoded = try encodeBulkString(allocator, str);
        try result.writer().print("{s}", .{encoded});
    }

    return result.toOwnedSlice();
}

pub fn encodeNull(allocator: Allocator) ![]const u8 {
    return encodeSimpleString(allocator, "$-1\r\n");
}

pub fn encodeError(allocator: Allocator, err: []const u8) ![]const u8 {
    var result = ArrayList(u8).init(allocator);
    errdefer result.deinit();
    try result.writer().print("-ERR {s}\r\n", .{err});
    return result.toOwnedSlice();
}
