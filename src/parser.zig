const std = @import("std");
const mem = std.mem;
const log = std.log;
const eqlIgnoreCase = std.ascii.eqlIgnoreCase;
const Allocator = std.mem.Allocator;
const HashMap = std.StringHashMap;
const Mutex = std.Thread.Mutex;
const ArrayList = std.ArrayList;

pub const Parser = struct {
    allocator: Allocator,
    cache: HashMap([]const u8),
    mutex: Mutex,

    pub const Command = union(enum) {
        Ping,
        Echo: []const u8,
        Get: []const u8,
        Set,
        Unknown,
    };

    const ParseError = error{
        InvalidNumArgs,
        InvalidFormat,
        InvalidArgLen,
        ArgLenMismatch,
        InvalidCommand,
    };

    pub fn init(allocator: Allocator) Parser {
        return Parser{
            .allocator = allocator,
            .cache = HashMap([]const u8).init(allocator),
            .mutex = Mutex{},
        };
    }

    pub fn deinit(self: *Parser) void {
        self.cache.deinit();
    }

    pub fn parseCommands(self: *Parser, allocator: std.mem.Allocator, input: []const u8) ![]Command {
        var commands = ArrayList(Command).init(allocator);
        defer commands.deinit();

        var it = std.mem.splitSequence(u8, input, "\r\n");
        while (it.next()) |numArgs| {
            if (numArgs.len <= 1) continue;

            if (!std.mem.startsWith(u8, numArgs, "*")) return ParseError.InvalidFormat;

            const numArgsInt = std.fmt.parseInt(usize, numArgs[1..], 10) catch return ParseError.InvalidFormat;

            var args = std.ArrayList([]const u8).init(allocator);
            defer args.deinit();

            var i: usize = 0;
            while (i < numArgsInt) : (i += 1) {
                const argLen = it.next() orelse return ParseError.InvalidArgLen;
                if (!std.mem.startsWith(u8, argLen, "$")) return ParseError.InvalidArgLen;
                const argLenInt = std.fmt.parseInt(usize, argLen[1..], 10) catch return ParseError.InvalidArgLen;

                const arg = it.next() orelse return ParseError.InvalidArgLen;
                if (arg.len != argLenInt) return ParseError.ArgLenMismatch;

                try args.append(arg);
            }

            const command = try self.handleCommand(args.items);
            try commands.append(command);
        }

        return commands.toOwnedSlice();
    }

    fn handleCommand(self: *Parser, args: [][]const u8) !Command {
        if (eqlIgnoreCase(args[0], "ping")) {
            if (args.len != 1) return ParseError.InvalidCommand;
            return Command.Ping;
        }

        if (eqlIgnoreCase(args[0], "echo")) {
            if (args.len != 2) return ParseError.InvalidCommand;
            return Command{ .Echo = args[1] };
        }

        if (eqlIgnoreCase(args[0], "get")) {
            if (args.len != 2) return ParseError.InvalidCommand;
            const key = args[1];

            self.mutex.lock();
            defer self.mutex.unlock();

            return Command{ .Get = self.cache.get(key) orelse "" };
        }

        if (eqlIgnoreCase(args[0], "set")) {
            if (args.len != 3) return ParseError.InvalidCommand;
            const key = args[1];
            const value = args[2];

            self.mutex.lock();
            defer self.mutex.unlock();

            try self.cache.put(key, value);
            return Command.Set;
        }

        return ParseError.InvalidCommand;
    }

    pub fn handleConnection(self: *Parser, connection: std.net.Server.Connection) !void {
        log.info("New connection established", .{});

        var buffer: [1024]u8 = undefined;
        var reader = connection.stream.reader();
        var writer = connection.stream.writer();

        while (true) {
            log.info("Waiting to read data from connection", .{});
            const bytes_read = try reader.read(&buffer);
            log.info("Bytes read: {}", .{bytes_read});
            if (bytes_read == 0) break;

            const data = buffer[0..bytes_read];
            log.info("Data received: {s}", .{data});
            const commandList = self.parseCommands(self.allocator, data) catch |err| {
                log.err("Error parsing command: {}", .{err});
                try writer.writeAll("-ERR invalid command\r\n");
                continue;
            };
            defer self.allocator.free(commandList);

            for (commandList) |command| {
                var response: []const u8 = undefined;
                defer {
                    switch (command) {
                        .Echo => self.allocator.free(response),
                        .Get => |val| if (val.len != 0) self.allocator.free(response),
                        else => {},
                    }
                }

                switch (command) {
                    .Ping => {
                        response = "+PONG\r\n";
                        log.info("Command: PING, Response: {s}", .{response});
                    },
                    .Set => {
                        response = "+OK\r\n";
                        log.info("Command: SET, Response: {s}", .{response});
                    },
                    .Echo => |msg| {
                        response = try std.fmt.allocPrint(self.allocator, "${d}\r\n{s}\r\n", .{ msg.len, msg });
                        log.info("Command: ECHO, Message: {s}, Response: {s}", .{ msg, response });
                    },
                    .Get => |value| {
                        if (value.len == 0) {
                            response = "$-1\r\n";
                        } else {
                            response = try std.fmt.allocPrint(self.allocator, "${d}\r\n{s}\r\n", .{ value.len, value });
                        }
                        log.info("Command: GET, Value: {s}, Response: {s}", .{ value, response });
                    },
                    .Unknown => {
                        response = "-ERR unknown command\r\n";
                        log.info("Command: UNKNOWN, Response: {s}", .{response});
                    },
                }

                try writer.writeAll(response);
                log.info("Response sent: {s}", .{response});
            }
        }
    }
};
