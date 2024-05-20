const std = @import("std");
const mem = std.mem;
const log = std.log;
const time = std.time;
const eqlIgnoreCase = std.ascii.eqlIgnoreCase;
const Allocator = std.mem.Allocator;
const HashMap = std.StringHashMap;
const Mutex = std.Thread.Mutex;
const ArrayList = std.ArrayList;

pub const Parser = struct {
    allocator: Allocator,
    cache: HashMap(Item),
    mutex: Mutex,

    const Command = union(enum) {
        Ping,
        Echo: []const u8,
        Get: []const u8,
        Set,
        Unknown,
    };

    const Item = struct {
        value: []const u8,
        expiration: ?u64,
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
            .cache = HashMap(Item).init(allocator),
            .mutex = Mutex{},
        };
    }

    pub fn deinit(self: *Parser) void {
        self.cache.deinit();
    }

    pub fn parseCommands(self: *Parser, allocator: std.mem.Allocator, input: []const u8) ![]Command {
        var commands = ArrayList(Command).init(allocator);
        defer commands.deinit();

        log.info("Parsing commands from input of length {}", .{input.len});

        var it = std.mem.splitSequence(u8, input, "\r\n");
        while (it.next()) |numArgs| {
            log.info("numArgs: {s}", .{numArgs});
            if (numArgs.len <= 1) {
                log.info("Skipping empty or single character line", .{});
                continue;
            }

            if (!std.mem.startsWith(u8, numArgs, "*")) {
                log.err("Invalid format, expected line to start with '*', got '{s}'", .{numArgs});
                return ParseError.InvalidFormat;
            }

            const numArgsInt = std.fmt.parseInt(usize, numArgs[1..], 10) catch {
                log.err("Invalid format, failed to parse number of arguments '{s}'", .{numArgs[1..]});
                return ParseError.InvalidFormat;
            };

            log.info("Parsing command with {} arguments", .{numArgsInt});

            var args = std.ArrayList([]const u8).init(allocator);
            defer args.deinit();

            var i: usize = 0;
            while (i < numArgsInt) : (i += 1) {
                const argLen = it.next() orelse {
                    log.err("Invalid argument length, expected {} more arguments", .{numArgsInt - i});
                    return ParseError.InvalidArgLen;
                };
                if (!std.mem.startsWith(u8, argLen, "$")) {
                    log.err("Invalid argument length, expected line to start with '$', got '{s}'", .{argLen});
                    return ParseError.InvalidArgLen;
                }
                const argLenInt = std.fmt.parseInt(usize, argLen[1..], 10) catch {
                    log.err("Invalid argument length, failed to parse length '{s}'", .{argLen[1..]});
                    return ParseError.InvalidArgLen;
                };

                const arg = it.next() orelse {
                    log.err("Invalid argument length, expected argument of length {}", .{argLenInt});
                    return ParseError.InvalidArgLen;
                };
                if (arg.len != argLenInt) {
                    log.err("Argument length mismatch, expected length {}, got {}, arg: {s}", .{ argLenInt, arg.len, arg });
                    return ParseError.ArgLenMismatch;
                }

                log.info("Parsed argument '{s}' with length {}", .{ arg, argLenInt });
                try args.append(arg);
            }

            const command = try self.handleCommand(args.items);
            log.info("Handled command '{}'", .{command});
            try commands.append(command);
        }

        log.info("Parsed {} commands", .{commands.items.len});
        return commands.toOwnedSlice();
    }

    fn handleCommand(self: *Parser, args: [][]const u8) !Command {
        log.info("Handling command: {s}", .{args[0]});

        if (eqlIgnoreCase(args[0], "ping")) {
            return self.handlePingCommand(args);
        }

        if (eqlIgnoreCase(args[0], "echo")) {
            return self.handleEchoCommand(args);
        }

        if (eqlIgnoreCase(args[0], "get")) {
            return self.handleGetCommand(args);
        }

        if (eqlIgnoreCase(args[0], "set")) {
            return self.handleSetCommand(args);
        }

        log.err("Invalid command: {s}", .{args[0]});
        return ParseError.InvalidCommand;
    }

    fn handlePingCommand(_: *Parser, args: [][]const u8) !Command {
        if (args.len != 1) {
            log.err("Invalid number of arguments for PING command. Expected 1, got {}.", .{args.len});
            return ParseError.InvalidCommand;
        }
        log.info("Received PING command", .{});
        return Command.Ping;
    }

    fn handleEchoCommand(_: *Parser, args: [][]const u8) !Command {
        if (args.len != 2) {
            log.err("Invalid number of arguments for ECHO command. Expected 2, got {}.", .{args.len});
            return ParseError.InvalidCommand;
        }
        log.info("Received ECHO command with message: {s}", .{args[1]});
        return Command{ .Echo = args[1] };
    }

    fn handleGetCommand(self: *Parser, args: [][]const u8) !Command {
        if (args.len != 2) {
            log.err("Invalid number of arguments for GET command. Expected 2, got {}.", .{args.len});
            return ParseError.InvalidCommand;
        }
        const key = args[1];
        log.info("Received GET command for key: {s}", .{key});

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.cache.get(key)) |item| {
            log.info("Value: {s}, {?}", .{ item.value, item.expiration });
            if (item.expiration) |exp| {
                const now: u64 = @intCast(time.milliTimestamp());
                log.info("Now: {}", .{now});
                if (now > exp) {
                    log.info("Key {s} has expired, removing from cache", .{key});
                    _ = self.cache.remove(key);
                    return Command{ .Get = "" };
                }
            }
            log.info("Found value for key {s}: {s}", .{ key, item.value });
            return Command{ .Get = item.value };
        } else {
            log.info("Key {s} not found in cache", .{key});
            return Command{ .Get = "" };
        }
    }

    fn handleSetCommand(self: *Parser, args: [][]const u8) !Command {
        if (args.len != 3 and args.len != 5) {
            log.err("Invalid number of arguments for SET command. Expected 3 or 5, got {}. Available args: {s}", .{ args.len, args });
            return ParseError.InvalidCommand;
        }
        const key = args[1];
        const value = args[2];
        var expiration: ?u64 = null;
        log.info("Received SET command for key {s} with value {s}", .{ key, value });

        if (args.len == 5) {
            if (!eqlIgnoreCase(args[3], "px")) {
                log.err("Invalid expiration format. Expected 'px', got {s}", .{args[3]});
                return ParseError.InvalidCommand;
            }
            expiration = std.fmt.parseInt(u64, args[4], 10) catch {
                log.err("Invalid expiration value: {s}", .{args[4]});
                return ParseError.InvalidCommand;
            };
            log.info("Setting expiration to {d} milliseconds", .{expiration.?});
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        const now: u64 = @intCast(time.milliTimestamp());
        try self.cache.put(key, Item{
            .value = value,
            .expiration = if (expiration) |exp| now + exp else null,
        });
        log.info("Key {s} set successfully", .{key});
        return Command.Set;
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
                        .Ping, .Set, .Unknown => {},
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
