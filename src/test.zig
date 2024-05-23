const std = @import("std");
const Resp = @import("resp.zig").RESP;
const testing = std.testing;
const Command = @import("resp.zig").RESP.Command;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Response = @import("resp.zig").RESP.Response;
const Cache = @import("cache.zig").Cache;
const Role = @import("cache.zig").Cache.Role;

fn testEncodeDecode(expected_commands: []const Command, encoded_string: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak occured");
    const allocator = gpa.allocator();

    var resp = Resp.init(allocator);
    defer resp.deinit();

    const commands = try resp.decode(encoded_string);
    defer allocator.free(commands);

    try testing.expectEqualSlices(Command, expected_commands, commands);
}

fn testCommand(command: Command, expected_response: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak occured");
    const allocator = gpa.allocator();

    var cache = Cache.init(allocator, Role{ .Slave = "" });
    defer cache.deinit();

    var resp = Resp.init(allocator);
    defer resp.deinit();

    var result = ArrayList(u8).init(allocator);
    defer result.deinit();

    const response = try switch (command) {
        .Ping => resp.encode(&[_]Response{Response.Pong}),
        .Echo => |message| resp.encode(&[_]Response{Response{ .Echo = message }}),
        .Get => |key| cache.handleGetCommand(key),
        .Set => |set| cache.handleSetCommand(set),
        .Info => cache.handleInfoCommand(),
        .Unknown => resp.encode(&[_]Response{Response.Unknown}),
    };
    defer allocator.free(response);

    try result.writer().print("{s}", .{response});
    try testing.expectEqualSlices(u8, expected_response, result.items);
}

test "RESP - Decode multiple commands" {
    const encoded_string = "*2\r\n$4\r\nECHO\r\n$13\r\nHello, World!\r\n*2\r\n$3\r\nGET\r\n$7\r\nmykey\r\n";
    const expected_commands = [_]Command{
        Command{ .Echo = "Hello, World!" },
        Command{ .Get = "mykey" },
    };

    try testEncodeDecode(&expected_commands, encoded_string);
}

test "RESP - Decode single command" {
    const encoded_string = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n";
    const expected_commands = [_]Command{
        Command{ .Set = .{
            .key = "mykey",
            .value = "value",
            .expiration = null,
        } },
    };

    try testEncodeDecode(&expected_commands, encoded_string);
}

test "Command - Ping" {
    const command = Command.Ping;
    const expected_response = "+PONG\r\n";

    try testCommand(command, expected_response);
}

test "Command - Echo" {
    const message = "Hello from Zig!";
    const command = Command{ .Echo = message };
    const expected_response = try std.fmt.allocPrint(std.testing.allocator, "$16\r\n{s}\r\n", .{message});
    defer std.testing.allocator.free(expected_response);

    try testCommand(command, expected_response);
}

test "Command - Get - Not found" {
    const key = "nonexistent_key";
    const command = Command{ .Get = key };
    const expected_response = "$-1\r\n";

    try testCommand(command, expected_response);
}

test "Command - Set" {
    const command = Command{ .Set = .{
        .key = "mykey",
        .value = "myvalue",
        .expiration = null,
    } };
    const expected_response = "+OK\r\n";

    try testCommand(command, expected_response);
}

test "Command - Get - Found" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak occured");
    const allocator = gpa.allocator();

    var cache = Cache.init(allocator, Role{ .Slave = "" });
    defer cache.deinit();

    var resp = Resp.init(allocator);
    defer resp.deinit();

    var result = ArrayList(u8).init(allocator);
    defer result.deinit();

    const key = "mykey";
    const value = "myvalue";

    _ = try cache.handleSetCommand(Resp.SetCommand{
        .key = key,
        .value = value,
        .expiration = null,
    });

    const response = try cache.handleGetCommand(key);
    defer allocator.free(response);

    try result.writer().print("{s}", .{response});
    const expected_response = try std.fmt.allocPrint(std.testing.allocator, "$8\r\n{s}\r\n", .{value});
    defer std.testing.allocator.free(expected_response);

    try testing.expectEqualSlices(u8, expected_response, result.items);
}

test "Command - Info - Master" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak occured");
    const allocator = gpa.allocator();

    var cache = Cache.init(allocator, Role{
        .Master = .{
            .master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            .master_repl_offset = 0,
        },
    });
    defer cache.deinit();

    var resp = Resp.init(allocator);
    defer resp.deinit();

    var result = ArrayList(u8).init(allocator);
    defer result.deinit();

    const response = try cache.handleInfoCommand();
    defer allocator.free(response);

    try result.writer().print("{s}", .{response});
    const expected_response = "$77\r\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n\r\n";

    try testing.expectEqualSlices(u8, expected_response, result.items);
}

test "Command - Info - Slave" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("leak occured");
    const allocator = gpa.allocator();

    var cache = Cache.init(allocator, Role{ .Slave = "" });
    defer cache.deinit();

    var resp = Resp.init(allocator);
    defer resp.deinit();

    var result = ArrayList(u8).init(allocator);
    defer result.deinit();

    const response = try cache.handleInfoCommand();
    defer allocator.free(response);

    try result.writer().print("{s}", .{response});
    const expected_response = "$11\r\nrole:slave\r\n";

    try testing.expectEqualSlices(u8, expected_response, result.items);
}

test "Command - Unknown" {
    const command = Command.Unknown;
    const expected_response = "-ERR unknown command\r\n";

    try testCommand(command, expected_response);
}

test "Failing test" {
    try testing.expect(false);
}
