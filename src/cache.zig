const std = @import("std");
const mem = std.mem;
const log = std.log;
const time = std.time;
const Allocator = std.mem.Allocator;
const HashMap = std.StringHashMap;
const Mutex = std.Thread.Mutex;
const RESP = @import("resp.zig");
const Command = RESP.Command;
const Response = RESP.Response;

const EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub const Cache = struct {
    allocator: Allocator,
    cache: HashMap(Item),
    mutex: Mutex,
    role: Role,
    replication_id: ?[]const u8,

    pub const MasterInfo = struct {
        master_replid: []const u8,
        master_repl_offset: u64,
    };

    pub const SlaveInfo = struct {
        master_address: []const u8,
        master_port: u16,
        own_port: u16,
    };

    pub const Role = union(enum) {
        Master: MasterInfo,
        Slave: SlaveInfo,
    };

    const Item = struct {
        value: []const u8,
        expiration: ?u64,
    };

    pub fn init(allocator: Allocator, role: Role) Cache {
        return Cache{
            .allocator = allocator,
            .cache = HashMap(Item).init(allocator),
            .mutex = Mutex{},
            .role = role,
            .replication_id = if (role == .Master) role.Master.master_replid else null,
        };
    }

    pub fn deinit(self: *Cache) void {
        self.cache.deinit();
    }

    pub fn handleGetCommand(self: *Cache, allocator: Allocator, key: []const u8) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.cache.get(key)) |item| {
            if (item.expiration) |exp| {
                const now: u64 = @intCast(time.milliTimestamp());
                if (now > exp) {
                    _ = self.cache.remove(key);
                    return RESP.encode(allocator, &[_]Response{Response{ .Get = null }});
                }
            }
            return RESP.encode(allocator, &[_]Response{Response{ .Get = item.value }});
        }
        return RESP.encode(allocator, &[_]Response{Response{ .Get = null }});
    }

    pub fn handleSetCommand(self: *Cache, allocator: Allocator, set: RESP.SetCommand) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now: u64 = @intCast(time.milliTimestamp());
        try self.cache.put(set.key, Item{
            .value = set.value,
            .expiration = if (set.expiration) |exp| now + exp else null,
        });
        return RESP.encode(allocator, &[_]Response{Response.Set});
    }

    pub fn handleInfoCommand(self: *Cache, allocator: Allocator) ![]const u8 {
        const info_response = switch (self.role) {
            .Master => |master_info| Response{
                .Info = Response.InfoResponse{
                    .Master = .{
                        .master_replid = master_info.master_replid,
                        .master_repl_offset = master_info.master_repl_offset,
                    },
                },
            },
            .Slave => Response{
                .Info = Response.InfoResponse.Slave,
            },
        };
        return RESP.encode(allocator, &[_]Response{info_response});
    }

    pub fn encodeRDB(_: *Cache, allocator: Allocator, file: []const u8) ![]const u8 {
        const buf = try allocator.alloc(u8, file.len / 2);
        defer allocator.free(buf);

        const decoded = try std.fmt.hexToBytes(buf, file);
        const encoded_file = try std.fmt.allocPrint(allocator, "${d}\r\n{s}", .{ decoded.len, decoded });
        return encoded_file;
    }

    pub fn handleConnection(self: *Cache, connection: std.net.Server.Connection) !void {
        var buffer: [1024]u8 = undefined;
        var reader = connection.stream.reader();
        var writer = connection.stream.writer();

        while (true) {
            const bytes_read = try reader.read(&buffer);
            if (bytes_read == 0) break;

            const data = buffer[0..bytes_read];
            const command_list = try RESP.decode(self.allocator, data);
            defer self.allocator.free(command_list);

            for (command_list) |command| {
                const response = try switch (command) {
                    .Ping => RESP.encode(self.allocator, &[_]Response{Response.Pong}),
                    .Echo => |message| RESP.encode(self.allocator, &[_]Response{Response{ .Echo = message }}),
                    .Get => |key| self.handleGetCommand(self.allocator, key),
                    .Set => |set| self.handleSetCommand(self.allocator, set),
                    .Info => self.handleInfoCommand(self.allocator),
                    .ReplConf => RESP.encode(self.allocator, &[_]Response{Response{ .ReplConf = null }}),
                    .Psync => |psync| RESP.encode(self.allocator, &[_]Response{Response{ .Psync = .{ .Master = .{
                        .master_repl_offset = psync.master_repl_offset,
                        .master_replid = self.replication_id orelse "",
                    } } }}),
                    .Unknown => RESP.encode(self.allocator, &[_]Response{Response.Unknown}),
                };
                defer self.allocator.free(response);
                try writer.writeAll(response);

                if (command == .Psync) {
                    const file = try self.encodeRDB(self.allocator, EMPTY_RDB_HEX);
                    defer self.allocator.free(file);

                    try writer.writeAll(file);
                }
            }
        }
    }
};
