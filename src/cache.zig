const std = @import("std");
const mem = std.mem;
const log = std.log;
const time = std.time;
const Allocator = std.mem.Allocator;
const HashMap = std.StringHashMap;
const Mutex = std.Thread.Mutex;
const Resp = @import("resp.zig").Resp;
const Command = @import("resp.zig").Resp.Command;
const Response = @import("resp.zig").Resp.Response;

pub const Cache = struct {
    allocator: Allocator,
    cache: HashMap(Item),
    mutex: Mutex,
    resp: Resp,
    role: Role,

    pub const MasterInfo = struct {
        master_replid: []const u8,
        master_repl_offset: u64,
    };

    pub const Role = union(enum) {
        Master: MasterInfo,
        Slave: []const u8,
    };

    const Item = struct {
        value: []const u8,
        expiration: ?u64,
    };

    pub fn init(
        allocator: Allocator,
        role: Role,
    ) Cache {
        return Cache{
            .allocator = allocator,
            .cache = HashMap(Item).init(allocator),
            .mutex = Mutex{},
            .resp = Resp.init(allocator),
            .role = role,
        };
    }

    pub fn deinit(self: *Cache) void {
        self.cache.deinit();
        self.resp.deinit();
    }

    fn handleGetCommand(self: *Cache, key: []const u8) ![]const u8 {
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
                    return self.resp.encode(&[_]Response{Response{ .Get = null }});
                }
            }
            log.info("Found value for key {s}: {s}", .{ key, item.value });
            return self.resp.encode(&[_]Response{Response{ .Get = item.value }});
        }
        log.info("Key {s} not found in cache", .{key});
        return self.resp.encode(&[_]Response{Response{ .Get = null }});
    }

    fn handleSetCommand(self: *Cache, set: Resp.SetCommand) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now: u64 = @intCast(time.milliTimestamp());
        try self.cache.put(set.key, Item{
            .value = set.value,
            .expiration = if (set.expiration) |exp| now + exp else null,
        });
        log.info("Key {s} set successfully", .{set.key});
        return self.resp.encode(&[_]Response{Response.Set});
    }

    fn handleInfoCommand(self: *Cache) ![]const u8 {
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
        return self.resp.encode(&[_]Response{info_response});
    }

    pub fn handleConnection(self: *Cache, connection: std.net.Server.Connection) !void {
        // log.info("New connection established", .{});

        var buffer: [1024]u8 = undefined;
        var reader = connection.stream.reader();
        var writer = connection.stream.writer();

        while (true) {
            // log.info("Waiting to read data from connection", .{});
            const bytes_read = try reader.read(&buffer);
            // log.info("Bytes read: {}", .{bytes_read});
            if (bytes_read == 0) break;

            const data = buffer[0..bytes_read];
            // log.info("Data received: {s}", .{data});

            const commandList = try self.resp.decode(data);
            defer self.allocator.free(commandList);

            for (commandList) |command| {
                // log.info("Command: {}", .{command});
                const response = blk: {
                    switch (command) {
                        .Ping => break :blk try self.resp.encode(&[_]Response{Response.Pong}),
                        .Echo => |message| break :blk try self.resp.encode(&[_]Response{Response{ .Echo = message }}),
                        .Get => |key| break :blk try self.handleGetCommand(key),
                        .Set => |set| break :blk try self.handleSetCommand(set),
                        .Info => break :blk try self.handleInfoCommand(),
                        .Unknown => break :blk try self.resp.encode(&[_]Response{Response.Unknown}),
                    }
                };
                defer self.allocator.free(response);
                // const response_type_info = @typeInfo(@TypeOf(response));
                // log.info("Response type: {}", .{response_type_info});
                try writer.writeAll(response);
            }
        }
    }
};