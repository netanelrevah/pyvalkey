from dataclasses import dataclass, field

from r3dis.commands.acls import AclSetUser
from r3dis.commands.core import CommandContext, CommandHandler
from r3dis.commands.hash_maps import (
    HashMapDelete,
    HashMapExists,
    HashMapGet,
    HashMapGetAll,
    HashMapGetMultiple,
    HashMapIncreaseBy,
    HashMapKeys,
    HashMapLength,
    HashMapMultiSet,
    HashMapSet,
    HashMapStringLength,
    HashMapValues,
)
from r3dis.commands.lists import (
    ListIndex,
    ListInsert,
    ListLength,
    ListPop,
    ListPush,
    ListRange,
    ListRemove,
)
from r3dis.commands.sets import (
    SetAdd,
    SetCardinality,
    SetIsMember,
    SetIsMembers,
    SetMembers,
    SetOperation,
    SetPop,
    SetRemove,
    SetStoreOperation,
)
from r3dis.commands.sorted_sets import SortedSetAdd, SortedSetCount, SortedSetRange
from r3dis.consts import Command
from r3dis.errors import RouterKeyError


@dataclass
class Router(CommandHandler):
    routes: dict[bytes, CommandHandler] = field(default_factory=dict)
    parent_command: Command | None = None

    def handle(self, command: bytes, parameters: list[bytes]):
        command_to_route = Command(command)
        if self.parent_command:
            command_to_route = Command(self.parent_command + b'|' + command)

        try:
            command_handler = self.routes[command_to_route]
        except KeyError:
            raise RouterKeyError()
        if not command_handler:
            raise RouterKeyError()
        parsed_parameters = command_handler.parse(parameters)
        return command_handler.handle(*parsed_parameters)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        command = parameters.pop(0).upper()
        return command, parameters


def create_base_router(command_context: CommandContext):
    router = Router(command_context)
    # String
    router.routes[b"GET"] = None
    router.routes[b"DEL"] = None
    router.routes[b"SET"] = None
    router.routes[b"INCR"] = None
    router.routes[b"INCRBY"] = None
    router.routes[b"INCRBYFLOAT"] = None
    router.routes[b"DECR"] = None
    router.routes[b"DECRBY"] = None
    router.routes[b"APPEND"] = None
    # # Bitmap
    router.routes[b"GETBIT"] = None
    router.routes[b"SETBIT"] = None
    router.routes[b"BITCOUNT"] = None
    router.routes[b"BITCOUNT"] = None
    router.routes[b"BITOP"] = None
    # Hash Map
    router.routes[b"HGET"] = HashMapGet(command_context)
    router.routes[b"HVALS"] = HashMapValues(command_context)
    router.routes[b"HSTRLEN"] = HashMapStringLength(command_context)
    router.routes[b"HDEL"] = HashMapDelete(command_context)
    router.routes[b"HSET"] = HashMapSet(command_context)
    router.routes[b"HGETALL"] = HashMapGetAll(command_context)
    router.routes[b"HEXISTS"] = HashMapExists(command_context)
    router.routes[b"HINCRBY"] = HashMapIncreaseBy(command_context)
    router.routes[b"HINCRBYFLOAT"] = HashMapIncreaseBy(command_context, float_allowed=True)
    router.routes[b"HKEYS"] = HashMapKeys(command_context)
    router.routes[b"HLEN"] = HashMapLength(command_context)
    router.routes[b"HMGET"] = HashMapGetMultiple(command_context)
    router.routes[b"HMSET"] = HashMapMultiSet(command_context)
    # Set
    router.routes[b"SPOP"] = SetPop(command_context)
    router.routes[b"SREM"] = SetRemove(command_context)
    router.routes[b"SUNION"] = SetOperation(command_context, set.union)
    router.routes[b"SINTER"] = SetOperation(command_context, set.intersection)
    router.routes[b"SDIFF"] = SetOperation(command_context, set.difference)
    router.routes[b"SUNIONSTORE"] = SetStoreOperation(command_context, set.union)
    router.routes[b"SINTERSTORE"] = SetStoreOperation(command_context, set.intersection)
    router.routes[b"SDIFFSTORE"] = SetStoreOperation(command_context, set.difference)
    router.routes[b"SADD"] = SetAdd(command_context)
    router.routes[b"SISMEMBER"] = SetIsMember(command_context)
    router.routes[b"SMEMBERS"] = SetMembers(command_context)
    router.routes[b"SMISMEMBER"] = SetIsMembers(command_context)
    router.routes[b"SMOVE"] = SetIsMembers(command_context)
    router.routes[b"SCARD"] = SetCardinality(command_context)
    # Sorted Set
    router.routes[b"ZRANGE"] = SortedSetRange(
        command_context,
        rev_allowed=True,
        limit_allowed=True,
        with_scores_allowed=True,
        bylex_allowed=True,
        byscore_allowed=True,
    )
    router.routes[b"ZRANGESTORE"] = SortedSetRange(
        command_context,
        store=True,
        rev_allowed=True,
        limit_allowed=True,
        with_scores_allowed=True,
        bylex_allowed=True,
        byscore_allowed=True,
    )
    router.routes[b"ZREVRANGE"] = SortedSetRange(
        command_context,
        is_reversed=True,
        with_scores_allowed=True,
    )
    router.routes[b"ZRANGEBYSCORE"] = SortedSetRange(
        command_context,
        with_scores_allowed=True,
        limit_allowed=True,
    )
    router.routes[b"ZREVRANGEBYSCORE"] = SortedSetRange(
        command_context,
        is_reversed=True,
        with_scores_allowed=True,
        limit_allowed=True,
    )
    router.routes[b"ZRANGEBYLEX"] = SortedSetRange(command_context, limit_allowed=True)
    router.routes[b"ZREVRANGEBYLEX"] = SortedSetRange(command_context, is_reversed=True, limit_allowed=True)
    router.routes[b"ZCOUNT"] = SortedSetCount(command_context)
    router.routes[b"ZADD"] = SortedSetAdd(command_context)
    # List
    router.routes[b"LRANGE"] = ListRange(command_context)
    router.routes[b"LPUSH"] = ListPush(command_context)
    router.routes[b"LPOP"] = ListPop(command_context)
    router.routes[b"LREM"] = ListRemove(command_context)
    router.routes[b"RPUSH"] = ListPush(command_context, at_tail=True)
    router.routes[b"LLEN"] = ListLength(command_context)
    router.routes[b"LINDEX"] = ListIndex(command_context)
    router.routes[b"LINSERT"] = ListInsert(command_context)
    # ACL
    router.routes[b"ACL"] = create_acl_router(command_context)
    # Config
    router.routes[b"CONFIG"] = None
    # Client
    router.routes[b"CLIENT"] = None
    # Database
    router.routes[b"FLUSHDB"] = None
    router.routes[b"SELECT"] = None
    router.routes[b"KEYS"] = None
    router.routes[b"DBSIZE"] = None
    # Management
    router.routes[b"AUTH"] = None
    router.routes[b"INFO"] = None
    router.routes[b"PING"] = None
    router.routes[b"ECHO"] = None

    return router


def create_acl_router(command_context: CommandContext):
    router = Router(command_context, parent_command=Command.Acl)
    # String
    router.routes[Command.AclGet] = None
    router.routes[Command.AclDelUser] = None
    router.routes[Command.AclDryRun] = None
    router.routes[Command.AclGenPass] = None
    router.routes[Command.AclGetUser] = None
    router.routes[Command.AclList] = None
    router.routes[Command.AclLoad] = None
    router.routes[Command.AclLog] = None
    router.routes[Command.AclSave] = None
    router.routes[Command.AclSetUser] = AclSetUser(command_context)
    router.routes[Command.AclUsers] = None
    router.routes[Command.AclWhoAmI] = None

    return router
