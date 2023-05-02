from dataclasses import dataclass, field

from r3dis.commands.acls import (
    AclCategory,
    AclDeleteUser,
    AclGeneratePassword,
    AclGetUser,
    AclSetUser,
)
from r3dis.commands.clients import (
    ClientGetName,
    ClientId,
    ClientKill,
    ClientList,
    ClientPause,
    ClientReply,
    ClientSetName,
    ClientUnpause,
)
from r3dis.commands.core import ClientContext, CommandHandler
from r3dis.commands.hash_maps import (
    HashMapDelete,
    HashMapExists,
    HashMapGet,
    HashMapGetAll,
    HashMapGetMultiple,
    HashMapIncreaseBy,
    HashMapKeys,
    HashMapLength,
    HashMapSet,
    HashMapSetMultiple,
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
            command_to_route = Command(self.parent_command + b"|" + command)

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


def create_base_router(command_context: ClientContext):
    router = Router(command_context)
    # String
    router.routes[Command.Get] = None
    router.routes[Command.Delete] = None
    router.routes[Command.Set] = None
    router.routes[Command.Increment] = None
    router.routes[Command.IncrementBy] = None
    router.routes[Command.IncrementByFloat] = None
    router.routes[Command.Decrement] = None
    router.routes[Command.DecrementBy] = None
    router.routes[Command.Append] = None
    # # Bitmap
    router.routes[Command.GetBit] = None
    router.routes[Command.SetBit] = None
    router.routes[Command.BitCount] = None
    router.routes[Command.BitOperation] = None
    # Hash Map
    router.routes[Command.HashMapGet] = HashMapGet(command_context)
    router.routes[Command.HashMapValues] = HashMapValues(command_context)
    router.routes[Command.HashMapStringLength] = HashMapStringLength(command_context)
    router.routes[Command.HashMapDelete] = HashMapDelete(command_context)
    router.routes[Command.HashMapSet] = HashMapSet(command_context)
    router.routes[Command.HashMapGetAll] = HashMapGetAll(command_context)
    router.routes[Command.HashMapExists] = HashMapExists(command_context)
    router.routes[Command.HashMapIncreaseBy] = HashMapIncreaseBy(command_context)
    router.routes[Command.HashMapIncreaseByFloat] = HashMapIncreaseBy(command_context, float_allowed=True)
    router.routes[Command.HashMapKeys] = HashMapKeys(command_context)
    router.routes[Command.HashMapLength] = HashMapLength(command_context)
    router.routes[Command.HashMapGetMultiple] = HashMapGetMultiple(command_context)
    router.routes[Command.HashMapSetMultiple] = HashMapSetMultiple(command_context)
    # Set
    router.routes[Command.SetPop] = SetPop(command_context)
    router.routes[Command.SetRemove] = SetRemove(command_context)
    router.routes[Command.SetUnion] = SetOperation(command_context, set.union)
    router.routes[Command.SetIntersection] = SetOperation(command_context, set.intersection)
    router.routes[Command.SetDifference] = SetOperation(command_context, set.difference)
    router.routes[Command.SetUnionStore] = SetStoreOperation(command_context, set.union)
    router.routes[Command.SetIntersectionStore] = SetStoreOperation(command_context, set.intersection)
    router.routes[Command.SetDifferenceStore] = SetStoreOperation(command_context, set.difference)
    router.routes[Command.SetAdd] = SetAdd(command_context)
    router.routes[Command.SetIsMember] = SetIsMember(command_context)
    router.routes[Command.SetMembers] = SetMembers(command_context)
    router.routes[Command.SetMultipleIsMember] = SetIsMembers(command_context)
    router.routes[Command.SetMove] = SetIsMembers(command_context)
    router.routes[Command.SetCardinality] = SetCardinality(command_context)
    # Sorted Set
    router.routes[Command.SortedSetRange] = SortedSetRange(
        command_context,
        rev_allowed=True,
        limit_allowed=True,
        with_scores_allowed=True,
        bylex_allowed=True,
        byscore_allowed=True,
    )
    router.routes[Command.SortedSetRangeStore] = SortedSetRange(
        command_context,
        store=True,
        rev_allowed=True,
        limit_allowed=True,
        with_scores_allowed=True,
        bylex_allowed=True,
        byscore_allowed=True,
    )
    router.routes[Command.SortedSetReversedRange] = SortedSetRange(
        command_context,
        is_reversed=True,
        with_scores_allowed=True,
    )
    router.routes[Command.SortedSetRangeByScore] = SortedSetRange(
        command_context,
        with_scores_allowed=True,
        limit_allowed=True,
    )
    router.routes[Command.SortedSetReversedRangeByScore] = SortedSetRange(
        command_context,
        is_reversed=True,
        with_scores_allowed=True,
        limit_allowed=True,
    )
    router.routes[Command.SortedSetRangeByLexical] = SortedSetRange(command_context, limit_allowed=True)
    router.routes[Command.SortedSetReversedRangeByLexical] = SortedSetRange(
        command_context, is_reversed=True, limit_allowed=True
    )
    router.routes[Command.SortedSetCount] = SortedSetCount(command_context)
    router.routes[Command.SortedSetAdd] = SortedSetAdd(command_context)
    # List
    router.routes[Command.ListRange] = ListRange(command_context)
    router.routes[Command.ListPush] = ListPush(command_context)
    router.routes[Command.ListPop] = ListPop(command_context)
    router.routes[Command.ListRemove] = ListRemove(command_context)
    router.routes[Command.ListPushAtTail] = ListPush(command_context, at_tail=True)
    router.routes[Command.ListLength] = ListLength(command_context)
    router.routes[Command.ListIndex] = ListIndex(command_context)
    router.routes[Command.ListInsert] = ListInsert(command_context)
    # ACL
    router.routes[Command.Acl] = create_acl_router(command_context)
    # Config
    router.routes[Command.Config] = None
    # Client
    router.routes[Command.Client] = None
    # Database
    router.routes[Command.FlushDatabase] = None
    router.routes[Command.Select] = None
    router.routes[Command.Keys] = None
    router.routes[Command.DatabaseSize] = None
    # Management
    router.routes[Command.Authorize] = None
    router.routes[Command.Information] = None
    router.routes[Command.Ping] = None
    router.routes[Command.Echo] = None

    return router


def create_acl_router(command_context: ClientContext):
    router = Router(command_context, parent_command=Command.Acl)
    # Acl
    router.routes[Command.AclCategory] = AclCategory(command_context)
    router.routes[Command.AclDelUser] = AclDeleteUser(command_context)
    router.routes[Command.AclDryRun] = None
    router.routes[Command.AclGenPass] = AclGeneratePassword(command_context)
    router.routes[Command.AclGetUser] = AclGetUser(command_context)
    router.routes[Command.AclList] = None
    router.routes[Command.AclLoad] = None
    router.routes[Command.AclLog] = None
    router.routes[Command.AclSave] = None
    router.routes[Command.AclSetUser] = AclSetUser(command_context)
    router.routes[Command.AclUsers] = None
    router.routes[Command.AclWhoAmI] = None
    router.routes[Command.AclHelp] = None

    return router


def create_client_router(command_context: ClientContext):
    router = Router(command_context, parent_command=Command.Client)
    # Acl
    router.routes[Command.ClientSetName] = ClientSetName(command_context)
    router.routes[Command.ClientPause] = ClientPause(command_context)
    router.routes[Command.ClientUnpause] = ClientUnpause(command_context)
    router.routes[Command.ClientReply] = ClientReply(command_context)
    router.routes[Command.ClientKill] = ClientKill(command_context)
    router.routes[Command.ClientGetName] = ClientGetName(command_context)
    router.routes[Command.ClientId] = ClientId(command_context)
    router.routes[Command.ClientList] = ClientList(command_context)

    return router
