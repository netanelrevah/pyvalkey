from r3dis.commands.database.bits import (
    BitCount,
    BitOperation,
    Decrement,
    DecrementBy,
    GetBit,
    Increment,
    IncrementBy,
    IncrementByFloat,
    SetBit,
)
from r3dis.commands.database.core import (
    Append,
    DatabaseCommand,
    Delete,
    Get,
    Set,
    create_smart_command_parser,
)
from r3dis.commands.hash_maps import (
    HashMapDelete,
    HashMapExists,
    HashMapGet,
    HashMapGetAll,
    HashMapGetMultiple,
    HashMapIncreaseBy,
    HashMapIncreaseByFloat,
    HashMapKeys,
    HashMapLength,
    HashMapSet,
    HashMapSetMultiple,
    HashMapStringLength,
    HashMapValues,
)
from r3dis.commands.router import RouteParser
from r3dis.commands.sets import (
    SetAdd,
    SetAreMembers,
    SetCardinality,
    SetDifference,
    SetDifferenceStore,
    SetIntersection,
    SetIntersectionStore,
    SetIsMember,
    SetMembers,
    SetMove,
    SetPop,
    SetRemove,
    SetUnion,
    SetUnionStore,
)
from r3dis.commands.sorted_sets import (
    SortedSetRange,
    SortedSetRangeStore,
    SortedSetReversedRange,
    SortedSetRangeByScore,
    SortedSetReversedRangeByScore,
    SortedSetRangeByLexical,
    SortedSetReversedRangeByLexical,
    SortedSetCount,
    SortedSetCardinality,
)
from r3dis.consts import Commands
from r3dis.databases import Database

COMMAND_TO_COMMAND_CLS: dict[Commands, type[DatabaseCommand]] = {
    # String
    Commands.Get: Get,
    Commands.Delete: Delete,
    Commands.Set: Set,
    Commands.Increment: Increment,
    Commands.IncrementBy: IncrementBy,
    Commands.IncrementByFloat: IncrementByFloat,
    Commands.Decrement: Decrement,
    Commands.DecrementBy: DecrementBy,
    Commands.Append: Append,
    # # Bitmap
    Commands.GetBit: GetBit,
    Commands.SetBit: SetBit,
    Commands.BitCount: BitCount,
    Commands.BitOperation: BitOperation,
    # # Hash Map
    Commands.HashMapGet: HashMapGet,
    Commands.HashMapValues: HashMapValues,
    Commands.HashMapStringLength: HashMapStringLength,
    Commands.HashMapDelete: HashMapDelete,
    Commands.HashMapSet: HashMapSet,
    Commands.HashMapGetAll: HashMapGetAll,
    Commands.HashMapExists: HashMapExists,
    Commands.HashMapIncreaseBy: HashMapIncreaseBy,
    Commands.HashMapIncreaseByFloat: HashMapIncreaseByFloat,
    Commands.HashMapKeys: HashMapKeys,
    Commands.HashMapLength: HashMapLength,
    Commands.HashMapGetMultiple: HashMapGetMultiple,
    Commands.HashMapSetMultiple: HashMapSetMultiple,
    # # Set
    Commands.SetPop: SetPop,
    Commands.SetRemove: SetRemove,
    Commands.SetUnion: SetUnion,
    Commands.SetIntersection: SetIntersection,
    Commands.SetDifference: SetDifference,
    Commands.SetUnionStore: SetUnionStore,
    Commands.SetIntersectionStore: SetIntersectionStore,
    Commands.SetDifferenceStore: SetDifferenceStore,
    Commands.SetAdd: SetAdd,
    Commands.SetIsMember: SetIsMember,
    Commands.SetMembers: SetMembers,
    Commands.SetAreMembers: SetAreMembers,
    Commands.SetMove: SetMove,
    Commands.SetCardinality: SetCardinality,
    # Sorted Set
    Commands.SortedSetRange: SortedSetRange,
    Commands.SortedSetRangeStore: SortedSetRangeStore,
    Commands.SortedSetReversedRange: SortedSetReversedRange,
    Commands.SortedSetRangeByScore: SortedSetRangeByScore,
    Commands.SortedSetReversedRangeByScore: SortedSetReversedRangeByScore,
    Commands.SortedSetRangeByLexical: SortedSetRangeByLexical,
    Commands.SortedSetReversedRangeByLexical: SortedSetReversedRangeByLexical,
    Commands.SortedSetCount: SortedSetCount,
    Commands.SortedSetCardinality: SortedSetCardinality,
}


def fill_database_string_commands(router: RouteParser, database: Database):
    for command, command_cls in COMMAND_TO_COMMAND_CLS.items():
        create_smart_command_parser(router, command, command_cls, database)
