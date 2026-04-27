from dataclasses import field
from enum import Enum

from pyvalkey.blocking import ListBlockingManager
from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.consts import LONG_MAX
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.information import Information
from pyvalkey.enums import NotificationType
from pyvalkey.notifications import NotificationsManager
from pyvalkey.resp import RESP_OK, ArrayNone, ValueType
from pyvalkey.utils.dependencies import dependency


class DirectionMode(Enum):
    BEFORE = b"BEFORE"
    AFTER = b"AFTER"


@command(b"blpop", {b"blocking", b"list", b"slow"}, flags={b"blocking", b"write"})
class ListBlockingLeftPop(Command):
    """
    summary: >-
      Removes and returns the first element in a list. Blocks until an element is available otherwise. Deletes the
      list if the last element was popped.
    complexity: O(N) where N is the number of provided keys.
    since: 2.0.0
    function: blpopCommand
    reply_schema:
      oneOf:
      - type: 'null'
        description: No element could be popped and timeout expired
      - description: The key from which the element was popped and the value of the popped element
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: List key from which the element was popped.
          type: string
        - description: Value of the popped element.
          type: string
    """

    client_context: ClientContext = dependency()
    database: Database = dependency()
    information: Information = dependency()
    blocking_manager: ListBlockingManager = dependency()

    keys: list[bytes] = positional_parameter()
    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, b"blpop", self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(0)
        self.information.rdb_changes_since_last_save += 1
        return [self._key, value]


@command(b"brpop", {b"blocking", b"list", b"slow"}, flags={b"blocking", b"write"})
class ListBlockingRightPop(Command):
    """
    summary: >-
      Removes and returns the last element in a list. Blocks until an element is available otherwise. Deletes the
      list if the last element was popped.
    complexity: O(N) where N is the number of provided keys.
    since: 2.0.0
    function: brpopCommand
    reply_schema:
      oneOf:
      - description: No element could be popped and the timeout expired.
        type: 'null'
      - type: array
        minItems: 2
        maxItems: 2
        items:
        - description: 'The name of the key where an element was popped '
          type: string
        - description: The value of the popped element
          type: string
    """

    client_context: ClientContext = dependency()
    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    keys: list[bytes] = positional_parameter()
    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, b"brpop", self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(-1)
        return [self._key, value]


class Direction(Enum):
    LEFT = b"LEFT"
    RIGHT = b"RIGHT"


@command(b"blmpop", {b"blocking", b"list", b"slow"}, flags={b"blocking", b"write"})
class ListBlockingMultiplePop(Command):
    """
    summary: >-
      Pops the first element from one of multiple lists. Blocks until an element is available otherwise. Deletes
      the list if the last element was popped.
    complexity: >-
      O(N+M) where N is the number of provided keys and M is the number of elements returned.
    since: 7.0.0
    function: blmpopCommand
    reply_schema:
      oneOf:
      - description: Operation timed-out
        type: 'null'
      - description: The key from which elements were popped and the popped elements
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: List key from which elements were popped.
          type: string
        - description: Array of popped elements.
          type: array
          minItems: 1
          items:
            type: string
    """

    client_context: ClientContext = dependency()
    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")
    num_keys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(length_field_name="num_keys")
    direction: Direction = positional_parameter()
    count: int = keyword_parameter(default=1, token=b"COUNT")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, self.full_command_name, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        values = [
            list_value.pop(0 if self.direction == Direction.LEFT else -1)
            for _ in range(min(self.count, len(list_value)))
        ]
        return [self._key, values]


@command(b"lmpop", {b"list", b"slow"}, flags={b"write"})
class ListMultiplePop(Command):
    """
    summary: >-
      Returns multiple elements from a list after removing them. Deletes the list if the last element was popped.
    complexity: >-
      O(N+M) where N is the number of provided keys and M is the number of elements returned.
    since: 7.0.0
    function: lmpopCommand
    reply_schema:
      anyOf:
      - description: If no element could be popped.
        type: 'null'
      - description: List key from which elements were popped.
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Name of the key from which elements were popped.
          type: string
        - description: Array of popped elements.
          type: array
          minItems: 1
          items:
            type: string
    """

    client_context: ClientContext = dependency()
    database: Database = dependency()

    numkeys: int = positional_parameter(parse_error=b"ERR numkeys should be greater than 0")
    keys: list[bytes] = positional_parameter(length_field_name="numkeys")
    direction: Direction = positional_parameter()
    count: int = keyword_parameter(default=1, token=b"COUNT", parse_error=b"ERR count should be greater than 0")

    def execute(self) -> ValueType:
        if self.count <= 0:
            raise ServerError(b"ERR count should be greater than 0")

        for key in self.keys:
            value = self.database.list_database.get_value_or_none(key)
            if value is not None:
                break
        else:
            return None

        values = [value.pop(0 if self.direction == Direction.LEFT else -1) for _ in range(min(self.count, len(value)))]
        return [key, values]


@command(b"brpoplpush", {b"blocking", b"list", b"slow"}, flags={b"blocking", b"denyoom", b"write"})
class ListBlockingRightPopLeftPush(Command):
    """
    summary: >-
      Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available
      otherwise. Deletes the list if the last element was popped.
    complexity: O(1)
    since: 2.2.0
    function: brpoplpushCommand
    reply_schema:
      oneOf:
      - type: string
        description: The element being popped from source and pushed to destination.
      - type: 'null'
        description: Timeout is reached.
    """

    client_context: ClientContext = dependency()
    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    source: bytes = positional_parameter(key_mode=b"RW")
    destination: bytes = positional_parameter(key_mode=b"RW")
    timeout: int = positional_parameter()

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, self.full_command_name, [self.source], self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        destination_list = self.database.list_database.get_value_or_create(self.destination)
        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(-1)
        destination_list.insert(0, value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        if self._key is not None:
            await self.blocking_manager.notify(self.destination, in_multi=in_multi)


@command(b"blmove", {b"blocking", b"list", b"slow"}, flags={b"blocking", b"denyoom", b"write"})
class ListBlockingMove(Command):
    """
    summary: >-
      Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available
      otherwise. Deletes the list if the last element was moved.
    complexity: O(1)
    since: 6.2.0
    function: blmoveCommand
    reply_schema:
      oneOf:
      - description: The popped element.
        type: string
      - description: >-
          Operation timed-out or the command is issued from a transaction or a script and the source does not exist.
        type: 'null'
    """

    client_context: ClientContext = dependency()
    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()
    information: Information = dependency()

    source: bytes = positional_parameter(key_mode=b"RW")
    destination: bytes = positional_parameter(key_mode=b"RW")
    source_direction: Direction = positional_parameter()
    destination_direction: Direction = positional_parameter()
    timeout: int = positional_parameter()

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, self.full_command_name, [self.source], self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return None

        list_value = self.database.list_database.get_value(self._key)
        value = list_value.pop(0 if self.source_direction == Direction.LEFT else -1)
        if self.destination_direction == Direction.LEFT:
            self.database.list_database.get_or_create(self.destination).value.insert(0, value)
        else:
            self.database.list_database.get_or_create(self.destination).value.append(value)

        self.information.rdb_changes_since_last_save += 1
        return value

    async def after(self, in_multi: bool = False) -> None:
        if self._key is not None:
            await self.blocking_manager.notify(self.destination, in_multi=in_multi)


@command(b"lmove", {b"list", b"slow"}, flags={b"denyoom", b"write"})
class ListMove(Command):
    """
    summary: >-
      Returns an element after popping it from one list and pushing it to another. Deletes the list if the last
      element was moved.
    complexity: O(1)
    since: 6.2.0
    function: lmoveCommand
    reply_schema:
      oneOf:
      - description: The element being popped and pushed.
        type: string
      - description: Source does not exist.
        type: 'null'
    """

    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    source: bytes = positional_parameter(key_mode=b"RW")
    destination: bytes = positional_parameter(key_mode=b"RW")
    source_direction: Direction = positional_parameter()
    destination_direction: Direction = positional_parameter()

    def execute(self) -> ValueType:
        source_value = self.database.list_database.get_value_or_none(self.source)

        if source_value is None:
            return None

        destination_value = self.database.list_database.get_value_or_create(self.destination)

        value = source_value.pop(0 if self.source_direction == Direction.LEFT else -1)
        if self.destination_direction == Direction.LEFT:
            destination_value.insert(0, value)
        else:
            destination_value.append(value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.destination, in_multi=in_multi)


@command(b"lindex", {b"list", b"read", b"slow"}, flags={b"readonly"})
class ListIndex(Command):
    """
    summary: Returns an element from a list by its index.
    complexity: >-
      O(N) where N is the number of elements to traverse to get to the element at index. This makes asking for the
      first or the last element of the list O(1).
    since: 1.0.0
    function: lindexCommand
    reply_schema:
      oneOf:
      - type: 'null'
        description: Index is out of range
      - description: The requested element
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    index: int = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.list_database.get_value_or_none(self.key)

        if value is None or self.index >= len(value):
            return None

        return value[self.index]


@command(b"linsert", {b"list", b"slow"}, flags={b"denyoom", b"write"})
class ListInsert(Command):
    """
    summary: Inserts an element before or after another element in a list.
    complexity: >-
      O(N) where N is the number of elements to traverse before seeing the value pivot. This means that inserting
      somewhere on the left end on the list (head) can be considered O(1) and inserting somewhere on the right end
      (tail) is O(N).
    since: 2.2.0
    function: linsertCommand
    reply_schema:
      oneOf:
      - description: List length after a successful insert operation.
        type: integer
        minimum: 1
      - description: In case key doesn't exist.
        const: 0
      - description: When the pivot wasn't found.
        const: -1
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    direction: DirectionMode = positional_parameter()
    pivot: bytes = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        list_value = self.database.list_database.get_value_or_none(self.key)

        if list_value is None:
            return 0

        try:
            index = list_value.index(self.pivot)
        except ValueError:
            return -1
        list_value.insert(index + (0 if self.direction == DirectionMode.BEFORE else 1), self.element)
        return len(list_value)


@command(b"lset", {b"list", b"slow"}, flags={b"denyoom", b"write"})
class ListSet(Command):
    """
    summary: Sets the value of an element in a list by its index.
    complexity: >-
      O(N) where N is the length of the list. Setting either the first or the last element of the list is O(1).
    since: 1.0.0
    function: lsetCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    index: int = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        list_value = self.database.list_database.get_value_or_none(self.key)

        if list_value is None:
            raise ServerError(b"ERR no such key")

        if self.index >= len(list_value) or self.index < -len(list_value):
            raise ServerError(b"ERR index out of range")

        list_value[self.index] = self.element
        return RESP_OK


@command(b"llen", {b"list", b"read"}, flags={b"fast", b"readonly"})
class ListLength(Command):
    """
    summary: Returns the length of a list.
    complexity: O(1)
    since: 1.0.0
    function: llenCommand
    reply_schema:
      description: List length.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return len(self.database.list_database.get_value_or_empty(self.key))


@command(b"lrange", {b"list", b"read", b"slow"}, flags={b"readonly"})
class ListRange(Command):
    """
    summary: Returns a range of elements from a list.
    complexity: >-
      O(S+N) where S is the distance of start offset from HEAD for small lists, from nearest end (HEAD or TAIL)
      for large lists; and N is the number of elements in the specified range.
    since: 1.0.0
    function: lrangeCommand
    reply_schema:
      description: List of elements in the specified range
      type: array
      items:
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    start: int = positional_parameter()
    stop: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.list_database.get_value_or_empty(self.key)[parse_range_parameters(self.start, self.stop)]


@command(b"ltrim", {b"list", b"slow"}, flags={b"write"})
class ListTrim(Command):
    """
    summary: >-
      Removes elements from both ends of a list. Deletes the list if all elements were trimmed.
    complexity: O(N) where N is the number of elements to be removed by the operation.
    since: 1.0.0
    function: ltrimCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    start: int = positional_parameter()
    stop: int = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.list_database.get_or_none(self.key)

        if key_value is not None:
            key_value.value = key_value.value[parse_range_parameters(self.start, self.stop)]
        return RESP_OK


@command(b"lpop", {b"list"}, flags={b"fast", b"write"})
class ListPop(Command):
    """
    summary: >-
      Returns and removes one or more elements from the beginning of a list. Deletes the list if the last element
      was popped.
    complexity: O(N) where N is the number of elements returned
    since: 1.0.0
    function: lpopCommand
    reply_schema:
      oneOf:
      - description: Key does not exist.
        type: 'null'
      - description: In case `count` argument was not given, the value of the first element.
        type: string
      - description: In case `count` argument was given, a list of popped elements
        type: array
        items:
          type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.count is not None and self.count < 0:
            raise ServerError(b"ERR value is out of range, must be positive")

        list_value = self.database.list_database.get_value_or_empty(self.key)
        if not list_value:
            return None if (self.count is None) else ArrayNone
        if self.count is not None:
            value = [list_value.pop(0) for _ in range(min(len(list_value), self.count))]
        else:
            value = list_value.pop(0)
        return value


@command(b"lpos", {b"list", b"read", b"slow"}, flags={b"readonly"})
class ListPosition(Command):
    """
    summary: Returns the index of matching elements in a list.
    complexity: >-
      O(N) where N is the number of elements in the list, for the average case. When searching for elements near
      the head or the tail of the list, or when the MAXLEN option is provided, the command may run in constant time.
    since: 6.0.6
    function: lposCommand
    reply_schema:
      anyOf:
      - description: In case there is no matching element
        type: 'null'
      - description: An integer representing the matching element
        type: integer
      - description: >-
          If the COUNT option is given, an array of integers representing the matching elements (empty if there
          are no matches)
        type: array
        uniqueItems: true
        items:
          type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    element: bytes = positional_parameter()
    rank: int | None = keyword_parameter(token=b"RANK", default=None)
    number_of_matches: int | None = keyword_parameter(token=b"COUNT", default=None)
    maximum_length: int | None = keyword_parameter(token=b"MAXLEN", default=None)

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_or_create(self.key).value

        if self.rank is not None:
            if self.rank == 0:
                raise ServerError(
                    b"ERR RANK can't be zero: "
                    b"use 1 to start from the first match, "
                    b"2 from the second ... or use negative to start from the end of the list"
                )
            if self.rank < -LONG_MAX:
                raise ServerError(
                    b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"
                )

        indexes = []
        skip = abs(self.rank) - 1 if self.rank is not None else 0
        for index in range(len(a_list)):
            real_index = index
            if self.rank is not None and self.rank < 0:
                real_index = len(a_list) - 1 - index

            if self.maximum_length and index >= self.maximum_length:
                break

            item = a_list[real_index]

            if item != self.element:
                continue

            if skip > 0:
                skip -= 1
                continue

            if self.number_of_matches is None:
                return real_index

            indexes.append(real_index)

            if self.number_of_matches != 0 and len(indexes) >= self.number_of_matches:
                break
        return indexes


@command(b"lpush", {b"list"}, flags={b"denyoom", b"fast", b"write"})
class ListPush(Command):
    """
    summary: Prepends one or more elements to a list. Creates the key if it doesn't exist.
    complexity: >-
      O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    since: 1.0.0
    function: lpushCommand
    reply_schema:
      description: Length of the list after the push operations.
      type: integer
    """

    database: Database = dependency()
    notification: NotificationsManager = dependency()
    information: Information = dependency()
    blocking_manager: ListBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"W")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_create(self.key)

        for v in self.values:
            a_list.insert(0, v)
            self.information.rdb_changes_since_last_save += 1

        self.notification.notify(NotificationType.LIST, b"lpush", self.key)
        return len(a_list)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)


@command(b"lpushx", {b"list"}, flags={b"denyoom", b"fast", b"write"})
class ListPushIfExists(Command):
    """
    summary: Prepends one or more elements to a list only when the list exists.
    complexity: >-
      O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    since: 2.2.0
    function: lpushxCommand
    reply_schema:
      type: integer
      description: The length of the list after the push operation.
      minimum: 0
    """

    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"W")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_none(self.key)

        if a_list is None:
            return 0

        for v in self.values:
            a_list.insert(0, v)
        return len(a_list)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)


@command(b"rpop", {b"list"}, flags={b"fast", b"write"})
class ListRightPop(Command):
    """
    summary: >-
      Returns and removes one or more elements from the end of a list. Deletes the list if the last element was
      popped.
    complexity: O(N) where N is the number of elements returned
    since: 1.0.0
    function: rpopCommand
    reply_schema:
      oneOf:
      - type: 'null'
        description: Key does not exist.
      - type: string
        description: When 'COUNT' was not given, the value of the last element.
      - type: array
        description: When 'COUNT' was given, list of popped elements.
        items:
          type: string
    """

    database: Database = dependency()
    notification: NotificationsManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.count is not None and self.count < 0:
            raise ServerError(b"ERR value is out of range, must be positive")

        value = self.database.list_database.get_value_or_none(self.key)
        if not value or (self.count is not None and self.count == 0):
            return None if (self.count is None) else ArrayNone

        self.notification.notify(NotificationType.LIST, b"rpop", self.key)
        removed: ValueType
        if self.count is not None:
            removed = [value.pop(-1) for _ in range(min(len(value), self.count))]
        else:
            removed = value.pop(-1)
        if len(value) == 0:
            self.notification.notify(NotificationType.GENERIC, b"del", self.key)
        return removed


@command(b"rpoplpush", {b"list", b"slow"}, flags={b"denyoom", b"write"})
class ListRightPopLeftPush(Command):
    """
    summary: >-
      Returns the last element of a list after removing and pushing it to another list. Deletes the list if the
      last element was popped.
    complexity: O(1)
    since: 1.2.0
    function: rpoplpushCommand
    reply_schema:
      oneOf:
      - type: string
        description: The element being popped and pushed.
      - type: 'null'
        description: Source list is empty.
    """

    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()
    source: bytes = positional_parameter(key_mode=b"RW")
    destination: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        source_value = self.database.list_database.get_value_or_none(self.source)
        if source_value is None:
            return None

        destination_value = self.database.list_database.get_value_or_create(self.destination)
        value = source_value.pop(-1)
        destination_value.insert(0, value)
        return value

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.destination, in_multi=in_multi)


@command(b"lrem", {b"list", b"slow"}, flags={b"write"})
class ListRemove(Command):
    """
    summary: Removes elements from a list. Deletes the list if the last element was removed.
    complexity: >-
      O(N+M) where N is the length of the list and M is the number of elements removed.
    since: 1.0.0
    function: lremCommand
    reply_schema:
      description: The number of removed elements.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    count: int = positional_parameter()
    element: bytes = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_create(self.key)

        if not a_list:
            return 0
        count = int(self.count)
        to_delete = abs(count)
        if count < 0:
            a_list.reverse()

        deleted = 0
        for _ in range(to_delete if to_delete > 0 else a_list.count(self.element)):
            try:
                a_list.remove(self.element)
                deleted += 1
            except ValueError:
                break
        if count < 0:
            a_list.reverse()
        return deleted


@command(b"rpush", {b"list"}, flags={b"denyoom", b"fast", b"write"})
class ListPushAtTail(Command):
    """
    summary: Appends one or more elements to a list. Creates the key if it doesn't exist.
    complexity: >-
      O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    since: 1.0.0
    function: rpushCommand
    reply_schema:
      description: Length of the list after the push operations.
      type: integer
      minimum: 1
    """

    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    notification: NotificationsManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        a_list = self.database.list_database.get_value_or_create(self.key)

        for v in self.values:
            a_list.append(v)

        self.notification.notify(NotificationType.LIST, b"rpush", self.key)
        return len(a_list)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)


@command(b"rpushx", {b"list"}, flags={b"denyoom", b"fast", b"write"})
class ListPushAtTailIfExists(Command):
    """
    summary: Appends one or more elements to a list only when the list exists.
    complexity: >-
      O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    since: 2.2.0
    function: rpushxCommand
    reply_schema:
      type: integer
      description: Length of the list after the push operation.
      minimum: 0
    """

    database: Database = dependency()
    blocking_manager: ListBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    values: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        list_value = self.database.list_database.get_value_or_none(self.key)

        if list_value is None:
            return 0

        for v in self.values:
            list_value.append(v)
        return len(list_value)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)
