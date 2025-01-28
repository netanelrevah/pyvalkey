from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database, KeyValue, ServerSortedSet, StringType
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"sort_ro", [b"write", b"set", b"sortedset", b"list", b"slow", b"dangerous"])
class SortReadOnly(Command):
    database: Database = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = keyword_parameter(flag=b"ALPHA", default=False)

    def _get_referenced_value(self, value: bytes, reference: bytes) -> bytes | None:
        reference_key, reference_field = reference, None
        if b"->" in reference_key and not reference_key.endswith(b"->"):
            reference_key, reference_field = reference.rsplit(b"->", 1)

        key_value = self.database.get_or_none(reference_key.replace(b"*", value, 1))
        if key_value is None:
            return None
        if reference_field:
            if isinstance(key_value.value, dict):
                return key_value.value[reference_field]
            return None
        if not isinstance(key_value.value, StringType):
            return None
        return key_value.value.value

    def internal_execute(self) -> list[bytes | None] | None:
        key_value = self.database.get_or_none(self.key)

        if key_value is None:
            return None

        if not isinstance(key_value.value, list | set | ServerSortedSet):
            raise ServerWrongTypeError(b"Operation against a key holding the wrong kind of value")

        values: list[bytes]
        if isinstance(key_value.value, ServerSortedSet):
            values = [member for score, member in key_value.value.members]
        else:
            values = list(key_value.value)

        if self.by != b"nosort":
            referenced_values: dict[bytes, bytes] | None = None
            if self.by is not None:
                referenced_values = {}
                for value in values:
                    referenced_value = self._get_referenced_value(value, self.by)
                    if referenced_value is not None:
                        referenced_values[value] = referenced_value

            scores: dict[bytes, float | bytes] = {v: v for v in values}
            for value in values:
                if not self.alpha:
                    try:
                        scores[value] = float(
                            referenced_values.get(value, 0) if referenced_values is not None else value
                        )
                    except ValueError:
                        raise ServerError(b"ERR One or more scores can't be converted into double")
                else:
                    scores[value] = referenced_values.get(value, value) if referenced_values is not None else value

            values.sort(key=lambda v: (scores[v], v), reverse=self.descending)
        elif self.descending:
            values.reverse()

        if self.limit is not None:
            offset, count = self.limit
            values = values[offset : offset + count]

        result_values: list[bytes | None] = list(values)
        if self.get_values:
            get_result: list[bytes | None] = []
            for value in values:
                for get_value in self.get_values:
                    if get_value == b"#":
                        get_result.append(value)
                        continue
                    get_result.append(self._get_referenced_value(value, get_value))
            result_values = get_result

        return result_values

    def execute(self) -> ValueType:
        return self.internal_execute()


@ServerCommandsRouter.command(b"sort", [b"read", b"set", b"sortedset", b"list", b"slow", b"dangerous"])
class Sort(Command):
    database: Database = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = keyword_parameter(flag=b"ALPHA", default=False)
    destination: bytes | None = keyword_parameter(skip_first=True, token=b"STORE", default=None, key_mode=b"W")

    def execute(self) -> ValueType:
        result_values = SortReadOnly(
            database=self.database,
            key=self.key,
            by=self.by,
            limit=self.limit,
            get_values=self.get_values,
            descending=self.descending,
            alpha=self.alpha,
        ).internal_execute()

        if self.destination is None:
            return result_values

        if not result_values:
            self.database.pop(self.destination, None)
            return 0

        self.database.set_value(
            self.destination, KeyValue(self.destination, [v if v is not None else b"" for v in result_values])
        )
        return len(result_values)
