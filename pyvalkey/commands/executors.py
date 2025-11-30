from __future__ import annotations

from dataclasses import dataclass
from traceback import print_exc
from typing import TYPE_CHECKING

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.transactions_commands import (
    TransactionDiscard,
    TransactionExecute,
    TransactionStart,
    TransactionWatch,
)
from pyvalkey.database_objects.acl import ACLUser
from pyvalkey.database_objects.errors import (
    CommandPermissionError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ServerWrongTypeError,
)
from pyvalkey.resp import RespError, ValueType
from pyvalkey.utils.times import now_us

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command


TransactionCommand = TransactionExecute | TransactionDiscard | TransactionStart | TransactionWatch


@dataclass
class CommandExecutor:
    command: Command
    client_context: ClientContext

    @property
    def current_user(self) -> ACLUser | None:
        return self.client_context.current_user

    async def execute(self) -> ValueType:
        command_statistics = self.client_context.server_context.information.get_command_statistics(
            self.command.full_command_name
        )

        try:
            if self.client_context.server_context.configurations.maxmemory > 0:
                if b"denyoom" in self.command.flags:
                    raise ServerError(b"ERR OOM command not allowed when used memory > 'maxmemory'.")

            if self.client_context.transaction_context is not None:
                if b"nomulti" in self.command.flags:
                    raise ServerError(b"ERR Command not allowed inside a transaction")
                if not isinstance(self.command, TransactionCommand):
                    self.client_context.transaction_context.commands.append(self.command)
                    return "QUEUED"

            if self.current_user:
                self.current_user.check_permissions(self.command)

            command_statistics.calls += 1
            await self.command.before()
            start = now_us()
            result = self.command.execute()
            command_statistics.microseconds += now_us() - start
            if not isinstance(result, RespError):
                await self.command.after()
            self.client_context.current_client.last_command = self.command.full_command_name
            return result
        except ServerWrongNumberOfArgumentsError:
            command_statistics.rejected_calls += 1
            self.client_context.server_context.information.error_statistics[b"ERR"] += 1
            return RespError(
                b"ERR wrong number of arguments for '" + self.command.full_command_name.lower() + b"' command"
            )
        except ServerWrongTypeError:
            command_statistics.failed_calls += 1
            self.client_context.server_context.information.error_statistics[b"WRONGTYPE"] += 1
            return RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
        except CommandPermissionError as e:
            command_statistics.rejected_calls += 1
            if not self.current_user:
                raise e
            self.client_context.server_context.information.error_statistics[b"NOPERM"] += 1
            return RespError(
                b"NOPERM User "
                + self.current_user.name
                + b" has no permissions to run the '"
                + e.command_name
                + b"' command"
            )

        except ServerError as e:
            command_statistics.failed_calls += 1
            if self.client_context.transaction_context is not None:
                self.client_context.transaction_context.is_aborted = True
            self.client_context.server_context.information.error_statistics[e.message.split()[0].upper()] += 1
            return RespError(e.message)
        except Exception as e:
            print_exc()
            self.client_context.server_context.information.error_statistics[b"ERR"] += 1
            raise e
