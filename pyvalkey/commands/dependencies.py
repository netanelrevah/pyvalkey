from pyvalkey.blocking import BlockingManager, ListBlockingManager, SortedSetBlockingManager, StreamBlockingManager
from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.creators import CommandCreator
from pyvalkey.commands.scripting import FunctionsEngine, ScriptsEngine
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.information import Information
from pyvalkey.notifications import ClientSubscriptions, NotificationsManager
from pyvalkey.resp import RespProtocolVersion


def register_dependencies() -> None:
    CommandCreator.register_dependency(ACL, lambda ctx: ctx.server_context.acl)
    CommandCreator.register_dependency(BlockingManager, lambda ctx: ctx.server_context.blocking_manager)
    CommandCreator.register_dependency(
        ListBlockingManager, lambda ctx: ctx.server_context.blocking_manager.list_blocking_manager
    )
    CommandCreator.register_dependency(
        SortedSetBlockingManager, lambda ctx: ctx.server_context.blocking_manager.sorted_set_blocking_manager
    )
    CommandCreator.register_dependency(
        StreamBlockingManager, lambda ctx: ctx.server_context.blocking_manager.stream_blocking_manager
    )
    CommandCreator.register_dependency(ServerContext, lambda ctx: ctx.server_context)
    CommandCreator.register_dependency(ClientContext, lambda ctx: ctx)
    CommandCreator.register_dependency(ScriptsEngine, lambda ctx: ctx.server_context.scripts_engine)
    CommandCreator.register_dependency(FunctionsEngine, lambda ctx: ctx.server_context.functions_engine)
    CommandCreator.register_dependency(Configurations, lambda ctx: ctx.server_context.configurations)
    CommandCreator.register_dependency(Database, lambda ctx: ctx.database)
    CommandCreator.register_dependency(Information, lambda ctx: ctx.server_context.information)
    CommandCreator.register_dependency(NotificationsManager, lambda ctx: ctx.notifications_manager)
    CommandCreator.register_dependency(ClientSubscriptions, lambda ctx: ctx.subscriptions)
    CommandCreator.register_dependency(RespProtocolVersion, lambda ctx: ctx.protocol)
