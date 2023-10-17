from r3dis.commands.acls import acl_commands_router
from r3dis.commands.bits import bits_commands_router
from r3dis.commands.clients import client_commands_router
from r3dis.commands.configs import config_commands_router
from r3dis.commands.databases import database_commands_router
from r3dis.commands.hash_maps import hash_map_commands_router
from r3dis.commands.lists import list_commands_router
from r3dis.commands.router import RedisCommandsRouter
from r3dis.commands.servers import server_commands_router
from r3dis.commands.sets import set_commands_router
from r3dis.commands.sorted_sets import sorted_set_commands_router
from r3dis.consts import Commands

router = RedisCommandsRouter()
router.extend(list_commands_router)
router.extend(bits_commands_router)
router.extend(database_commands_router)
router.extend(set_commands_router)
router.extend(client_commands_router)
router.extend(server_commands_router)
router.extend(hash_map_commands_router)
router.extend(sorted_set_commands_router)
router.child(Commands.Acl).extend(acl_commands_router)
router.child(Commands.Config).extend(config_commands_router)
