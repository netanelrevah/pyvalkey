#ifndef LUA_EXTENSIONS_H
#define LUA_EXTENSIONS_H

#include "lua.h"

/* Valkey C extension module loaders */
int luaopen_cjson(lua_State *L);
int luaopen_cmsgpack(lua_State *L);
int luaopen_bit(lua_State *L);
int luaopen_struct(lua_State *L);

#endif /* LUA_EXTENSIONS_H */
