FUNCTION_CALL_EXECUTOR = b"""
function(f, start_ms, check_busy_reply_threshold, check_killed, KEYS, ARGV)
    debug.sethook(function()
        check_busy_reply_threshold(start_ms)
        if check_killed() then
            print("killed")
            error("Timeout reached!")
        end
    end, "", 1000)

    local status, result = pcall(f, KEYS, ARGV)

    debug.sethook()

    return status, result
end
""".strip()


FUNCTION_LOAD_EXECUTOR = b"""
function(start_ms, f, check_timeout)
    debug.sethook(function()
        if check_timeout(start_ms) then
            print("timeout")
            error("Timeout reached!")
        end
    end, "", 1000)

    local status, result = pcall(f)

    debug.sethook()

    if status == false then
        error(result)
    end
    return result
end
""".strip()


LUA_IMITATE_LUA_FUNCTION = b"""
function(f)
  return function(x) 
    if x == nil then
      error("wrong number of arguments")
    end
    return f(x) 
  end
end
""".strip()


LUA_CALL_WRAPPER = b"""
function(f, call_context)
  return function(command, ...)
    print(command, unpack(arg))
    
    return f(call_context, command, unpack(arg)) 
  end
end
""".strip()


LUA_MAKE_READONLY_SERVER_TABLE = b"""
function(t)
    return setmetatable({}, {
        __index = function(_, k)
            local v = rawget(t, k)
            if v ~= nil then return v end
            error(("Script attempted to access nonexistent global variable '%s'"):format(tostring(k)), 2)
        end,
        __newindex = function(_, k, v)
            error("Attempt to modify a readonly table", 2)
        end
    })
end
""".strip()


LUA_REGISTER_FUNCTION_WRAPPER = b"""
function(f, writeable, library)
  print("bla")
  return function(function_name, callback, ...)
    print("blabla")
    return f(library, writeable, function_name, callback, unpack(arg))
  end
end
""".strip()
