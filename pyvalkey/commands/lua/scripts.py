import re

WRITEABLE_FUNCTION_EXECUTOR = b"""
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
  local sha1hex = function(x) 
    if x == nil then
      error("wrong number of arguments")
    end
    return f(x) 
  end
  return sha1hex
end
""".strip()

LUA_CALL_WRAPPER = b"""
function(f, call_context)
  local wrapped_call = function(command, ...)
    print(command, unpack(arg))
    
    return f(call_context, command, unpack(arg)) 
  end
  return wrapped_call
end
""".strip()

LUA_REGISTER_FUNCTION_WRAPPER = b"""
function(f, readonly, library)
  local register_function = function(function_name, callback, ...)
    return f(library, readonly, function_name, callback, unpack(arg))
  end
  return register_function
end
""".strip()

LIBRARY_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")
