FUNCTION_CALL_EXECUTOR = b"""
function(f, start_ms, check_busy_reply_threshold, check_killed, KEYS, ARGV)
    local hooks = {check_busy = check_busy_reply_threshold, check_killed = check_killed, start_ms = start_ms}
    debug.sethook(function()
        hooks.check_busy(hooks.start_ms)
        if hooks.check_killed() then
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
    local hooks = {check_timeout = check_timeout, start_ms = start_ms}
    debug.sethook(function()
        if hooks.check_timeout(hooks.start_ms) then
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


LUA_MAKE_READONLY_SERVER_TABLE = b"""
function(t)
    local proxy = setmetatable({}, {
        __index = function(_, k)
            local v = rawget(t, k)
            if v ~= nil then return v end
            error(("Script attempted to access nonexistent global variable '%s'"):format(tostring(k)), 2)
        end,
    })
    return proxy
end
""".strip()


LUA_SETUP_CALL_ENV = b"""
function(f)
    local env = setmetatable({}, {
        __index = _G,
        __newindex = function(_, k, v)
            error('Attempt to modify a readonly table', 2)
        end,
    })
    setfenv(f, env)
end
""".strip()
