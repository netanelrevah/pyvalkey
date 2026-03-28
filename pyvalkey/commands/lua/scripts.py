FUNCTION_CALL_EXECUTOR = b"""
function(f, start_ms, check_busy_reply_threshold, check_killed, KEYS, ARGV)
    local state = rawget(_G, '__pyv_state')
    state.cb = check_busy_reply_threshold
    state.ms = start_ms
    state.ck = check_killed
    debug.sethook(function()
        rawget(_G, '__pyv_state').cb(rawget(_G, '__pyv_state').ms)
        if rawget(_G, '__pyv_state').ck() then
            debug.sethook(function() error("Timeout reached!") end, "", 1)
            error("Timeout reached!")
        end
    end, "", 1000)

    local status, result = pcall(f, KEYS, ARGV)

    debug.sethook()
    state.cb = nil
    state.ms = nil
    state.ck = nil

    return status, result
end
""".strip()


FUNCTION_LOAD_EXECUTOR = b"""
function(start_ms, f, check_timeout)
    rawset(_G, '__pyv_ct', check_timeout)
    rawset(_G, '__pyv_ms', start_ms)
    debug.sethook(function()
        if rawget(_G, '__pyv_ct')(rawget(_G, '__pyv_ms')) then
            error("Timeout reached!")
        end
    end, "", 1000)

    local status, result = pcall(f)

    debug.sethook()
    rawset(_G, '__pyv_ct', nil)
    rawset(_G, '__pyv_ms', nil)

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
    })
    setfenv(f, env)
    return env
end
""".strip()
