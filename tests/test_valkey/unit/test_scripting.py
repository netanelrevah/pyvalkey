import valkey
import valkey.exceptions
from pytest import raises

def test_eval_lua_interpreter_replies(s: valkey.Valkey):
    """EVAL - Does Lua interpreter replies to our requests?"""
    assert s.eval("return 'hello'", 0) == b"hello"

def test_eval_return_g(s: valkey.Valkey):
    """EVAL - Return _G"""
    assert s.eval("return _G", 0) is None

def test_eval_return_table_with_metatable_raise_error(s: valkey.Valkey):
    """EVAL - Return table with a metatable that raise error"""
    assert s.eval("local a = {}; setmetatable(a,{__index=function() foo() end}) return a", 0) == []

def test_eval_return_table_with_metatable_call_server(s: valkey.Valkey):
    """EVAL - Return table with a metatable that call server"""
    s.delete("x")
    # Lua table with a metatable that calls server on index access shouldn't trigger the index during return conversion
    assert s.eval("local a = {}; setmetatable(a,{__index=function() redis.call('set', 'x', '1') end}) return a", 1, "x") == []
    assert s.get("x") is None

def test_eval_lua_integer_to_redis_conversion(s: valkey.Valkey):
    """EVAL - Lua integer -> Redis protocol type conversion"""
    assert s.eval("return 100.5", 0) == 100

def test_eval_lua_string_to_redis_conversion(s: valkey.Valkey):
    """EVAL - Lua string -> Redis protocol type conversion"""
    assert s.eval("return 'hello world'", 0) == b"hello world"

def test_eval_lua_boolean_to_redis_conversion(s: valkey.Valkey):
    """EVAL - Lua true/false boolean -> Redis protocol type conversion"""
    assert s.eval("return true", 0) == 1
    assert s.eval("return false", 0) is None

def test_eval_lua_status_code_conversion(s: valkey.Valkey):
    """EVAL - Lua status code reply -> Redis protocol type conversion"""
    assert s.eval("return {ok='fine'}", 0) == b"fine"

def test_eval_ro_write_not_allowed(s: valkey.Valkey):
    """EVAL_RO - write commands not allowed"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("EVAL_RO", "return redis.call('set', KEYS[1], 'baz')", 1, "foo")
    assert "Write commands are not allowed" in str(e.value)

def test_evalsha_ro(s: valkey.Valkey):
    """EVALSHA_RO - basic usage"""
    sha = s.script_load("return redis.call('get', KEYS[1])")
    s.set("foo", "bar")
    assert s.execute_command("EVALSHA_RO", sha, 1, "foo") == b"bar"

def test_evalsha(s: valkey.Valkey):
    """EVALSHA - Basic usage"""
    script = "return {KEYS[1],ARGV[1]}"
    sha1 = s.script_load(script)
    assert s.evalsha(sha1, 1, "k1", "a1") == [b"k1", b"a1"]

def test_evalsha_uppercase(s: valkey.Valkey):
    """EVALSHA - Uppercase SHA1"""
    script = "return 1"
    sha1 = s.script_load(script)
    assert s.evalsha(sha1.upper(), 0) == 1

def test_evalsha_noscript(s: valkey.Valkey):
    """EVALSHA - NOSCRIPT error"""
    with raises(valkey.exceptions.NoScriptError):
        s.evalsha("fd758d1589d044dd850a6f05d52f2eefd27f033f", 0)

def test_script_exists(s: valkey.Valkey):
    """SCRIPT EXISTS"""
    script = "return 1"
    sha1 = s.script_load(script)
    assert s.script_exists(sha1, "ffffffffffffffffffffffffffffffffffffffff") == [True, False]

def test_script_flush(s: valkey.Valkey):
    """SCRIPT FLUSH"""
    script = "return 1"
    sha1 = s.script_load(script)
    assert s.script_exists(sha1) == [True]
    s.script_flush()
    assert s.script_exists(sha1) == [False]

def test_eval_lua_error_reply_conversion(s: valkey.Valkey):
    """EVAL - Lua error reply -> Redis protocol type conversion"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.eval("return {err='ERR this is an error'}", 0)
    assert "this is an error" in str(e.value)

def test_eval_lua_table_to_redis_conversion(s: valkey.Valkey):
    """EVAL - Lua table -> Redis protocol type conversion"""
    assert s.eval("return {1,2,3,'ciao',{1,2}}", 0) == [1, 2, 3, b"ciao", [1, 2]]

def test_eval_keys_and_argv_population(s: valkey.Valkey):
    """EVAL - Are the KEYS and ARGV arrays populated correctly?"""
    assert s.eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", 2, "a{t}", "b{t}", "c{t}", "d{t}") == [b"a{t}", b"b{t}", b"c{t}", b"d{t}"]

def test_eval_is_lua_able_to_call_redis_api(s: valkey.Valkey):
    """EVAL - is Lua able to call Redis API?"""
    s.set("mykey", "myval")
    assert s.eval("return redis.call('get',KEYS[1])", 1, "mykey") == b"myval"

def test_eval_redis_integer_to_lua_conversion(s: valkey.Valkey):
    """EVAL - Redis integer -> Lua type conversion"""
    s.set("x", 0)
    assert s.eval("local foo = redis.pcall('incr',KEYS[1]); return {type(foo),foo}", 1, "x") == [b"number", 1]

def test_eval_lua_number_to_redis_integer_conversion(s: valkey.Valkey):
    """EVAL - Lua number -> Redis integer conversion"""
    s.delete("hash")
    # Using hincrby as in original test
    assert s.eval("local foo = redis.pcall('hincrby','hash','field',200000000); return {type(foo),foo}", 0) == [b"number", 200000000]

def test_eval_redis_bulk_to_lua_conversion(s: valkey.Valkey):
    """EVAL - Redis bulk -> Lua type conversion"""
    s.set("mykey", "myval")
    assert s.eval("local foo = redis.pcall('get',KEYS[1]); return {type(foo),foo}", 1, "mykey") == [b"string", b"myval"]

def test_eval_redis_multi_bulk_to_lua_conversion(s: valkey.Valkey):
    """EVAL - Redis multi bulk -> Lua type conversion"""
    s.delete("mylist")
    s.rpush("mylist", "a", "b", "c")
    assert s.eval("local foo = redis.pcall('lrange',KEYS[1],0,-1); return {type(foo),foo[1],foo[2],foo[3],#foo}", 1, "mylist") == [b"table", b"a", b"b", b"c", 3]

def test_eval_redis_status_reply_to_lua_conversion(s: valkey.Valkey):
    """EVAL - Redis status reply -> Lua type conversion"""
    assert s.eval("local foo = redis.pcall('set',KEYS[1],'myval'); return {type(foo),foo['ok']}", 1, "mykey") == [b"table", b"OK"]

def test_eval_redis_error_reply_to_lua_conversion(s: valkey.Valkey):
    """EVAL - Redis error reply -> Lua type conversion"""
    s.set("mykey", "myval")
    assert s.eval("local foo = redis.pcall('incr',KEYS[1]); return {type(foo),foo['err']}", 1, "mykey") == [b"table", b"ERR value is not an integer or out of range"]

def test_eval_redis_nil_bulk_reply_to_lua_conversion(s: valkey.Valkey):
    """EVAL - Redis nil bulk reply -> Lua type conversion"""
    s.delete("mykey")
    # In pyvalkey, nil bulk result is False in Lua
    assert s.eval("local foo = redis.pcall('get',KEYS[1]); return {type(foo),foo == false}", 1, "mykey") == [b"boolean", 1]

def test_eval_lua_client_using_currently_selected_db(s: valkey.Valkey):
    """EVAL - Is the Lua client using the currently selected DB?"""
    # Fixture s is already set to DB 9
    s.set("mykey", "this is DB 9")
    
    # Check from Lua
    assert s.eval("return redis.call('get', KEYS[1])", 1, "mykey") == b"this is DB 9"

    # Select another DB
    s.select(10)
    s.set("mykey", "this is DB 10")
    assert s.eval("return redis.call('get', KEYS[1])", 1, "mykey") == b"this is DB 10"
    
    # Back to DB 9
    s.select(9)
    assert s.eval("return redis.call('get', KEYS[1])", 1, "mykey") == b"this is DB 9"

def test_eval_scripts_do_not_block_on_blpop(s: valkey.Valkey):
    """EVAL - Scripts do not block on blpop command"""
    s.lpush("l", 1)
    s.lpop("l")
    with raises(valkey.exceptions.ResponseError) as e:
        s.eval("return redis.pcall('blpop','l',0)", 1, "l")
    assert "not allowed from scripts" in str(e.value)
