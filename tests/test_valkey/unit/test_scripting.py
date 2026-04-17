import pytest

from tests.valkey_test_client import ValkeyTestClient

pytestmark = pytest.mark.scripting

def test_eval_lua_interpreter_replies(r: ValkeyTestClient):
    """EVAL - Does Lua interpreter replies to our requests?"""
    assert r.run("eval", "return 'hello'", 0) == b"hello"

def test_eval_return_g(r: ValkeyTestClient):
    """EVAL - Return _G"""
    assert r.run("eval", "return _G", 0) is None

def test_eval_return_table_with_metatable_raise_error(r: ValkeyTestClient):
    """EVAL - Return table with a metatable that raise error"""
    assert r.run("eval", "local a = {}; setmetatable(a,{__index=function() foo() end}) return a", 0) == []

def test_eval_return_table_with_metatable_call_server(r: ValkeyTestClient):
    """EVAL - Return table with a metatable that call server"""
    r.run("del", "x")
    assert r.run("eval", "local a = {}; setmetatable(a,{__index=function() redis.call('set', 'x', '1') end}) return a", 1, "x") == []
    assert r.run("get", "x") is None

def test_eval_lua_integer_to_redis_conversion(r: ValkeyTestClient):
    """EVAL - Lua integer -> Redis protocol type conversion"""
    assert r.run("eval", "return 100.5", 0) == 100

def test_eval_lua_string_to_redis_conversion(r: ValkeyTestClient):
    """EVAL - Lua string -> Redis protocol type conversion"""
    assert r.run("eval", "return 'hello world'", 0) == b"hello world"

def test_eval_lua_boolean_to_redis_conversion(r: ValkeyTestClient):
    """EVAL - Lua true/false boolean -> Redis protocol type conversion"""
    assert r.run("eval", "return true", 0) == 1
    assert r.run("eval", "return false", 0) is None

def test_eval_lua_status_code_conversion(r: ValkeyTestClient):
    """EVAL - Lua status code reply -> Redis protocol type conversion"""
    assert r.run("eval", "return {ok='fine'}", 0) == b"fine"

def test_eval_ro_write_not_allowed(r: ValkeyTestClient):
    """EVAL_RO - write commands not allowed"""
    assert "Write commands are not allowed" in r.error("EVAL_RO", "return redis.call('set', KEYS[1], 'baz')", 1, "foo")

def test_evalsha_ro(r: ValkeyTestClient):
    """EVALSHA_RO - basic usage"""
    sha = r.run("script", "load", "return redis.call('get', KEYS[1])")
    r.run("set", "foo", "bar")
    assert r.run("EVALSHA_RO", sha, 1, "foo") == b"bar"

def test_evalsha(r: ValkeyTestClient):
    """EVALSHA - Basic usage"""
    script = "return {KEYS[1],ARGV[1]}"
    sha1 = r.run("script", "load", script)
    assert r.run("evalsha", sha1, 1, "k1", "a1") == [b"k1", b"a1"]

def test_evalsha_uppercase(r: ValkeyTestClient):
    """EVALSHA - Uppercase SHA1"""
    script = "return 1"
    sha1 = r.run("script", "load", script)
    assert r.run("evalsha", sha1.upper(), 0) == 1

def test_evalsha_noscript(r: ValkeyTestClient):
    """EVALSHA - NOSCRIPT error"""
    assert "NOSCRIPT" in r.error("evalsha", "fd758d1589d044dd850a6f05d52f2eefd27f033f", 0)

def test_script_exists(r: ValkeyTestClient):
    """SCRIPT EXISTS"""
    script = "return 1"
    sha1 = r.run("script", "load", script)
    assert r.run("script", "exists", sha1, "ffffffffffffffffffffffffffffffffffffffff") == [1, 0]

def test_script_flush(r: ValkeyTestClient):
    """SCRIPT FLUSH"""
    script = "return 1"
    sha1 = r.run("script", "load", script)
    assert r.run("script", "exists", sha1) == [1]
    r.run("script", "flush")
    assert r.run("script", "exists", sha1) == [0]

def test_eval_lua_error_reply_conversion(r: ValkeyTestClient):
    """EVAL - Lua error reply -> Redis protocol type conversion"""
    assert "this is an error" in r.error("eval", "return {err='ERR this is an error'}", 0)

def test_eval_lua_table_to_redis_conversion(r: ValkeyTestClient):
    """EVAL - Lua table -> Redis protocol type conversion"""
    assert r.run("eval", "return {1,2,3,'ciao',{1,2}}", 0) == [1, 2, 3, b"ciao", [1, 2]]

def test_eval_keys_and_argv_population(r: ValkeyTestClient):
    """EVAL - Are the KEYS and ARGV arrays populated correctly?"""
    assert r.run("eval", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", 2, "a{t}", "b{t}", "c{t}", "d{t}") == [b"a{t}", b"b{t}", b"c{t}", b"d{t}"]

def test_eval_is_lua_able_to_call_redis_api(r: ValkeyTestClient):
    """EVAL - is Lua able to call Redis API?"""
    r.run("set", "mykey", "myval")
    assert r.run("eval", "return redis.call('get',KEYS[1])", 1, "mykey") == b"myval"

def test_eval_redis_integer_to_lua_conversion(r: ValkeyTestClient):
    """EVAL - Redis integer -> Lua type conversion"""
    r.run("set", "x", 0)
    assert r.run("eval", "local foo = redis.pcall('incr',KEYS[1]); return {type(foo),foo}", 1, "x") == [b"number", 1]

def test_eval_lua_number_to_redis_integer_conversion(r: ValkeyTestClient):
    """EVAL - Lua number -> Redis integer conversion"""
    r.run("del", "hash")
    assert r.run("eval", "local foo = redis.pcall('hincrby','hash','field',200000000); return {type(foo),foo}", 0) == [b"number", 200000000]

def test_eval_redis_bulk_to_lua_conversion(r: ValkeyTestClient):
    """EVAL - Redis bulk -> Lua type conversion"""
    r.run("set", "mykey", "myval")
    assert r.run("eval", "local foo = redis.pcall('get',KEYS[1]); return {type(foo),foo}", 1, "mykey") == [b"string", b"myval"]

def test_eval_redis_multi_bulk_to_lua_conversion(r: ValkeyTestClient):
    """EVAL - Redis multi bulk -> Lua type conversion"""
    r.run("del", "mylist")
    r.run("rpush", "mylist", "a", "b", "c")
    assert r.run("eval", "local foo = redis.pcall('lrange',KEYS[1],0,-1); return {type(foo),foo[1],foo[2],foo[3],#foo}", 1, "mylist") == [b"table", b"a", b"b", b"c", 3]

def test_eval_redis_status_reply_to_lua_conversion(r: ValkeyTestClient):
    """EVAL - Redis status reply -> Lua type conversion"""
    assert r.run("eval", "local foo = redis.pcall('set',KEYS[1],'myval'); return {type(foo),foo['ok']}", 1, "mykey") == [b"table", b"OK"]

def test_eval_redis_error_reply_to_lua_conversion(r: ValkeyTestClient):
    """EVAL - Redis error reply -> Lua type conversion"""
    r.run("set", "mykey", "myval")
    assert r.run("eval", "local foo = redis.pcall('incr',KEYS[1]); return {type(foo),foo['err']}", 1, "mykey") == [b"table", b"ERR value is not an integer or out of range"]

def test_eval_redis_nil_bulk_reply_to_lua_conversion(r: ValkeyTestClient):
    """EVAL - Redis nil bulk reply -> Lua type conversion"""
    r.run("del", "mykey")
    assert r.run("eval", "local foo = redis.pcall('get',KEYS[1]); return {type(foo),foo == false}", 1, "mykey") == [b"boolean", 1]

def test_eval_lua_client_using_currently_selected_db(r: ValkeyTestClient):
    """EVAL - Is the Lua client using the currently selected DB?"""
    r.run("set", "mykey", "this is DB 9")

    assert r.run("eval", "return redis.call('get', KEYS[1])", 1, "mykey") == b"this is DB 9"

    r.run("select", 10)
    r.run("set", "mykey", "this is DB 10")
    assert r.run("eval", "return redis.call('get', KEYS[1])", 1, "mykey") == b"this is DB 10"

    r.run("select", 9)
    assert r.run("eval", "return redis.call('get', KEYS[1])", 1, "mykey") == b"this is DB 9"

def test_eval_scripts_do_not_block_on_blpop(r: ValkeyTestClient):
    """EVAL - Scripts do not block on blpop command"""
    r.run("lpush", "l", 1)
    r.run("lpop", "l")
    assert "not allowed from scripts" in r.error("eval", "return redis.pcall('blpop','l',0)", 1, "l")
