import time

import pytest

from tests.valkey_test_client import ValkeyTestClient

pytestmark = pytest.mark.scripting


def get_function_code(engine, lib_name, func_name, code):
    return f"#!{engine} name={lib_name}\nserver.register_function('{func_name}', function(KEYS, ARGV)\n {code} \nend)"


def get_no_writes_function_code(engine, lib_name, func_name, code):
    return f"#!{engine} name={lib_name}\nserver.register_function{{function_name='{func_name}', callback=function(KEYS, ARGV)\n {code} \nend, flags={{'no-writes'}}}}"


def test_function_basic_usage(r: ValkeyTestClient):
    """FUNCTION - Basic usage"""
    r.run("function", "load", get_function_code("LUA", "test", "test", "return 'hello'"))
    assert r.run("fcall", "test", 0) == b"hello"


def test_function_load_with_unknown_argument(r: ValkeyTestClient):
    """FUNCTION - Load with unknown argument"""
    assert "Unknown option given" in r.error("function", "load", "foo", "bar", get_function_code("LUA", "test", "test", "return 'hello'"))


def test_function_create_already_existing_library_error(r: ValkeyTestClient):
    """FUNCTION - Create an already exiting library raise error"""
    r.run("function", "load", "REPLACE", get_function_code("LUA", "test", "test", "return 'hello'"))
    assert "already exists" in r.error("function", "load", get_function_code("LUA", "test", "test", "return 'hello1'")).lower()


def test_function_create_library_with_wrong_name_format(r: ValkeyTestClient):
    """FUNCTION - Create a library with wrong name format"""
    assert "Library names can only contain letters, numbers, or underscores(_)" in r.error("function", "load", get_function_code("LUA", "bad\\0format", "test", "return 'hello1'"))


def test_function_create_library_with_unexisting_engine(r: ValkeyTestClient):
    """FUNCTION - Create library with unexisting engine"""
    assert "Engine 'bad_engine' not found" in r.error("function", "load", get_function_code("bad_engine", "test", "test", "return 'hello1'"))


def test_function_test_uncompiled_script(r: ValkeyTestClient):
    """FUNCTION - Test uncompiled script"""
    assert "Error compiling function" in r.error("function", "load", "REPLACE", get_function_code("LUA", "test", "test", "bad script"))


def test_function_test_replace_argument(r: ValkeyTestClient):
    """FUNCTION - test replace argument"""
    r.run("function", "load", "REPLACE", get_function_code("LUA", "test", "test", "return 'hello1'"))
    assert r.run("fcall", "test", 0) == b"hello1"


def test_function_test_function_case_insensitive(r: ValkeyTestClient):
    """FUNCTION - test function case insensitive"""
    r.run("function", "load", "REPLACE", get_function_code("LUA", "test", "test", "return 'hello1'"))
    assert r.run("fcall", "TEST", 0) == b"hello1"


def test_function_test_function_delete(r: ValkeyTestClient):
    """FUNCTION - test function delete"""
    r.run("function", "load", "REPLACE", get_function_code("LUA", "test", "test", "return 'hello'"))
    r.run("function", "delete", "test")
    assert "Function not found" in r.error("fcall", "test", 0)


def test_function_test_fcall_bad_arguments(r: ValkeyTestClient):
    """FUNCTION - test fcall bad arguments"""
    r.run("function", "load", "REPLACE", get_function_code("LUA", "test", "test", "return 'hello'"))
    assert "Bad number of keys provided" in r.error("fcall", "test", "bad_arg")


def test_function_test_fcall_bad_number_of_keys_arguments(r: ValkeyTestClient):
    """FUNCTION - test fcall bad number of keys arguments"""
    assert "Number of keys can't be greater than number of args" in r.error("fcall", "test", "10", "key1")


def test_function_test_fcall_negative_number_of_keys(r: ValkeyTestClient):
    """FUNCTION - test fcall negative number of keys"""
    assert "Number of keys can't be negative" in r.error("fcall", "test", "-1", "key1")


def test_function_test_delete_on_not_existing_library(r: ValkeyTestClient):
    """FUNCTION - test delete on not exiting library"""
    assert "Library not found" in r.error("function", "delete", "test1")


def test_function_test_function_kill_when_not_running(r: ValkeyTestClient):
    """FUNCTION - test function kill when function is not running"""
    assert "No scripts in execution" in r.error("function", "kill")


def test_function_test_wrong_subcommand(r: ValkeyTestClient):
    """FUNCTION - test wrong subcommand"""
    assert "unknown subcommand" in r.error("function", "bad_subcommand").lower()


def test_function_test_fcall_ro_with_read_only_commands(r: ValkeyTestClient):
    """FUNCTION - test fcall_ro with write command"""
    r.run("function", "load", "REPLACE", get_no_writes_function_code("lua", "test", "test", "return redis.call('set', 'x', '1')"))
    assert "Write commands are not allowed from read-only scripts" in r.error("fcall_ro", "test", "1", "x")


def test_function_test_fcall_ro_with_read_only_commands_success(r: ValkeyTestClient):
    """FUNCTION - test fcall_ro with read only commands"""
    r.run("function", "load", "REPLACE", get_no_writes_function_code("lua", "test", "test", "return redis.call('get', 'x')"))
    r.run("set", "x", "1")
    res = r.run("fcall_ro", "test", "1", "x")
    if isinstance(res, int):
        assert res == 1
    else:
        assert res == b"1"


def test_function_test_keys_and_argv(r: ValkeyTestClient):
    """FUNCTION - test keys and argv"""
    r.run("function", "load", "REPLACE", get_function_code("lua", "test", "test", "return redis.call('set', KEYS[1], ARGV[1])"))
    assert r.run("fcall", "test", "1", "x", "foo") == b"OK"
    assert r.run("get", "x") == b"foo"


def test_function_test_function_flush(r: ValkeyTestClient):
    """FUNCTION - test function flush"""
    r.run("function", "load", "REPLACE", get_function_code("lua", "test", "test", "return 1"))
    assert len(r.run("function", "list")) > 0
    r.run("function", "flush")
    assert r.run("function", "list") == []


def test_function_dump_restore(r: ValkeyTestClient):
    """FUNCTION DUMP and RESTORE"""
    r.run("function", "flush")
    code = "#!lua name=test\nredis.register_function('test', function() return 'hello' end)"
    r.run("function", "load", code)

    payload = r.run("function", "dump")
    r.run("function", "delete", "test")
    assert r.run("function", "list") == []

    r.run("function", "restore", payload)
    res = r.run("function", "list")
    assert len(res) == 1
    res_dict = {res[0][i]: res[0][i + 1] for i in range(0, len(res[0]), 2)}
    assert res_dict[b"library_name"] == b"test"
    assert r.run("fcall", "test", 0) == b"hello"


def test_function_restore_replace(r: ValkeyTestClient):
    """FUNCTION RESTORE REPLACE"""
    r.run("function", "flush")
    code1 = "#!lua name=test\nserver.register_function('test', function() return 'hello' end)"
    r.run("function", "load", code1)
    payload = r.run("function", "dump")

    code2 = "#!lua name=test\nserver.register_function('test', function() return 'world' end)"
    r.run("function", "load", "REPLACE", code2)
    assert r.run("fcall", "test", 0) == b"world"

    r.run("function", "restore", payload, "REPLACE")
    assert r.run("fcall", "test", 0) == b"hello"

def test_function_kill(r: ValkeyTestClient, rd: ValkeyTestClient):
    """
    test {FUNCTION - test function kill} {
        set rd [valkey_deferring_client]
        r config set busy-reply-threshold 10
        r function load REPLACE [get_function_code lua test test {local a = 1 while true do a = a + 1 end}]
        $rd fcall test 0
        after 200
        catch {r ping} e
        assert_match {BUSY*} $e
        assert_match {running_script {name test command {fcall test 0} duration_ms *} engines {*}} [r FUNCTION STATS]
        r function kill
        after 200 ; # Give some time to Lua to call the hook again...
        assert_equal [r ping] "PONG"
        assert_error {ERR Script killed by user with FUNCTION KILL*} {$rd read}
        $rd close
    }
    """
    r.run("config", "set", "busy-reply-threshold", 10)
    r.run("function", "load", "REPLACE", get_function_code("lua", "test", "test", "local a = 1 while true do a = a + 1 end"))
    deferred = rd.send("fcall", "test", 0)

    time.sleep(0.2)
    assert "BUSY" in r.error("ping")
    stats = r.run("function", "stats")
    assert b'running_script' == stats[0]
    assert b'name' == stats[1][0]
    assert b"duration_ms" == stats[1][4]
    assert b"engines" == stats[2]
    r.run("function", "kill")
    time.sleep(0.2)
    assert r.run("ping") == b"PONG"
    assert "Script killed by user with FUNCTION KILL" in r.error_from(deferred)


def test_libraries_shared_function_access_globals(r: ValkeyTestClient):
    """LIBRARIES - test shared function can access default globals"""
    r.run("function", "flush")
    code = """#!lua name=lib1
        local function ping()
            return redis.call('ping')
        end
        server.register_function(
            'f1',
            function(keys, args)
                return ping()
            end
        )
    """
    r.run("function", "load", code)
    assert r.run("fcall", "f1", 0) == b"PONG"


def test_libraries_usage_and_code_sharing(r: ValkeyTestClient):
    """LIBRARIES - usage and code sharing"""
    r.run("function", "flush")
    code = """#!lua name=lib1
        local function add1(a)
            return a + 1
        end
        server.register_function(
            'f1',
            function(keys, args)
                return add1(1)
            end
        )
        server.register_function(
            'f2',
            function(keys, args)
                return add1(2)
            end
        )
    """
    r.run("function", "load", code)
    assert r.run("fcall", "f1", 0) == 2
    assert r.run("fcall", "f2", 0) == 3


def test_libraries_registration_failure_reverts_load(r: ValkeyTestClient):
    """LIBRARIES - test registration failure revert the entire load"""
    r.run("function", "flush")
    code_ok = """#!lua name=lib1
        server.register_function('f1', function() return 2 end)
    """
    r.run("function", "load", code_ok)

    code_bad = """#!lua name=lib1
        server.register_function('f1', function() return 10 end)
        server.register_function('f2', 'not a function')
    """
    assert "must be a function" in r.error("function", "load", "REPLACE", code_bad)

    assert r.run("fcall", "f1", 0) == 2
    r.error("fcall", "f2", 0)


def test_libraries_registration_function_name_collision(r: ValkeyTestClient):
    """LIBRARIES - test registration function name collision"""
    r.run("function", "flush")
    r.run("function", "load", "#!lua name=lib1\nserver.register_function('f1', function() return 1 end)")

    assert "already exists" in r.error("function", "load", "#!lua name=lib2\nserver.register_function('f1', function() return 2 end)")


def test_libraries_registration_collision_same_library(r: ValkeyTestClient):
    """LIBRARIES - test registration function name collision on same library"""
    code = """#!lua name=lib2
        server.register_function('f1', function() return 1 end)
        server.register_function('f1', function() return 1 end)
    """
    assert "already exists" in r.error("function", "load", code).lower()


def test_libraries_registration_no_argument(r: ValkeyTestClient):
    """LIBRARIES - test registration with no argument"""
    assert "wrong number of arguments" in r.error("function", "load", "#!lua name=lib2\nserver.register_function()").lower()


def test_libraries_registration_only_name(r: ValkeyTestClient):
    """LIBRARIES - test registration with only name"""
    assert "calling server.register_function with a single argument is only applicable to Lua table" in r.error("function", "load", "#!lua name=lib2\nserver.register_function('f1')")


def test_libraries_math_random_forbidden(r: ValkeyTestClient):
    """LIBRARIES - math.random from function load (forbidden)"""
    assert "nonexistent global variable 'math'" in r.error("function", "load", "#!lua name=lib2\nreturn math.random()")


def test_libraries_redis_call_forbidden_in_load(r: ValkeyTestClient):
    """LIBRARIES - redis.call from function load (forbidden)"""
    assert "nonexistent global variable" in r.error("function", "load", "#!lua name=lib2\nreturn redis.call('ping')")


def test_libraries_no_functions_registered(r: ValkeyTestClient):
    """LIBRARIES - register library with no functions"""
    assert "No functions registered" in r.error("function", "load", "#!lua name=lib\nreturn 1")


def test_libraries_global_protection(r: ValkeyTestClient):
    """LIBRARIES - verify global protection on the load run"""
    assert "Attempt to modify a readonly table" in r.error("function", "load", "#!lua name=lib\na = 1")
