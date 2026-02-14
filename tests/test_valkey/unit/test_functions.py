import time

import valkey
import valkey.exceptions
from pytest import raises


def get_function_code(engine, lib_name, func_name, code):
    return f"#!{engine} name={lib_name}\nserver.register_function('{func_name}', function(KEYS, ARGV)\n {code} \nend)"


def get_no_writes_function_code(engine, lib_name, func_name, code):
    return f"#!{engine} name={lib_name}\nserver.register_function{{function_name='{func_name}', callback=function(KEYS, ARGV)\n {code} \nend, flags={{'no-writes'}}}}"


def test_function_basic_usage(s: valkey.Valkey):
    """FUNCTION - Basic usage"""
    s.function_load(get_function_code("LUA", "test", "test", "return 'hello'"))
    assert s.fcall("test", 0) == b"hello"


def test_function_load_with_unknown_argument(s: valkey.Valkey):
    """FUNCTION - Load with unknown argument"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FUNCTION", "LOAD", "foo", "bar", get_function_code("LUA", "test", "test", "return 'hello'"))
    assert "Unknown option given" in str(e.value)


def test_function_create_already_existing_library_error(s: valkey.Valkey):
    """FUNCTION - Create an already exiting library raise error"""
    s.function_load(get_function_code("LUA", "test", "test", "return 'hello'"), replace=True)
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(get_function_code("LUA", "test", "test", "return 'hello1'"))
    assert "already exists" in str(e.value).lower()


def test_function_create_library_with_wrong_name_format(s: valkey.Valkey):
    """FUNCTION - Create a library with wrong name format"""
    # BAD\0FORMAT is a bit tricky in Python strings but we can use escape
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(get_function_code("LUA", "bad\\0format", "test", "return 'hello1'"))
    assert "Library names can only contain letters, numbers, or underscores(_)" in str(e.value)


def test_function_create_library_with_unexisting_engine(s: valkey.Valkey):
    """FUNCTION - Create library with unexisting engine"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(get_function_code("bad_engine", "test", "test", "return 'hello1'"))
    assert "Engine 'bad_engine' not found" in str(e.value)


def test_function_test_uncompiled_script(s: valkey.Valkey):
    """FUNCTION - Test uncompiled script"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(get_function_code("LUA", "test", "test", "bad script"), replace=True)
    assert "Error compiling function" in str(e.value)


def test_function_test_replace_argument(s: valkey.Valkey):
    """FUNCTION - test replace argument"""
    s.function_load(get_function_code("LUA", "test", "test", "return 'hello1'"), replace=True)
    assert s.fcall("test", 0) == b"hello1"


def test_function_test_function_case_insensitive(s: valkey.Valkey):
    """FUNCTION - test function case insensitive"""
    # Assuming the library "test" from previous tests might still exist if s is shared,
    # but fixtures usually clean up or provide fresh instance.
    s.function_load(get_function_code("LUA", "test", "test", "return 'hello1'"), replace=True)
    assert s.fcall("TEST", 0) == b"hello1"


def test_function_test_function_delete(s: valkey.Valkey):
    """FUNCTION - test function delete"""
    s.function_load(get_function_code("LUA", "test", "test", "return 'hello'"), replace=True)
    s.function_delete("test")
    with raises(valkey.exceptions.ResponseError) as e:
        s.fcall("test", 0)
    assert "Function not found" in str(e.value)


def test_function_test_fcall_bad_arguments(s: valkey.Valkey):
    """FUNCTION - test fcall bad arguments"""
    s.function_load(get_function_code("LUA", "test", "test", "return 'hello'"), replace=True)
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FCALL", "test", "bad_arg")
    assert "Bad number of keys provided" in str(e.value)


def test_function_test_fcall_bad_number_of_keys_arguments(s: valkey.Valkey):
    """FUNCTION - test fcall bad number of keys arguments"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FCALL", "test", "10", "key1")
    assert "Number of keys can't be greater than number of args" in str(e.value)


def test_function_test_fcall_negative_number_of_keys(s: valkey.Valkey):
    """FUNCTION - test fcall negative number of keys"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FCALL", "test", "-1", "key1")
    assert "Number of keys can't be negative" in str(e.value)


def test_function_test_delete_on_not_existing_library(s: valkey.Valkey):
    """FUNCTION - test delete on not exiting library"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_delete("test1")
    assert "Library not found" in str(e.value)


def test_function_test_function_kill_when_not_running(s: valkey.Valkey):
    """FUNCTION - test function kill when function is not running"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FUNCTION", "KILL")
    assert "No scripts in execution" in str(e.value)


def test_function_test_wrong_subcommand(s: valkey.Valkey):
    """FUNCTION - test wrong subcommand"""
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FUNCTION", "bad_subcommand")
    assert "unknown subcommand" in str(e.value).lower()


def test_function_test_fcall_ro_with_read_only_commands(s: valkey.Valkey):
    """FUNCTION - test fcall_ro with write command"""
    s.function_load(
        get_no_writes_function_code("lua", "test", "test", "return redis.call('set', 'x', '1')"), replace=True
    )
    with raises(valkey.exceptions.ResponseError) as e:
        s.execute_command("FCALL_RO", "test", "1", "x")
    assert "Write commands are not allowed from read-only scripts" in str(e.value)


def test_function_test_fcall_ro_with_read_only_commands_success(s: valkey.Valkey):
    """FUNCTION - test fcall_ro with read only commands"""
    s.function_load(get_no_writes_function_code("lua", "test", "test", "return redis.call('get', 'x')"), replace=True)
    s.set("x", "1")
    res = s.execute_command("FCALL_RO", "test", "1", "x")
    # Handle int or bytes
    if isinstance(res, int):
        assert res == 1
    else:
        assert res == b"1"


def test_function_test_keys_and_argv(s: valkey.Valkey):
    """FUNCTION - test keys and argv"""
    s.function_load(
        get_function_code(
            "lua", "test", "test", "print('in', KEYS['1'], ARGV['1'])\nreturn redis.call('set', KEYS[1], ARGV[1])"
        ),
        replace=True,
    )
    assert s.execute_command("FCALL", "test", "1", "x", "foo") == b"OK"
    assert s.get("x") == b"foo"


def test_function_test_function_flush(s: valkey.Valkey):
    """FUNCTION - test function flush"""
    s.function_load(get_function_code("lua", "test", "test", "return 1"), replace=True)
    # Using execute_command directly to avoid valkey-py sending extra args for FUNCTION LIST
    assert len(s.execute_command("FUNCTION", "LIST")) > 0
    s.execute_command("FUNCTION", "FLUSH")
    assert s.execute_command("FUNCTION", "LIST") == []


def test_function_dump_restore(s: valkey.Valkey):
    """FUNCTION DUMP and RESTORE"""
    s.execute_command("FUNCTION", "FLUSH")
    code = "#!lua name=test\nredis.register_function('test', function() return 'hello' end)"
    s.execute_command("FUNCTION", "LOAD", code)

    payload = s.execute_command("FUNCTION", "DUMP")
    s.execute_command("FUNCTION", "DELETE", "test")
    assert s.execute_command("FUNCTION", "LIST") == []

    s.execute_command("FUNCTION", "RESTORE", payload)
    res = s.execute_command("FUNCTION", "LIST")
    assert len(res) == 1
    # res[0] is an interleaved list of [key, value, key, value, ...]
    res_dict = {res[0][i]: res[0][i + 1] for i in range(0, len(res[0]), 2)}
    assert res_dict[b"library_name"] == b"test"
    assert s.fcall("test", 0) == b"hello"


def test_function_restore_replace(s: valkey.Valkey):
    """FUNCTION RESTORE REPLACE"""
    s.execute_command("FUNCTION", "FLUSH")
    code1 = "#!lua name=test\nserver.register_function('test', function() return 'hello' end)"
    s.execute_command("FUNCTION", "LOAD", code1)
    payload = s.execute_command("FUNCTION", "DUMP")

    code2 = "#!lua name=test\nserver.register_function('test', function() return 'world' end)"
    s.execute_command("FUNCTION", "LOAD", "REPLACE", code2)
    assert s.fcall("test", 0) == b"world"

    s.execute_command("FUNCTION", "RESTORE", payload, "REPLACE")
    assert s.fcall("test", 0) == b"hello"


def test_libraries_shared_function_access_globals(s: valkey.Valkey):
    """LIBRARIES - test shared function can access default globals"""
    s.execute_command("FUNCTION", "FLUSH")
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
    s.function_load(code)
    assert s.fcall("f1", 0) == b"PONG"


def test_libraries_usage_and_code_sharing(s: valkey.Valkey):
    """LIBRARIES - usage and code sharing"""
    s.execute_command("FUNCTION", "FLUSH")
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
    s.function_load(code)
    assert s.fcall("f1", 0) == 2
    assert s.fcall("f2", 0) == 3


def test_libraries_registration_failure_reverts_load(s: valkey.Valkey):
    """LIBRARIES - test registration failure revert the entire load"""
    s.execute_command("FUNCTION", "FLUSH")
    # First load a working version
    code_ok = """#!lua name=lib1
        server.register_function('f1', function() return 2 end)
    """
    s.function_load(code_ok)

    # Try to replace with a version that fails at second registration
    code_bad = """#!lua name=lib1
        server.register_function('f1', function() return 10 end)
        server.register_function('f2', 'not a function')
    """
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code_bad, replace=True)
    assert "must be a function" in str(e.value)

    # Should still have the old version
    assert s.fcall("f1", 0) == 2
    with raises(valkey.exceptions.ResponseError):
        s.fcall("f2", 0)


def test_libraries_registration_function_name_collision(s: valkey.Valkey):
    """LIBRARIES - test registration function name collision"""
    s.execute_command("FUNCTION", "FLUSH")
    s.function_load("#!lua name=lib1\nserver.register_function('f1', function() return 1 end)")

    code_coll = "#!lua name=lib2\nserver.register_function('f1', function() return 2 end)"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code_coll)
    assert "already exists" in str(e.value)


def test_libraries_registration_collision_same_library(s: valkey.Valkey):
    """LIBRARIES - test registration function name collision on same library"""
    code = """#!lua name=lib2
        server.register_function('f1', function() return 1 end)
        server.register_function('f1', function() return 1 end)
    """
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    assert "already exists" in str(e.value).lower()


def test_libraries_registration_no_argument(s: valkey.Valkey):
    """LIBRARIES - test registration with no argument"""
    code = "#!lua name=lib2\nserver.register_function()"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    # The actual error message might vary based on implementation
    assert "wrong number of arguments" in str(e.value).lower()


def test_libraries_registration_only_name(s: valkey.Valkey):
    """LIBRARIES - test registration with only name"""
    code = "#!lua name=lib2\nserver.register_function('f1')"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    assert "callback" in str(e.value).lower() or "arguments" in str(e.value).lower()


def test_libraries_math_random_forbidden(s: valkey.Valkey):
    """LIBRARIES - math.random from function load (forbidden)"""
    code = "#!lua name=lib2\nreturn math.random()"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    assert "nonexistent global variable 'math'" in str(e.value)


def test_libraries_redis_call_forbidden_in_load(s: valkey.Valkey):
    """LIBRARIES - redis.call from function load (forbidden)"""
    code = "#!lua name=lib2\nreturn redis.call('ping')"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    # It might be 'call' or 'redis' depending on what's exposed
    assert "nonexistent global variable" in str(e.value)


def test_libraries_no_functions_registered(s: valkey.Valkey):
    """LIBRARIES - register library with no functions"""
    code = "#!lua name=lib\nreturn 1"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    assert "No functions registered" in str(e.value)


def test_libraries_global_protection(s: valkey.Valkey):
    """LIBRARIES - verify global protection on the load run"""
    code = "#!lua name=lib\na = 1"
    with raises(valkey.exceptions.ResponseError) as e:
        s.function_load(code)
    assert "Attempt to modify a readonly table" in str(e.value)


def test_function_kill(s: valkey.Valkey):
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
    s.config_set("busy-reply-threshold", 10)
    s.function_load(get_function_code("lua", "test", "test", "local a = 1 while true do a = a + 1 end"), replace=True)
    rd = s.client()
    rd.fcall("test", 0)
    # Wait for the script to be detected as busy

    time.sleep(0.2)
    with raises(valkey.exceptions.ResponseError) as e:
        s.ping()
    assert "BUSY" in str(e.value)
    stats = s.execute_command("FUNCTION", "STATS")
    assert any(
        b"name" in stat and stat[b"name"] == b"test" and b"duration_ms" in stat and b"engines" in stat for stat in stats
    )
    s.execute_command("FUNCTION", "KILL")
    time.sleep(0.2)
    assert s.ping() == b"PONG"
    with raises(valkey.exceptions.ResponseError) as e:
        rd.read()
    assert "Script killed by user with FUNCTION KILL" in str(e.value)
    rd.close()
