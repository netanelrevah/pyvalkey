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
    s.function_load(get_no_writes_function_code("lua", "test", "test", "return redis.call('set', 'x', '1')"), replace=True)
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
    s.function_load(get_function_code("lua", "test", "test", "return redis.call('set', KEYS[1], ARGV[1])"), replace=True)
    assert s.fcall("test", 1, "x", "foo") == b"OK"
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
    res_dict = {res[0][i]: res[0][i+1] for i in range(0, len(res[0]), 2)}
    assert res_dict[b"library_name"] == b"test"
    assert s.fcall("test", 0) == b"hello"

def test_function_restore_replace(s: valkey.Valkey):
    """FUNCTION RESTORE REPLACE"""
    s.execute_command("FUNCTION", "FLUSH")
    code1 = "#!lua name=test\nredis.register_function('test', function() return 'hello' end)"
    s.execute_command("FUNCTION", "LOAD", code1)
    payload = s.execute_command("FUNCTION", "DUMP")
    
    code2 = "#!lua name=test\nredis.register_function('test', function() return 'world' end)"
    s.execute_command("FUNCTION", "LOAD", "REPLACE", code2)
    assert s.fcall("test", 0) == b"world"
    
    s.execute_command("FUNCTION", "RESTORE", payload, "REPLACE")
    assert s.fcall("test", 0) == b"hello"
