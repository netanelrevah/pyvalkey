import valkey
import valkey.exceptions
from pytest import raises

from tests.utils import key_value_list_to_dict


def test_basic_multiple_selectors(s: valkey.Valkey, c: valkey.Valkey):
    s.acl_setuser("selector-1", reset=True, enabled=True, nopass=True, categories=["-@all"])

    c.auth(password="password", username="selector-1")
    with raises(valkey.exceptions.NoPermissionError) as e:
        c.ping()
    assert e.value.args[0] == "User selector-1 has no permissions to run the 'ping' command"

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.set("write::foo", "var")
    assert e.value.args[0] == "User selector-1 has no permissions to run the 'set' command"

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.get("read::foo")
    assert e.value.args[0] == "User selector-1 has no permissions to run the 'get' command"

    s.acl_setuser(username="selector-1", enabled=None, selectors=[("+@write", "~write::*"), ("+@read", "~read::*")])

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.ping()
    assert e.value.args[0] == "User selector-1 has no permissions to run the 'ping' command"

    c.set("write::foo", "var")

    assert c.get("read::foo") is None

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.get("write::foo")
    assert e.value.args[0] == "No permissions to access a key"

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.set("read::foo", "bar")
    assert e.value.args[0] == "No permissions to access a key"


def test_acl_selectors_by_default_have_no_permissions(s: valkey.Valkey, c: valkey.Valkey):
    s.execute_command("ACL SETUSER", "selector-default", "reset", "()")
    user = s.acl_getuser("selector-default")

    assert 1 == len(user["selectors"])
    selector = dict(zip(user["selectors"][0][0::2], user["selectors"][0][1::2]))
    assert "" == selector["keys"]
    assert "" == selector["channels"]
    assert "-@all" == selector["commands"]


def test_deleting_selectors(s: valkey.Valkey, c: valkey.Valkey):
    s.execute_command("ACL SETUSER", "selector-del", "on", "clearselectors", "(~added-selector)")
    user = s.acl_getuser("selector-del")
    assert 1 == len(user["selectors"])
    selector = dict(zip(user["selectors"][0][0::2], user["selectors"][0][1::2]))
    assert "~added-selector" == selector["keys"]

    s.execute_command("ACL SETUSER", "selector-del", "clearselectors")
    user = s.acl_getuser("selector-del")
    assert 0 == len(user["selectors"])


def test_select_syntax_error_reports_the_error_in_the_selector_context(s: valkey.Valkey, c: valkey.Valkey):
    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "selector-syntax", "on", "(this-is-invalid)")
    assert e.value.args == ("Error in ACL SETUSER modifier '(this-is-invalid)': Syntax error",)

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "selector-syntax", "on", *"(&* &fail)".split())
    assert e.value.args == (
        "Error in ACL SETUSER modifier '(&* &fail)': Adding a pattern after the * pattern"
        " (or the 'allchannels' flag) is not valid and does not have any effect."
        " Try 'resetchannels' to start with an empty list of channels",
    )

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "selector-syntax", *"on (+PING (+SELECT (+DEL".split())
    assert e.value.args == ("Unmatched parenthesis in acl selector starting at '(+PING'.",)

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "selector-syntax", *"on (+PING (+SELECT (+DEL ) ) )".split())
    assert e.value.args == ("Error in ACL SETUSER modifier '(+PING (+SELECT (+DEL )': Syntax error",)

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "selector-syntax", *"on (+PING (+SELECT (+DEL )".split())
    assert e.value.args == ("Error in ACL SETUSER modifier '(+PING (+SELECT (+DEL )': Syntax error",)

    assert s.acl_getuser("selector-syntax") is None


def test_flexible_selector_definition(s: valkey.Valkey, c: valkey.Valkey):
    s.execute_command("ACL SETUSER", "selector-2", "(~key1 +get )", "( ~key2 +get )", "( ~key3 +get)", "(~key4 +get)")
    s.execute_command("ACL SETUSER", "selector-2", *"(~key5 +get ) ( ~key6 +get ) ( ~key7 +get) (~key8 +get)".split())

    user = s.acl_getuser("selector-2")

    assert "~key1" == key_value_list_to_dict(user["selectors"][0])["keys"]
    assert "~key2" == key_value_list_to_dict(user["selectors"][1])["keys"]
    assert "~key3" == key_value_list_to_dict(user["selectors"][2])["keys"]
    assert "~key4" == key_value_list_to_dict(user["selectors"][3])["keys"]
    assert "~key5" == key_value_list_to_dict(user["selectors"][4])["keys"]
    assert "~key6" == key_value_list_to_dict(user["selectors"][5])["keys"]
    assert "~key7" == key_value_list_to_dict(user["selectors"][6])["keys"]
    assert "~key8" == key_value_list_to_dict(user["selectors"][7])["keys"]

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "invalid-selector", " () ")
    assert e.value.args[0] == "Error in ACL SETUSER modifier ' () ': Syntax error"

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "invalid-selector", "(")
    assert e.value.args[0] == "Unmatched parenthesis in acl selector starting at '('."

    with raises(valkey.ValkeyError) as e:
        s.execute_command("ACL SETUSER", "invalid-selector", ")")
    assert e.value.args[0] == "Error in ACL SETUSER modifier ')': Syntax error"


def test_separate_read_permission(s: valkey.Valkey, c: valkey.Valkey):
    s.execute_command("ACL SETUSER", "key-permission-R", *"on nopass %R~read* +@all".split())
    c.auth("password", "key-permission-R")

    assert c.ping() is True

    s.set("readstr", "bar")
    assert c.get("readstr") == b"bar"

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.set("readstr", "bar")
    assert e.value.args[0] == "No permissions to access a key"

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.get("notread")
    assert e.value.args[0] == "No permissions to access a key"


def test_separate_write_permission(s: valkey.Valkey, c: valkey.Valkey):
    s.execute_command("ACL SETUSER", "key-permission-W", *"on nopass %W~write* +@all".split())
    c.auth("password", "key-permission-W")

    assert c.ping() is True

    c.lpush("writelist", 10)

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.get("writestr")
    assert e.value.args[0] == "No permissions to access a key"

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.lpush("notwrite", 10)
    assert e.value.args[0] == "No permissions to access a key"


def test_separate_read_and_write_permission(s: valkey.Valkey, c: valkey.Valkey):
    s.execute_command("ACL SETUSER", "key-permission-RW", *"on nopass %R~read* %W~write +@all".split())
    c.auth("password", "key-permission-RW")

    assert c.ping() is True

    s.set("read", "bar")

    c.copy("read", "write")

    with raises(valkey.exceptions.NoPermissionError) as e:
        c.copy("write", "read")
    assert e.value.args[0] == "No permissions to access a key"
