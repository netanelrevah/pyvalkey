import pytest

from tests.utils import assert_raises, key_value_list_to_dict
from tests.valkey_test_client import ValkeyError, ValkeyTestClient

pytestmark = pytest.mark.acl


def test_basic_multiple_selectors(r: ValkeyTestClient, rd: ValkeyTestClient):
    r.run("acl", "setuser", "selector-1", "reset", "on", "nopass", "-@all")

    rd.run("auth", "selector-1", "password")
    assert "User selector-1 has no permissions to run the 'ping' command" == rd.error("ping")
    assert "User selector-1 has no permissions to run the 'set' command" == rd.error("set", "write::foo", "var")
    assert "User selector-1 has no permissions to run the 'get' command" == rd.error("get", "read::foo")

    r.run("acl", "setuser", "selector-1", "(+@write ~write::*)", "(+@read ~read::*)")

    assert "User selector-1 has no permissions to run the 'ping' command" == rd.error("ping")

    rd.run("set", "write::foo", "var")

    assert rd.run("get", "read::foo") is None

    assert "No permissions to access a key" == rd.error("get", "write::foo")
    assert "No permissions to access a key" == rd.error("set", "read::foo", "bar")


def test_acl_selectors_by_default_have_no_permissions(r: ValkeyTestClient):
    r.run("acl", "setuser", "selector-default", "reset", "()")
    user = r.run("acl", "getuser", "selector-default")

    user_dict = dict(zip(user[0::2], user[1::2]))
    assert 1 == len(user_dict[b"selectors"])
    selector = dict(zip(user_dict[b"selectors"][0][0::2], user_dict[b"selectors"][0][1::2]))
    assert b"" == selector[b"keys"]
    assert b"" == selector[b"channels"]
    assert b"-@all" == selector[b"commands"]


def test_deleting_selectors(r: ValkeyTestClient):
    r.run("acl", "setuser", "selector-del", "on", "clearselectors", "(~added-selector)")
    user = r.run("acl", "getuser", "selector-del")
    user_dict = dict(zip(user[0::2], user[1::2]))
    assert 1 == len(user_dict[b"selectors"])
    selector = dict(zip(user_dict[b"selectors"][0][0::2], user_dict[b"selectors"][0][1::2]))
    assert b"~added-selector" == selector[b"keys"]

    r.run("acl", "setuser", "selector-del", "clearselectors")
    user = r.run("acl", "getuser", "selector-del")
    user_dict = dict(zip(user[0::2], user[1::2]))
    assert 0 == len(user_dict[b"selectors"])


def test_select_syntax_error_reports_the_error_in_the_selector_context(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "ERR Error in ACL SETUSER modifier '(this-is-invalid)': Syntax error"):
        r.run("acl", "setuser", "selector-syntax", "on", "(this-is-invalid)")

    assert r.error("acl", "setuser", "selector-syntax", "on", "(&*", "&fail)") == (
        "ERR Error in ACL SETUSER modifier '(&* &fail)': Adding a pattern after the * pattern"
        " (or the 'allchannels' flag) is not valid and does not have any effect."
        " Try 'resetchannels' to start with an empty list of channels"
    )

    assert "Unmatched parenthesis in acl selector starting at '(+PING'." == r.error(
        "acl", "setuser", "selector-syntax", "on", "(+PING", "(+SELECT", "(+DEL"
    )

    assert "ERR Error in ACL SETUSER modifier '(+PING (+SELECT (+DEL )': Syntax error" == r.error(
        "acl", "setuser", "selector-syntax", "on", "(+PING", "(+SELECT", "(+DEL", ")", ")", ")"
    )

    assert "ERR Error in ACL SETUSER modifier '(+PING (+SELECT (+DEL )': Syntax error" == r.error(
        "acl", "setuser", "selector-syntax", "on", "(+PING", "(+SELECT", "(+DEL", ")"
    )

    assert r.run("acl", "getuser", "selector-syntax") is None


def test_flexible_selector_definition(r: ValkeyTestClient):
    r.run("acl", "setuser", "selector-2", "(~key1 +get )", "( ~key2 +get )", "( ~key3 +get)", "(~key4 +get)")
    r.run(
        "acl",
        "setuser",
        "selector-2",
        "(~key5",
        "+get",
        ")",
        "(",
        "~key6",
        "+get",
        ")",
        "(",
        "~key7",
        "+get)",
        "(~key8",
        "+get)",
    )

    user = r.run("acl", "getuser", "selector-2")
    user_dict = dict(zip(user[0::2], user[1::2]))

    assert b"~key1" == key_value_list_to_dict(user_dict[b"selectors"][0])[b"keys"]
    assert b"~key2" == key_value_list_to_dict(user_dict[b"selectors"][1])[b"keys"]
    assert b"~key3" == key_value_list_to_dict(user_dict[b"selectors"][2])[b"keys"]
    assert b"~key4" == key_value_list_to_dict(user_dict[b"selectors"][3])[b"keys"]
    assert b"~key5" == key_value_list_to_dict(user_dict[b"selectors"][4])[b"keys"]
    assert b"~key6" == key_value_list_to_dict(user_dict[b"selectors"][5])[b"keys"]
    assert b"~key7" == key_value_list_to_dict(user_dict[b"selectors"][6])[b"keys"]
    assert b"~key8" == key_value_list_to_dict(user_dict[b"selectors"][7])[b"keys"]

    assert "Error in ACL SETUSER modifier ' () ': Syntax error" == r.error("acl", "setuser", "invalid-selector", " () ")
    assert "Unmatched parenthesis in acl selector starting at '('." == r.error(
        "acl", "setuser", "invalid-selector", "("
    )
    assert "Error in ACL SETUSER modifier ')': Syntax error" == r.error("acl", "setuser", "invalid-selector", ")")


def test_separate_read_permission(r: ValkeyTestClient, rd: ValkeyTestClient):
    r.run("acl", "setuser", "key-permission-R", "on", "nopass", "%R~read*", "+@all")
    rd.run("auth", "key-permission-R", "password")

    assert rd.run("ping") == b"PONG"

    r.run("set", "readstr", "bar")
    assert rd.run("get", "readstr") == b"bar"

    assert "No permissions to access a key" == rd.error("set", "readstr", "bar")
    assert "No permissions to access a key" == rd.error("get", "notread")


def test_separate_write_permission(r: ValkeyTestClient, rd: ValkeyTestClient):
    r.run("acl", "setuser", "key-permission-W", "on", "nopass", "%W~write*", "+@all")
    rd.run("auth", "key-permission-W", "password")

    assert rd.run("ping") == b"PONG"

    rd.run("lpush", "writelist", 10)

    assert "No permissions to access a key" == rd.error("get", "writestr")
    assert "No permissions to access a key" == rd.error("lpush", "notwrite", 10)


def test_separate_read_and_write_permission(r: ValkeyTestClient, rd: ValkeyTestClient):
    r.run("acl", "setuser", "key-permission-RW", "on", "nopass", "%R~read*", "%W~write", "+@all")
    rd.run("auth", "key-permission-RW", "password")

    assert rd.run("ping") == b"PONG"

    r.run("set", "read", "bar")

    rd.run("copy", "read", "write")

    assert "No permissions to access a key" == rd.error("copy", "write", "read")
