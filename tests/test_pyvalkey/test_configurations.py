from pyvalkey.database_objects.configurations import Configurations


def test_get_field_name():
    a = Configurations()
    assert a.get_field_name(b"list-max-listpack-size") == "list_max_listpack_size"


def test_set_values():
    a = Configurations()
    a.set_value(b"zset-max-ziplist-entries", b"0")
