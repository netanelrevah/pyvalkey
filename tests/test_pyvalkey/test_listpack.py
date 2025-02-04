from pyvalkey.listpack import listpack

MIX_LIST = [b"hello", b"foo", b"quux", b"1024"]
INT_LIST = [b"4294967296", b"-100", b"100", b"128000", b"non integer", b"much much longer non integer"]


def create_list():
    instance = listpack()
    instance.append(MIX_LIST[1])
    instance.append(MIX_LIST[2])
    instance.append(MIX_LIST[0])
    instance.append(MIX_LIST[3])
    return instance


def create_int_list():
    instance = listpack()
    instance.append(INT_LIST[2])
    instance.append(INT_LIST[3])
    instance.append(INT_LIST[1])
    instance.append(INT_LIST[0])
    instance.append(INT_LIST[4])
    instance.append(INT_LIST[5])
    return instance


def test_listpack_create():
    instance = listpack()
    assert instance.total_bytes == 7
    assert instance.number_of_elements == 0


def test_listpack_create_int_list():
    instance = create_int_list()
    assert len(instance) == 6


def test_listpack_create_list():
    instance = create_list()
    assert len(instance) == 4
