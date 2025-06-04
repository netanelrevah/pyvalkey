from pyvalkey.listpack import ListpackIterator, listpack

MIX_LIST = [b"hello", b"foo", b"quux", b"1024"]
INT_LIST = [b"4294967296", b"-100", b"100", b"128000", b"non integer", b"much much longer non integer"]


def create_list():
    instance = listpack()
    instance.append(MIX_LIST[1])
    instance.append(MIX_LIST[2])
    instance.prepend(MIX_LIST[0])
    instance.append(MIX_LIST[3])
    return instance


def create_integers_list():
    instance = listpack()
    instance.append(INT_LIST[2])
    instance.append(INT_LIST[3])
    instance.prepend(INT_LIST[1])
    instance.prepend(INT_LIST[0])
    instance.append(INT_LIST[4])
    instance.append(INT_LIST[5])
    return instance


def test_listpack_create():
    instance = listpack()
    assert instance.total_bytes == 7
    assert instance.number_of_elements == 0


def test_listpack_create_integers_list():
    instance = create_integers_list()
    assert len(instance) == 6
    assert instance.number_of_elements == 6


def test_listpack_create_list():
    instance = create_list()
    assert len(instance) == 4
    assert instance.number_of_elements == 4


def test_listpack_prepend():
    instance = listpack()
    instance.prepend(b"abc")
    instance.prepend(b"1024")
    assert instance.number_of_elements == 2
    assert len(instance) == 2
    assert instance.seek(0) == 1024
    assert instance.seek(1) == b"abc"


def test_listpack_prepend_integers():
    instance = listpack()
    instance.prepend(127)
    instance.prepend(4095)
    instance.prepend(32767)
    instance.prepend(8388607)
    instance.prepend(2147483647)
    instance.prepend(9223372036854775807)
    assert instance.number_of_elements == 6
    assert len(instance) == 6
    assert instance.seek(0) == 9223372036854775807
    assert instance.seek(-1) == 127


def test_listpack_get_element_at_index():
    instance = create_list()
    assert instance.seek(0) == b"hello"
    assert instance.seek(3) == 1024
    assert instance.seek(-1) == 1024
    assert instance.seek(-4) == b"hello"
    assert instance.seek(4) is None
    assert instance.seek(-5) is None


def test_listpack_pop():
    instance = create_list()

    assert instance.pop(-1) == 1024
    assert instance.pop(0) == b"hello"
    assert instance.pop(-1) == b"quux"
    assert instance.pop(-1) == b"foo"
    assert instance.total_bytes == 7
    assert instance.number_of_elements == 0
    assert len(instance) == 0


def test_listpack_iterate_0_to_end():
    instance = create_list()

    iterator = iter(instance)
    for index, element in enumerate(iterator):
        assert iterator.current_index

        if isinstance(element, int):
            assert element == int(MIX_LIST[index])
        else:
            assert element == MIX_LIST[index]


def test_listpack_iterate_end_to_start():
    instance = create_list()

    reversed_mix_list = list(reversed(MIX_LIST))

    iterator = ListpackIterator(instance, reversed=True)
    for index, element in enumerate(iterator):
        assert iterator.current_index

        if isinstance(element, int):
            assert element == int(reversed_mix_list[index])
        else:
            assert element == reversed_mix_list[index]


def test_listpack_complicated():
    values = [
        b"a",
        b"1",
        b"b",
        b"2",
        b":qoCBzKxd81?NkoKOhK?e54t12=e=z<FYB^9t?Z6z0l5XKDOWnI5x_Be4KZD`d5KEOJmk]9r5s`1@qb5iU",
        b"3",
    ]

    instance = listpack()
    for item in values:
        instance.append(item)

    iterator = ListpackIterator(instance)
    for index, element in enumerate(iterator):
        if isinstance(element, int):
            assert element == int(values[index])
        else:
            assert element == values[index]
