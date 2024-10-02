import pytest
import valkey

pytestmark = [pytest.mark.set]


"""
proc create_set {key entries} {
        r del $key
        foreach entry $entries { r sadd $key $entry }
}
"""


"""
array set initelems {listpack {foo} hashtable {foo}}
"""


"""
for {set i 0} {$i < 130} {incr i} {
        lappend initelems(hashtable) [format "i%03d" $i]
}
"""


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("listpack"), ("hashtable")])
def sadd_scard_sismember_smismember_smembers_basics_type(s: valkey.Valkey):
    """
    {
            create_set myset $initelems($type)
            assert_encoding $type myset
            assert_equal 1 [r sadd myset bar]
            assert_equal 0 [r sadd myset bar]
            assert_equal [expr [llength $initelems($type)] + 1] [r scard myset]
            assert_equal 1 [r sismember myset foo]
            assert_equal 1 [r sismember myset bar]
            assert_equal 0 [r sismember myset bla]
            assert_equal {1} [r smismember myset foo]
            assert_equal {1 1} [r smismember myset foo bar]
            assert_equal {1 0} [r smismember myset foo bla]
            assert_equal {0 1} [r smismember myset bla foo]
            assert_equal {0} [r smismember myset bla]
            assert_equal "bar $initelems($type)" [lsort [r smembers myset]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sadd_scard_sismember_smismember_smembers_basics_intset(s: valkey.Valkey):
    """
    {
            create_set myset {17}
            assert_encoding intset myset
            assert_equal 1 [r sadd myset 16]
            assert_equal 0 [r sadd myset 16]
            assert_equal 2 [r scard myset]
            assert_equal 1 [r sismember myset 16]
            assert_equal 1 [r sismember myset 17]
            assert_equal 0 [r sismember myset 18]
            assert_equal {1} [r smismember myset 16]
            assert_equal {1 1} [r smismember myset 16 17]
            assert_equal {1 0} [r smismember myset 16 18]
            assert_equal {0 1} [r smismember myset 18 16]
            assert_equal {0} [r smismember myset 18]
            assert_equal {16 17} [lsort [r smembers myset]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smismember_smembers_scard_against_non_set(s: valkey.Valkey):
    """
    {
            r lpush mylist foo
            assert_error WRONGTYPE* {r smismember mylist bar}
            assert_error WRONGTYPE* {r smembers mylist}
            assert_error WRONGTYPE* {r scard mylist}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smismember_smembers_scard_against_non_existing_key(s: valkey.Valkey):
    """
    {
            assert_equal {0} [r smismember myset1 foo]
            assert_equal {0 0} [r smismember myset1 foo bar]
            assert_equal {} [r smembers myset1]
            assert_equal {0} [r scard myset1]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smismember_requires_one_or_more_members(s: valkey.Valkey):
    """
    {
            r del zmscoretest
            r zadd zmscoretest 10 x
            r zadd zmscoretest 20 y

            catch {r smismember zmscoretest} e
            assert_match {*ERR*wrong*number*arg*} $e
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sadd_against_non_set(s: valkey.Valkey):
    """
    {
            r lpush mylist foo
            assert_error WRONGTYPE* {r sadd mylist bar}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sadd_a_non_integer_against_a_small_intset(s: valkey.Valkey):
    """
    {
            create_set myset {1 2 3}
            assert_encoding intset myset
            assert_equal 1 [r sadd myset a]
            assert_encoding listpack myset
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sadd_a_non_integer_against_a_large_intset(s: valkey.Valkey):
    """
    {
            create_set myset {0}
            for {set i 1} {$i < 130} {incr i} {r sadd myset $i}
            assert_encoding intset myset
            assert_equal 1 [r sadd myset a]
            assert_encoding hashtable myset
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sadd_an_integer_larger_than_64_bits(s: valkey.Valkey):
    """
    {
            create_set myset {213244124402402314402033402}
            assert_encoding listpack myset
            assert_equal 1 [r sismember myset 213244124402402314402033402]
            assert_equal {1} [r smismember myset 213244124402402314402033402]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sadd_an_integer_larger_than_64_bits_to_a_large_intset(s: valkey.Valkey):
    """
    {
            create_set myset {0}
            for {set i 1} {$i < 130} {incr i} {r sadd myset $i}
            assert_encoding intset myset
            r sadd myset 213244124402402314402033402
            assert_encoding hashtable myset
            assert_equal 1 [r sismember myset 213244124402402314402033402]
            assert_equal {1} [r smismember myset 213244124402402314402033402]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("single"), ("multiple"), ("single_multiple")])
def sadd_overflows_the_maximum_allowed_integers_in_an_intset_type(s: valkey.Valkey):
    """
    {
            r del myset

            if {$type == "single"} {
                # All are single sadd commands.
                for {set i 0} {$i < 512} {incr i} { r sadd myset $i }
            } elseif {$type == "multiple"} {
                # One sadd command to add all elements.
                set args {}
                for {set i 0} {$i < 512} {incr i} { lappend args $i }
                r sadd myset {*}$args
            } elseif {$type == "single_multiple"} {
                # First one sadd adds an element (creates a key) and then one sadd adds all elements.
                r sadd myset 1
                set args {}
                for {set i 0} {$i < 512} {incr i} { lappend args $i }
                r sadd myset {*}$args
            }

            assert_encoding intset myset
            assert_equal 512 [r scard myset]
            assert_equal 1 [r sadd myset 512]
            assert_encoding hashtable myset
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("single"), ("multiple"), ("single_multiple")])
def sadd_overflows_the_maximum_allowed_elements_in_a_listpack_type(s: valkey.Valkey):
    """
    {
            r del myset

            if {$type == "single"} {
                # All are single sadd commands.
                r sadd myset a
                for {set i 0} {$i < 127} {incr i} { r sadd myset $i }
            } elseif {$type == "multiple"} {
                # One sadd command to add all elements.
                set args {}
                lappend args a
                for {set i 0} {$i < 127} {incr i} { lappend args $i }
                r sadd myset {*}$args
            } elseif {$type == "single_multiple"} {
                # First one sadd adds an element (creates a key) and then one sadd adds all elements.
                r sadd myset a
                set args {}
                lappend args a
                for {set i 0} {$i < 127} {incr i} { lappend args $i }
                r sadd myset {*}$args
            }

            assert_encoding listpack myset
            assert_equal 128 [r scard myset]
            assert_equal 1 [r sadd myset b]
            assert_encoding hashtable myset
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def variadic_sadd(s: valkey.Valkey):
    """
    {
            r del myset
            assert_equal 3 [r sadd myset a b c]
            assert_equal 2 [r sadd myset A a b c B]
            assert_equal [lsort {A a b c B}] [lsort [r smembers myset]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def set_encoding_after_debug_reload(s: valkey.Valkey):
    """
    {
            r del myintset
            r del myhashset
            r del mylargeintset
            r del mysmallset
            for {set i 0} {$i <  100} {incr i} { r sadd myintset $i }
            for {set i 0} {$i < 1280} {incr i} { r sadd mylargeintset $i }
            for {set i 0} {$i <   50} {incr i} { r sadd mysmallset [format "i%03d" $i] }
            for {set i 0} {$i <  256} {incr i} { r sadd myhashset [format "i%03d" $i] }
            assert_encoding intset myintset
            assert_encoding hashtable mylargeintset
            assert_encoding listpack mysmallset
            assert_encoding hashtable myhashset

            r debug reload
            assert_encoding intset myintset
            assert_encoding hashtable mylargeintset
            assert_encoding listpack mysmallset
            assert_encoding hashtable myhashset
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("listpack"), ("hashtable")])
def srem_basics_type(s: valkey.Valkey):
    """
    {
                create_set myset $initelems($type)
                r sadd myset ciao
                assert_encoding $type myset
                assert_equal 0 [r srem myset qux]
                assert_equal 1 [r srem myset ciao]
                assert_equal $initelems($type) [lsort [r smembers myset]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def srem_basics_intset(s: valkey.Valkey):
    """
    {
            create_set myset {3 4 5}
            assert_encoding intset myset
            assert_equal 0 [r srem myset 6]
            assert_equal 1 [r srem myset 4]
            assert_equal {3 5} [lsort [r smembers myset]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def srem_with_multiple_arguments(s: valkey.Valkey):
    """
    {
            r del myset
            r sadd myset a b c d
            assert_equal 0 [r srem myset k k k]
            assert_equal 2 [r srem myset b d x y]
            lsort [r smembers myset]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def srem_variadic_version_with_more_args_needed_to_destroy_the_key(s: valkey.Valkey):
    """
    {
            r del myset
            r sadd myset 1 2 3
            r srem myset 1 2 3 4 5 6 7 8
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sintercard_with_illegal_arguments(s: valkey.Valkey):
    """
    {
            assert_error "ERR wrong number of arguments for 'sintercard' command" {r sintercard}
            assert_error "ERR wrong number of arguments for 'sintercard' command" {r sintercard 1}

            assert_error "ERR numkeys*" {r sintercard 0 myset{t}}
            assert_error "ERR numkeys*" {r sintercard a myset{t}}

            assert_error "ERR Number of keys*" {r sintercard 2 myset{t}}
            assert_error "ERR Number of keys*" {r sintercard 3 myset{t} myset2{t}}

            assert_error "ERR syntax error*" {r sintercard 1 myset{t} myset2{t}}
            assert_error "ERR syntax error*" {r sintercard 1 myset{t} bar_arg}
            assert_error "ERR syntax error*" {r sintercard 1 myset{t} LIMIT}

            assert_error "ERR LIMIT*" {r sintercard 1 myset{t} LIMIT -1}
            assert_error "ERR LIMIT*" {r sintercard 1 myset{t} LIMIT a}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sintercard_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            r del set{t}
            r sadd set{t} a b c
            r set key1{t} x

            assert_error "WRONGTYPE*" {r sintercard 1 key1{t}}
            assert_error "WRONGTYPE*" {r sintercard 2 set{t} key1{t}}
            assert_error "WRONGTYPE*" {r sintercard 2 key1{t} noset{t}}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sintercard_against_non_existing_key(s: valkey.Valkey):
    """
    {
            assert_equal 0 [r sintercard 1 non-existing-key]
            assert_equal 0 [r sintercard 1 non-existing-key limit 0]
            assert_equal 0 [r sintercard 1 non-existing-key limit 10]
    }
    """
    assert False


"""
set smallenc listpack
 set bigenc hashtable

"""


"""
array set encoding TCLDoubleQuotedWord(value='1 $bigenc 2 $bigenc 3 $smallenc 4 $bigenc 5 $smallenc')
"""


"""
for {set i 1} {$i <= 5} {incr i} {
            r del [format "set%d{t}" $i]
}
"""


"""
for {set i 0} {$i < 200} {incr i} {
            r sadd set1{t} $i
            r sadd set2{t} [expr $i+195]
}
"""


"""
r sadd set3{t} $i

"""


"""
for {set i 5} {$i < 200} {incr i} {
            r sadd set4{t} $i
}
"""


"""
r sadd set5{t} 0

 # To make sure the sets are encoded as the type we are testing -- also
 # when the VM is enabled and the values may be swapped in and out
 # while the tests are running -- an extra element is added to every
 # set that determines its encoding.
 set large 200
 if {$type eq "regular"} {
            set large foo
}
"""


"""
for {set i 1} {$i <= 5} {incr i} {
            r sadd [format "set%d{t}" $i] $large
}
"""


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def generated_sets_must_be_encoded_correctly_type(s: valkey.Valkey):
    """
    {
                for {set i 1} {$i <= 5} {incr i} {
                    assert_encoding $encoding($i) [format "set%d{t}" $i]
                }
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sinter_with_two_sets_type(s: valkey.Valkey):
    """
    {
                assert_equal [list 195 196 197 198 199 $large] [lsort [r sinter set1{t} set2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sintercard_with_two_sets_type(s: valkey.Valkey):
    """
    {
                assert_equal 6 [r sintercard 2 set1{t} set2{t}]
                assert_equal 6 [r sintercard 2 set1{t} set2{t} limit 0]
                assert_equal 3 [r sintercard 2 set1{t} set2{t} limit 3]
                assert_equal 6 [r sintercard 2 set1{t} set2{t} limit 10]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sinterstore_with_two_sets_type(s: valkey.Valkey):
    """
    {
                r sinterstore setres{t} set1{t} set2{t}
                assert_encoding $smallenc setres{t}
                assert_equal [list 195 196 197 198 199 $large] [lsort [r smembers setres{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sinterstore_with_two_sets_after_a_debug_reload_type(s: valkey.Valkey):
    """
    {
                r debug reload
                r sinterstore setres{t} set1{t} set2{t}
                assert_encoding $smallenc setres{t}
                assert_equal [list 195 196 197 198 199 $large] [lsort [r smembers setres{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sunion_with_two_sets_type(s: valkey.Valkey):
    """
    {
                set expected [lsort -uniq "[r smembers set1{t}] [r smembers set2{t}]"]
                assert_equal $expected [lsort [r sunion set1{t} set2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sunionstore_with_two_sets_type(s: valkey.Valkey):
    """
    {
                r sunionstore setres{t} set1{t} set2{t}
                assert_encoding $bigenc setres{t}
                set expected [lsort -uniq "[r smembers set1{t}] [r smembers set2{t}]"]
                assert_equal $expected [lsort [r smembers setres{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sinter_against_three_sets_type(s: valkey.Valkey):
    """
    {
                assert_equal [list 195 199 $large] [lsort [r sinter set1{t} set2{t} set3{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sintercard_against_three_sets_type(s: valkey.Valkey):
    """
    {
                assert_equal 3 [r sintercard 3 set1{t} set2{t} set3{t}]
                assert_equal 3 [r sintercard 3 set1{t} set2{t} set3{t} limit 0]
                assert_equal 2 [r sintercard 3 set1{t} set2{t} set3{t} limit 2]
                assert_equal 3 [r sintercard 3 set1{t} set2{t} set3{t} limit 10]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sinterstore_with_three_sets_type(s: valkey.Valkey):
    """
    {
                r sinterstore setres{t} set1{t} set2{t} set3{t}
                assert_equal [list 195 199 $large] [lsort [r smembers setres{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sunion_with_non_existing_keys_type(s: valkey.Valkey):
    """
    {
                set expected [lsort -uniq "[r smembers set1{t}] [r smembers set2{t}]"]
                assert_equal $expected [lsort [r sunion nokey1{t} set1{t} set2{t} nokey2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sdiff_with_two_sets_type(s: valkey.Valkey):
    """
    {
                assert_equal {0 1 2 3 4} [lsort [r sdiff set1{t} set4{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sdiff_with_three_sets_type(s: valkey.Valkey):
    """
    {
                assert_equal {1 2 3 4} [lsort [r sdiff set1{t} set4{t} set5{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sdiffstore_with_three_sets_type(s: valkey.Valkey):
    """
    {
                r sdiffstore setres{t} set1{t} set4{t} set5{t}
                # When we start with intsets, we should always end with intsets.
                if {$type eq {intset}} {
                    assert_encoding intset setres{t}
                }
                assert_equal {1 2 3 4} [lsort [r smembers setres{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type"], [("regular"), ("intset")])
def sinter_or_sunion_or_sdiff_with_three_same_sets_type(s: valkey.Valkey):
    """
    {
                set expected [lsort "[r smembers set1{t}]"]
                assert_equal $expected [lsort [r sinter set1{t} set1{t} set1{t}]]
                assert_equal $expected [lsort [r sunion set1{t} set1{t} set1{t}]]
                assert_equal {} [lsort [r sdiff set1{t} set1{t} set1{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinterstore_with_two_listpack_sets_where_result_is_intset(s: valkey.Valkey):
    """
    {
            r del setres{t} set1{t} set2{t}
            r sadd set1{t} a b c 1 3 6 x y z
            r sadd set2{t} e f g 1 2 3 u v w
            assert_encoding listpack set1{t}
            assert_encoding listpack set2{t}
            r sinterstore setres{t} set1{t} set2{t}
            assert_equal [list 1 3] [lsort [r smembers setres{t}]]
            assert_encoding intset setres{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinterstore_with_two_hashtable_sets_where_result_is_intset(s: valkey.Valkey):
    """
    {
            r del setres{t} set1{t} set2{t}
            r sadd set1{t} a b c 444 555 666
            r sadd set2{t} e f g 111 222 333
            set expected {}
            for {set i 1} {$i < 130} {incr i} {
                r sadd set1{t} $i
                r sadd set2{t} $i
                lappend expected $i
            }
            assert_encoding hashtable set1{t}
            assert_encoding hashtable set2{t}
            r sinterstore setres{t} set1{t} set2{t}
            assert_equal [lsort $expected] [lsort [r smembers setres{t}]]
            assert_encoding intset setres{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sunion_hashtable_and_listpack(s: valkey.Valkey):
    """
    {
            # This adds code coverage for adding a non-sds string to a hashtable set
            # which already contains the string.
            r del set1{t} set2{t}
            set union {abcdefghijklmnopqrstuvwxyz1234567890 a b c 1 2 3}
            create_set set1{t} $union
            create_set set2{t} {a b c}
            assert_encoding hashtable set1{t}
            assert_encoding listpack set2{t}
            assert_equal [lsort $union] [lsort [r sunion set1{t} set2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiff_with_first_set_empty(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}
            r sadd set2{t} 1 2 3 4
            r sadd set3{t} a b c d
            r sdiff set1{t} set2{t} set3{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiff_with_same_set_two_times(s: valkey.Valkey):
    """
    {
            r del set1
            r sadd set1 a b c 1 2 3 4 5 6
            r sdiff set1 set1
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiff_fuzzing(s: valkey.Valkey):
    """
    {
            for {set j 0} {$j < 100} {incr j} {
                unset -nocomplain s
                array set s {}
                set args {}
                set num_sets [expr {[randomInt 10]+1}]
                for {set i 0} {$i < $num_sets} {incr i} {
                    set num_elements [randomInt 100]
                    r del set_$i{t}
                    lappend args set_$i{t}
                    while {$num_elements} {
                        set ele [randomValue]
                        r sadd set_$i{t} $ele
                        if {$i == 0} {
                            set s($ele) x
                        } else {
                            unset -nocomplain s($ele)
                        }
                        incr num_elements -1
                    }
                }
                set result [lsort [r sdiff {*}$args]]
                assert_equal $result [lsort [array names s]]
            }
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiff_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            # with an empty set
            r set key1{t} x
            assert_error "WRONGTYPE*" {r sdiff key1{t} noset{t}}
            # different order
            assert_error "WRONGTYPE*" {r sdiff noset{t} key1{t}}

            # with a legal set
            r del set1{t}
            r sadd set1{t} a b c
            assert_error "WRONGTYPE*" {r sdiff key1{t} set1{t}}
            # different order
            assert_error "WRONGTYPE*" {r sdiff set1{t} key1{t}}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiff_should_handle_non_existing_key_as_empty(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}

            r sadd set1{t} a b c
            r sadd set2{t} b c d
            assert_equal {a} [lsort [r sdiff set1{t} set2{t} set3{t}]]
            assert_equal {} [lsort [r sdiff set3{t} set2{t} set1{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiffstore_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t} key1{t}
            r set key1{t} x

            # with en empty dstkey
            assert_error "WRONGTYPE*" {r SDIFFSTORE set3{t} key1{t} noset{t}}
            assert_equal 0 [r exists set3{t}]
            assert_error "WRONGTYPE*" {r SDIFFSTORE set3{t} noset{t} key1{t}}
            assert_equal 0 [r exists set3{t}]

            # with a legal dstkey
            r sadd set1{t} a b c
            r sadd set2{t} b c d
            r sadd set3{t} e
            assert_error "WRONGTYPE*" {r SDIFFSTORE set3{t} key1{t} set1{t} noset{t}}
            assert_equal 1 [r exists set3{t}]
            assert_equal {e} [lsort [r smembers set3{t}]]

            assert_error "WRONGTYPE*" {r SDIFFSTORE set3{t} set1{t} key1{t} set2{t}}
            assert_equal 1 [r exists set3{t}]
            assert_equal {e} [lsort [r smembers set3{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sdiffstore_should_handle_non_existing_key_as_empty(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}

            r set setres{t} xxx
            assert_equal 0 [r sdiffstore setres{t} foo111{t} bar222{t}]
            assert_equal 0 [r exists setres{t}]

            # with a legal dstkey, should delete dstkey
            r sadd set3{t} a b c
            assert_equal 0 [r sdiffstore set3{t} set1{t} set2{t}]
            assert_equal 0 [r exists set3{t}]

            r sadd set1{t} a b c
            assert_equal 3 [r sdiffstore set3{t} set1{t} set2{t}]
            assert_equal 1 [r exists set3{t}]
            assert_equal {a b c} [lsort [r smembers set3{t}]]

            # with a legal dstkey and empty set2, should delete the dstkey
            r sadd set3{t} a b c
            assert_equal 0 [r sdiffstore set3{t} set2{t} set1{t}]
            assert_equal 0 [r exists set3{t}]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinter_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            r set key1{t} x
            assert_error "WRONGTYPE*" {r sinter key1{t} noset{t}}
            # different order
            assert_error "WRONGTYPE*" {r sinter noset{t} key1{t}}

            r sadd set1{t} a b c
            assert_error "WRONGTYPE*" {r sinter key1{t} set1{t}}
            # different order
            assert_error "WRONGTYPE*" {r sinter set1{t} key1{t}}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinter_should_handle_non_existing_key_as_empty(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}
            r sadd set1{t} a b c
            r sadd set2{t} b c d
            r sinter set1{t} set2{t} set3{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinter_with_same_integer_elements_but_different_encoding(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t}
            r sadd set1{t} 1 2 3
            r sadd set2{t} 1 2 3 a
            r srem set2{t} a
            assert_encoding intset set1{t}
            assert_encoding listpack set2{t}
            lsort [r sinter set1{t} set2{t}]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinterstore_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t} key1{t}
            r set key1{t} x

            # with en empty dstkey
            assert_error "WRONGTYPE*" {r sinterstore set3{t} key1{t} noset{t}}
            assert_equal 0 [r exists set3{t}]
            assert_error "WRONGTYPE*" {r sinterstore set3{t} noset{t} key1{t}}
            assert_equal 0 [r exists set3{t}]

            # with a legal dstkey
            r sadd set1{t} a b c
            r sadd set2{t} b c d
            r sadd set3{t} e
            assert_error "WRONGTYPE*" {r sinterstore set3{t} key1{t} set2{t} noset{t}}
            assert_equal 1 [r exists set3{t}]
            assert_equal {e} [lsort [r smembers set3{t}]]

            assert_error "WRONGTYPE*" {r sinterstore set3{t} noset{t} key1{t} set2{t}}
            assert_equal 1 [r exists set3{t}]
            assert_equal {e} [lsort [r smembers set3{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sinterstore_against_non_existing_keys_should_delete_dstkey(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}

            r set setres{t} xxx
            assert_equal 0 [r sinterstore setres{t} foo111{t} bar222{t}]
            assert_equal 0 [r exists setres{t}]

            # with a legal dstkey
            r sadd set3{t} a b c
            assert_equal 0 [r sinterstore set3{t} set1{t} set2{t}]
            assert_equal 0 [r exists set3{t}]

            r sadd set1{t} a b c
            assert_equal 0 [r sinterstore set3{t} set1{t} set2{t}]
            assert_equal 0 [r exists set3{t}]

            assert_equal 0 [r sinterstore set3{t} set2{t} set1{t}]
            assert_equal 0 [r exists set3{t}]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sunion_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            r set key1{t} x
            assert_error "WRONGTYPE*" {r sunion key1{t} noset{t}}
            # different order
            assert_error "WRONGTYPE*" {r sunion noset{t} key1{t}}

            r del set1{t}
            r sadd set1{t} a b c
            assert_error "WRONGTYPE*" {r sunion key1{t} set1{t}}
            # different order
            assert_error "WRONGTYPE*" {r sunion set1{t} key1{t}}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sunion_should_handle_non_existing_key_as_empty(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}

            r sadd set1{t} a b c
            r sadd set2{t} b c d
            assert_equal {a b c d} [lsort [r sunion set1{t} set2{t} set3{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sunionstore_against_non_set_should_throw_error(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t} key1{t}
            r set key1{t} x

            # with en empty dstkey
            assert_error "WRONGTYPE*" {r sunionstore set3{t} key1{t} noset{t}}
            assert_equal 0 [r exists set3{t}]
            assert_error "WRONGTYPE*" {r sunionstore set3{t} noset{t} key1{t}}
            assert_equal 0 [r exists set3{t}]

            # with a legal dstkey
            r sadd set1{t} a b c
            r sadd set2{t} b c d
            r sadd set3{t} e
            assert_error "WRONGTYPE*" {r sunionstore set3{t} key1{t} key2{t} noset{t}}
            assert_equal 1 [r exists set3{t}]
            assert_equal {e} [lsort [r smembers set3{t}]]

            assert_error "WRONGTYPE*" {r sunionstore set3{t} noset{t} key1{t} key2{t}}
            assert_equal 1 [r exists set3{t}]
            assert_equal {e} [lsort [r smembers set3{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sunionstore_should_handle_non_existing_key_as_empty(s: valkey.Valkey):
    """
    {
            r del set1{t} set2{t} set3{t}

            r set setres{t} xxx
            assert_equal 0 [r sunionstore setres{t} foo111{t} bar222{t}]
            assert_equal 0 [r exists setres{t}]

            # set1 set2 both empty, should delete the dstkey
            r sadd set3{t} a b c
            assert_equal 0 [r sunionstore set3{t} set1{t} set2{t}]
            assert_equal 0 [r exists set3{t}]

            r sadd set1{t} a b c
            r sadd set3{t} e f
            assert_equal 3 [r sunionstore set3{t} set1{t} set2{t}]
            assert_equal 1 [r exists set3{t}]
            assert_equal {a b c} [lsort [r smembers set3{t}]]

            r sadd set3{t} d
            assert_equal 3 [r sunionstore set3{t} set2{t} set1{t}]
            assert_equal 1 [r exists set3{t}]
            assert_equal {a b c} [lsort [r smembers set3{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def sunionstore_against_non_existing_keys_should_delete_dstkey(s: valkey.Valkey):
    """
    {
            r set setres{t} xxx
            assert_equal 0 [r sunionstore setres{t} foo111{t} bar222{t}]
            assert_equal 0 [r exists setres{t}]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type", "contents"], [("listpack", "a b c"), ("intset", "1 2 3")])
def spop_basics_type(s: valkey.Valkey):
    """
    {
                create_set myset $contents
                assert_encoding $type myset
                assert_equal $contents [lsort [list [r spop myset] [r spop myset] [r spop myset]]]
                assert_equal 0 [r scard myset]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type", "contents"], [("listpack", "a b c"), ("intset", "1 2 3")])
def spop_with_count_1_type(s: valkey.Valkey):
    """
    {
                create_set myset $contents
                assert_encoding $type myset
                assert_equal $contents [lsort [list [r spop myset 1] [r spop myset 1] [r spop myset 1]]]
                assert_equal 0 [r scard myset]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(["type", "contents"], [("listpack", "a b c"), ("intset", "1 2 3")])
def srandmember_type(s: valkey.Valkey):
    """
    {
                create_set myset $contents
                unset -nocomplain myset
                array set myset {}
                for {set i 0} {$i < 100} {incr i} {
                    set myset([r srandmember myset]) 1
                }
                assert_equal $contents [lsort [array names myset]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def spop_integer_from_listpack_set(s: valkey.Valkey):
    """
    {
            create_set myset {a 1 2 3 4 5 6 7}
            assert_encoding listpack myset
            set a [r spop myset]
            set b [r spop myset]
            assert {[string is digit $a] || [string is digit $b]}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(
    ["type", "contents"],
    [
        ("listpack", "a b c d e f g h i j k l m n o p q r s t u v w x y z"),
        ("intset", "1 10 11 12 13 14 15 16 17 18 19 2 20 21 22 23 24 25 26 3 4 5 6 7 8 9"),
        ("hashtable", "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 b c d e f g h i j k l m n o p q r s t u v w x y z"),
    ],
)
def spop_with_count_type(s: valkey.Valkey):
    """
    {
                create_set myset $contents
                assert_encoding $type myset
                assert_equal $contents [lsort [concat [r spop myset 11] [r spop myset 9] [r spop myset 0] [r spop myset 4] [r spop myset 1] [r spop myset 0] [r spop myset 1] [r spop myset 0]]]
                assert_equal 0 [r scard myset]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def spop_using_integers_testing_knuth_s_and_floyd_s_algorithm(s: valkey.Valkey):
    """
    {
            create_set myset {1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20}
            assert_encoding intset myset
            assert_equal 20 [r scard myset]
            r spop myset 1
            assert_equal 19 [r scard myset]
            r spop myset 2
            assert_equal 17 [r scard myset]
            r spop myset 3
            assert_equal 14 [r scard myset]
            r spop myset 10
            assert_equal 4 [r scard myset]
            r spop myset 10
            assert_equal 0 [r scard myset]
            r spop myset 1
            assert_equal 0 [r scard myset]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def spop_using_integers_with_knuth_s_algorithm(s: valkey.Valkey):
    """
    {
            r spop nonexisting_key 100
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(
    ["type", "content"],
    [
        ("intset", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"),
        ("listpack", "a 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"),
    ],
)
def spop_new_implementation_code_path_1_type(s: valkey.Valkey):
    """
    {
            create_set myset $content
            assert_encoding $type myset
            set res [r spop myset 30]
            assert {[lsort $content] eq [lsort $res]}
            assert_equal {0} [r exists myset]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(
    ["type", "content"],
    [
        ("intset", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"),
        ("listpack", "a 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"),
    ],
)
def spop_new_implementation_code_path_2_type(s: valkey.Valkey):
    """
    {
            create_set myset $content
            assert_encoding $type myset
            set res [r spop myset 2]
            assert {[llength $res] == 2}
            assert {[r scard myset] == 18}
            set union [concat [r smembers myset] $res]
            assert {[lsort $union] eq [lsort $content]}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(
    ["type", "content"],
    [
        ("intset", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"),
        ("listpack", "a 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"),
    ],
)
def spop_new_implementation_code_path_3_type(s: valkey.Valkey):
    """
    {
            create_set myset $content
            assert_encoding $type myset
            set res [r spop myset 18]
            assert {[llength $res] == 18}
            assert {[r scard myset] == 2}
            set union [concat [r smembers myset] $res]
            assert {[lsort $union] eq [lsort $content]}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def spop_new_implementation_code_path_1_propagate_as_del_or_unlink(s: valkey.Valkey):
    """
    {
            r del myset1{t} myset2{t}
            r sadd myset1{t} 1 2 3 4 5
            r sadd myset2{t} 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65

            set repl [attach_to_replication_stream]

            r config set lazyfree-lazy-server-del no
            r spop myset1{t} [r scard myset1{t}]
            r config set lazyfree-lazy-server-del yes
            r spop myset2{t} [r scard myset2{t}]
            assert_equal {0} [r exists myset1{t} myset2{t}]

            # Verify the propagate of DEL and UNLINK.
            assert_replication_stream $repl {
                {select *}
                {del myset1{t}}
                {unlink myset2{t}}
            }

            close_replication_stream $repl
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def srandmember_count_of_0_is_handled_correctly(s: valkey.Valkey):
    """
    {
            r srandmember myset 0
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def srandmember_with_count_against_non_existing_key(s: valkey.Valkey):
    """
    {
            r srandmember nonexisting_key 100
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def srandmember_count_overflow(s: valkey.Valkey):
    """
    {
            r sadd myset a
            assert_error {*value is out of range*} {r srandmember myset -9223372036854775808}
    }
    """
    assert False


"""
r readraw 1

 test TCLDoubleQuotedWord(value='SRANDMEMBER count of 0 is handled correctly - emptyarray') {
        r srandmember myset 0
} {*0}
"""


@pytest.mark.xfail(reason="not implemented")
def srandmember_with_count_against_non_existing_key_emptyarray(s: valkey.Valkey):
    """
    {
            r srandmember nonexisting_key 100
    }
    """
    assert False


"""
r readraw 0

 foreach {type contents} {
        listpack {
            1 5 10 50 125 50000 33959417 4775547 65434162
            12098459 427716 483706 2726473884 72615637475
            MARY PATRICIA LINDA BARBARA ELIZABETH JENNIFER MARIA
            SUSAN MARGARET DOROTHY LISA NANCY KAREN BETTY HELEN
            SANDRA DONNA CAROL RUTH SHARON MICHELLE LAURA SARAH
            KIMBERLY DEBORAH JESSICA SHIRLEY CYNTHIA ANGELA MELISSA
            BRENDA AMY ANNA REBECCA VIRGINIA KATHLEEN
        }
        intset {
            0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
            20 21 22 23 24 25 26 27 28 29
            30 31 32 33 34 35 36 37 38 39
            40 41 42 43 44 45 46 47 48 49
        }
        hashtable {
            ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789
            1 5 10 50 125 50000 33959417 4775547 65434162
            12098459 427716 483706 2726473884 72615637475
            MARY PATRICIA LINDA BARBARA ELIZABETH JENNIFER MARIA
            SUSAN MARGARET DOROTHY LISA NANCY KAREN BETTY HELEN
            SANDRA DONNA CAROL RUTH SHARON MICHELLE LAURA SARAH
            KIMBERLY DEBORAH JESSICA SHIRLEY CYNTHIA ANGELA MELISSA
            BRENDA AMY ANNA REBECCA VIRGINIA
        }
} {
        test "SRANDMEMBER with <count> - $type" {
            create_set myset $contents
            assert_encoding $type myset
            unset -nocomplain myset
            array set myset {}
            foreach ele [r smembers myset] {
                set myset($ele) 1
            }
            assert_equal [lsort $contents] [lsort [array names myset]]

            # Make sure that a count of 0 is handled correctly.
            assert_equal [r srandmember myset 0] {}

            # We'll stress different parts of the code, see the implementation
            # of SRANDMEMBER for more information, but basically there are
            # four different code paths.
            #
            # PATH 1: Use negative count.
            #
            # 1) Check that it returns repeated elements.
            set res [r srandmember myset -100]
            assert_equal [llength $res] 100

            # 2) Check that all the elements actually belong to the
            # original set.
            foreach ele $res {
                assert {[info exists myset($ele)]}
            }

            # 3) Check that eventually all the elements are returned.
            unset -nocomplain auxset
            set iterations 1000
            while {$iterations != 0} {
                incr iterations -1
                set res [r srandmember myset -10]
                foreach ele $res {
                    set auxset($ele) 1
                }
                if {[lsort [array names myset]] eq
                    [lsort [array names auxset]]} {
                    break;
                }
            }
            assert {$iterations != 0}

            # PATH 2: positive count (unique behavior) with requested size
            # equal or greater than set size.
            foreach size {50 100} {
                set res [r srandmember myset $size]
                assert_equal [llength $res] 50
                assert_equal [lsort $res] [lsort [array names myset]]
            }

            # PATH 3: Ask almost as elements as there are in the set.
            # In this case the implementation will duplicate the original
            # set and will remove random elements up to the requested size.
            #
            # PATH 4: Ask a number of elements definitely smaller than
            # the set size.
            #
            # We can test both the code paths just changing the size but
            # using the same code.

            foreach size {45 5} {
                set res [r srandmember myset $size]
                assert_equal [llength $res] $size

                # 1) Check that all the elements actually belong to the
                # original set.
                foreach ele $res {
                    assert {[info exists myset($ele)]}
                }

                # 2) Check that eventually all the elements are returned.
                unset -nocomplain auxset
                set iterations 1000
                while {$iterations != 0} {
                    incr iterations -1
                    set res [r srandmember myset $size]
                    foreach ele $res {
                        set auxset($ele) 1
                    }
                    if {[lsort [array names myset]] eq
                        [lsort [array names auxset]]} {
                        break;
                    }
                }
                assert {$iterations != 0}
            }
        }
}
"""


@pytest.mark.xfail(reason="not implemented")
@pytest.mark.parametrize(
    ["type", "contents"],
    [
        (
            "listpack",
            """            1 5 10 50 125
            MARY PATRICIA LINDA BARBARA ELIZABETH
        """,
        ),
        (
            "intset",
            """            0 1 2 3 4 5 6 7 8 9
        """,
        ),
        (
            "hashtable",
            """            ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789
            1 5 10 50 125
            MARY PATRICIA LINDA BARBARA
        """,
        ),
    ],
)
def srandmember_histogram_distribution_type(s: valkey.Valkey):
    """
    {
                create_set myset $contents
                assert_encoding $type myset
                unset -nocomplain myset
                array set myset {}
                foreach ele [r smembers myset] {
                    set myset($ele) 1
                }

                # Use negative count (PATH 1).
                # df = 9, 40 means 0.00001 probability
                set res [r srandmember myset -1000]
                assert_lessthan [chi_square_value $res] 40

                # Use positive count (both PATH 3 and PATH 4).
                foreach size {8 2} {
                    unset -nocomplain allkey
                    set iterations [expr {1000 / $size}]
                    while {$iterations != 0} {
                        incr iterations -1
                        set res [r srandmember myset $size]
                        foreach ele $res {
                            lappend allkey $ele
                        }
                    }
                    # df = 9, 40 means 0.00001 probability
                    assert_lessthan [chi_square_value $allkey] 40
                }
    }
    """
    assert False


"""
proc is_rehashing {myset} {
        set htstats [r debug HTSTATS-KEY $myset]
        return [string match {*rehashing target*} $htstats]
}
"""


"""
proc rem_hash_set_top_N {myset n} {
        set cursor 0
        set members {}
        set enough 0
        while 1 {
            set res [r sscan $myset $cursor]
            set cursor [lindex $res 0]
            set k [lindex $res 1]
            foreach m $k {
                lappend members $m
                if {[llength $members] >= $n} {
                    set enough 1
                    break
                }
            }
            if {$enough || $cursor == 0} {
                break
            }
        }
        r srem $myset {*}$members
}
"""


"""
proc verify_rehashing_completed_key {myset table_size keys} {
        set htstats [r debug HTSTATS-KEY $myset]
        assert {![string match {*rehashing target*} $htstats]}
        return {[string match {*table size: $table_size*number of elements: $keys*} $htstats]}
}
"""


@pytest.mark.xfail(reason="not implemented")
def srandmember_with_a_dict_containing_long_chain(s: valkey.Valkey):
    """
    {
            set origin_save [config_get_set save ""]
            set origin_max_lp [config_get_set set-max-listpack-entries 0]
            set origin_save_delay [config_get_set rdb-key-save-delay 2147483647]

            # 1) Create a hash set with 100000 members.
            set members {}
            for {set i 0} {$i < 100000} {incr i} {
                lappend members [format "m:%d" $i]
            }
            create_set myset $members

            # 2) Wait for the hash set rehashing to finish.
            while {[is_rehashing myset]} {
                r srandmember myset 100
            }

            # 3) Turn off the rehashing of this set, and remove the members to 500.
            r bgsave
            rem_hash_set_top_N myset [expr {[r scard myset] - 500}]
            assert_equal [r scard myset] 500

            # 4) Kill RDB child process to restart rehashing.
            set pid1 [get_child_pid 0]
            catch {exec kill -9 $pid1}
            waitForBgsave r

            # 5) Let the set hash to start rehashing
            r spop myset 1
            assert [is_rehashing myset]

            # 6) Verify that when rdb saving is in progress, rehashing will still be performed (because
            # the ratio is extreme) by waiting for it to finish during an active bgsave.
            r bgsave

            while {[is_rehashing myset]} {
                r srandmember myset 1
            }
            if {$::verbose} {
                puts [r debug HTSTATS-KEY myset full]
            }

            set pid1 [get_child_pid 0]
            catch {exec kill -9 $pid1}
            waitForBgsave r

            # 7) Check that eventually, SRANDMEMBER returns all elements.
            array set allmyset {}
            foreach ele [r smembers myset] {
                set allmyset($ele) 1
            }
            unset -nocomplain auxset
            set iterations 1000
            while {$iterations != 0} {
                incr iterations -1
                set res [r srandmember myset -10]
                foreach ele $res {
                    set auxset($ele) 1
                }
                if {[lsort [array names allmyset]] eq
                    [lsort [array names auxset]]} {
                    break;
                }
            }
            assert {$iterations != 0}

            # 8) Remove the members to 30 in order to calculate the value of Chi-Square Distribution,
            #    otherwise we would need more iterations.
            rem_hash_set_top_N myset [expr {[r scard myset] - 30}]
            assert_equal [r scard myset] 30

            # Hash set rehashing would be completed while removing members from the `myset`
            # We also check the size and members in the hash table.
            verify_rehashing_completed_key myset 64 30

            # Now that we have a hash set with only one long chain bucket.
            set htstats [r debug HTSTATS-KEY myset full]
            assert {[regexp {different slots: ([0-9]+)} $htstats - different_slots]}
            assert {[regexp {max chain length: ([0-9]+)} $htstats - max_chain_length]}
            assert {$different_slots == 1 && $max_chain_length == 30}

            # 9) Use positive count (PATH 4) to get 10 elements (out of 30) each time.
            unset -nocomplain allkey
            set iterations 1000
            while {$iterations != 0} {
                incr iterations -1
                set res [r srandmember myset 10]
                foreach ele $res {
                    lappend allkey $ele
                }
            }
            # validate even distribution of random sampling (df = 29, 73 means 0.00001 probability)
            assert_lessthan [chi_square_value $allkey] 73

            r config set save $origin_save
            r config set set-max-listpack-entries $origin_max_lp
            r config set rdb-key-save-delay $origin_save_delay
    }
    """
    assert False


"""
proc setup_move {} {
        r del myset3{t} myset4{t}
        create_set myset1{t} {1 a b}
        create_set myset2{t} {2 3 4}
        assert_encoding listpack myset1{t}
        assert_encoding intset myset2{t}
}
"""


@pytest.mark.xfail(reason="not implemented")
def smove_basics_from_regular_set_to_intset(s: valkey.Valkey):
    """
    {
            # move a non-integer element to an intset should convert encoding
            setup_move
            assert_equal 1 [r smove myset1{t} myset2{t} a]
            assert_equal {1 b} [lsort [r smembers myset1{t}]]
            assert_equal {2 3 4 a} [lsort [r smembers myset2{t}]]
            assert_encoding listpack myset2{t}

            # move an integer element should not convert the encoding
            setup_move
            assert_equal 1 [r smove myset1{t} myset2{t} 1]
            assert_equal {a b} [lsort [r smembers myset1{t}]]
            assert_equal {1 2 3 4} [lsort [r smembers myset2{t}]]
            assert_encoding intset myset2{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_basics_from_intset_to_regular_set(s: valkey.Valkey):
    """
    {
            setup_move
            assert_equal 1 [r smove myset2{t} myset1{t} 2]
            assert_equal {1 2 a b} [lsort [r smembers myset1{t}]]
            assert_equal {3 4} [lsort [r smembers myset2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_non_existing_key(s: valkey.Valkey):
    """
    {
            setup_move
            assert_equal 0 [r smove myset1{t} myset2{t} foo]
            assert_equal 0 [r smove myset1{t} myset1{t} foo]
            assert_equal {1 a b} [lsort [r smembers myset1{t}]]
            assert_equal {2 3 4} [lsort [r smembers myset2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_non_existing_src_set(s: valkey.Valkey):
    """
    {
            setup_move
            assert_equal 0 [r smove noset{t} myset2{t} foo]
            assert_equal {2 3 4} [lsort [r smembers myset2{t}]]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_from_regular_set_to_non_existing_destination_set(s: valkey.Valkey):
    """
    {
            setup_move
            assert_equal 1 [r smove myset1{t} myset3{t} a]
            assert_equal {1 b} [lsort [r smembers myset1{t}]]
            assert_equal {a} [lsort [r smembers myset3{t}]]
            assert_encoding listpack myset3{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_from_intset_to_non_existing_destination_set(s: valkey.Valkey):
    """
    {
            setup_move
            assert_equal 1 [r smove myset2{t} myset3{t} 2]
            assert_equal {3 4} [lsort [r smembers myset2{t}]]
            assert_equal {2} [lsort [r smembers myset3{t}]]
            assert_encoding intset myset3{t}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_wrong_src_key_type(s: valkey.Valkey):
    """
    {
            r set x{t} 10
            assert_error "WRONGTYPE*" {r smove x{t} myset2{t} foo}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_wrong_dst_key_type(s: valkey.Valkey):
    """
    {
            r set x{t} 10
            assert_error "WRONGTYPE*" {r smove myset2{t} x{t} foo}
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_with_identical_source_and_destination(s: valkey.Valkey):
    """
    {
            r del set{t}
            r sadd set{t} a b c
            r smove set{t} set{t} b
            lsort [r smembers set{t}]
    }
    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def smove_only_notify_dstset_when_the_addition_is_successful(s: valkey.Valkey):
    """
    {
            r del srcset{t}
            r del dstset{t}

            r sadd srcset{t} a b
            r sadd dstset{t} a

            r watch dstset{t}

            r multi
            r sadd dstset{t} c

            set r2 [valkey_client]
            $r2 smove srcset{t} dstset{t} a

            # The dstset is actually unchanged, multi should success
            r exec
            set res [r scard dstset{t}]
            assert_equal $res 2
            $r2 close
    }
    """
    assert False


"""
tags {slow} {
        test {intsets implementation stress testing} {
            for {set j 0} {$j < 20} {incr j} {
                unset -nocomplain s
                array set s {}
                r del s
                set len [randomInt 1024]
                for {set i 0} {$i < $len} {incr i} {
                    randpath {
                        set data [randomInt 65536]
                    } {
                        set data [randomInt 4294967296]
                    } {
                        set data [randomInt 18446744073709551616]
                    }
                    set s($data) {}
                    r sadd s $data
                }
                assert_equal [lsort [r smembers s]] [lsort [array names s]]
                set len [array size s]
                for {set i 0} {$i < $len} {incr i} {
                    set e [r spop s]
                    if {![info exists s($e)]} {
                        puts "Can't find '$e' on local array"
                        puts "Local array: [lsort [r smembers s]]"
                        puts "Remote array: [lsort [array names s]]"
                        error "exception"
                    }
                    array unset s $e
                }
                assert_equal [r scard s] 0
                assert_equal [array size s] 0
            }
        }
}
"""
