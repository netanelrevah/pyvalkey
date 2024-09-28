import pytest
import valkey

pytestmark = [pytest.mark.incr]


@pytest.mark.xfail(reason="not implemented")
def incr_against_non_existing_key(s: valkey.Valkey):
    """
    set res {}
    append res [r incr novar]
    append res [r get novar]

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_against_key_created_by_incr_itself(s: valkey.Valkey):
    """
    r incr novar

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def decr_against_key_created_by_incr(s: valkey.Valkey):
    """
    r decr novar

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def decr_against_key_is_not_exist_and_incr(s: valkey.Valkey):
    """
    r del novar_not_exist
    assert_equal {-1} [r decr novar_not_exist]
    assert_equal {0} [r incr novar_not_exist]

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_against_key_originally_set_with_set(s: valkey.Valkey):
    """
    r set novar 100
    r incr novar

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_over_32bit_value(s: valkey.Valkey):
    """
    r set novar 17179869184
    r incr novar

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrby_over_32bit_value_with_over_32bit_increment(s: valkey.Valkey):
    """
    r set novar 17179869184
    r incrby novar 17179869184

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_fails_against_key_with_spaces_left(s: valkey.Valkey):
    """
    r set novar "    11"
    catch {r incr novar} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_fails_against_key_with_spaces_right(s: valkey.Valkey):
    """
    r set novar "11    "
    catch {r incr novar} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_fails_against_key_with_spaces_both(s: valkey.Valkey):
    """
    r set novar "    11    "
    catch {r incr novar} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def decrby_negation_overflow(s: valkey.Valkey):
    """
    r set x 0
    catch {r decrby x -9223372036854775808} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_fails_against_a_key_holding_a_list(s: valkey.Valkey):
    """
    r rpush mylist 1
    catch {r incr mylist} err
    r rpop mylist
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def decrby_over_32bit_value_with_over_32bit_increment_negative_res(s: valkey.Valkey):
    """
    r set novar 17179869184
    r decrby novar 17179869185

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def decrby_against_key_is_not_exist(s: valkey.Valkey):
    """
    r del key_not_exist
    assert_equal {-1} [r decrby key_not_exist 1]

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_uses_shared_objects_in_the_0_9999_range(s: valkey.Valkey):
    """
    r set foo -1
    r incr foo
    assert_refcount_morethan foo 1
    r set foo 9998
    r incr foo
    assert_refcount_morethan foo 1
    r incr foo
    assert_refcount 1 foo

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incr_can_modify_objects_in_place(s: valkey.Valkey):
    """
    r set foo 20000
    r incr foo
    assert_refcount 1 foo
    set old [lindex [split [r debug object foo]] 1]
    r incr foo
    set new [lindex [split [r debug object foo]] 1]
    assert {[string range $old 0 2] eq "at:"}
    assert {[string range $new 0 2] eq "at:"}
    assert {$old eq $new}

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_against_non_existing_key(s: valkey.Valkey):
    """
    r del novar
    list    [roundFloat [r incrbyfloat novar 1]] \
            [roundFloat [r get novar]] \
            [roundFloat [r incrbyfloat novar 0.25]] \
            [roundFloat [r get novar]]

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_against_key_originally_set_with_set(s: valkey.Valkey):
    """
    r set novar 1.5
    roundFloat [r incrbyfloat novar 1.5]

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_over_32bit_value(s: valkey.Valkey):
    """
    r set novar 17179869184
    r incrbyfloat novar 1.5

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_over_32bit_value_with_over_32bit_increment(s: valkey.Valkey):
    """
    r set novar 17179869184
    r incrbyfloat novar 17179869184

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_fails_against_key_with_spaces_left(s: valkey.Valkey):
    """
    set err {}
    r set novar "    11"
    catch {r incrbyfloat novar 1.0} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_fails_against_key_with_spaces_right(s: valkey.Valkey):
    """
    set err {}
    r set novar "11    "
    catch {r incrbyfloat novar 1.0} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_fails_against_key_with_spaces_both(s: valkey.Valkey):
    """
    set err {}
    r set novar " 11 "
    catch {r incrbyfloat novar 1.0} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_fails_against_a_key_holding_a_list(s: valkey.Valkey):
    """
    r del mylist
    set err {}
    r rpush mylist 1
    catch {r incrbyfloat mylist 1.0} err
    r del mylist
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrbyfloat_decrement(s: valkey.Valkey):
    """
    r set foo 1
    roundFloat [r incrbyfloat foo -1.1]

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def string_to_double_with_null_terminator(s: valkey.Valkey):
    """
    r set foo 1
    r setrange foo 2 2
    catch {r incrbyfloat foo 1} err
    format $err

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def no_negative_zero(s: valkey.Valkey):
    """
    r del foo
    r incrbyfloat foo [expr double(1)/41]
    r incrbyfloat foo [expr double(-1)/41]
    r get foo

    """
    assert False


@pytest.mark.xfail(reason="not implemented")
def incrby_incrbyfloat_decrby_against_unhappy_path(s: valkey.Valkey):
    """
    r del mykeyincr
    assert_error "*ERR wrong number of arguments*" {r incr mykeyincr v}
    assert_error "*ERR wrong number of arguments*" {r decr mykeyincr v}
    assert_error "*value is not an integer or out of range*" {r incrby mykeyincr v}
    assert_error "*value is not an integer or out of range*" {r incrby mykeyincr 1.5}
    assert_error "*value is not an integer or out of range*" {r decrby mykeyincr v}
    assert_error "*value is not an integer or out of range*" {r decrby mykeyincr 1.5}
    assert_error "*value is not a valid float*" {r incrbyfloat mykeyincr v}

    """
    assert False
