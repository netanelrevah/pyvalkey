import time

import pytest

from tests.valkey_test_client import ValkeyTestClient

pytestmark = pytest.mark.hashexpire


def test_hexpire_sets_ttl_and_httl_returns_it(r: ValkeyTestClient):
    r.run("hset", "h", "f1", "v1", "f2", "v2")
    assert r.run("hexpire", "h", "100", "FIELDS", "2", "f1", "f2") == [1, 1]
    ttl = r.run("httl", "h", "FIELDS", "2", "f1", "f2")
    assert ttl[0] > 0 and ttl[1] > 0


def test_httl_no_field_no_ttl(r: ValkeyTestClient):
    r.run("hset", "h", "f1", "v1")
    assert r.run("httl", "h", "FIELDS", "2", "f1", "missing") == [-1, -2]


def test_httl_missing_key(r: ValkeyTestClient):
    assert r.run("httl", "missing_key_httl", "FIELDS", "1", "f1") == [-2]


def test_hpexpire_and_hpttl(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    assert r.run("hpexpire", "h", "50000", "FIELDS", "1", "f") == [1]
    assert r.run("hpttl", "h", "FIELDS", "1", "f")[0] > 0


def test_hexpireat_past_deletes(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    assert r.run("hexpireat", "h", "1", "FIELDS", "1", "f") == [2]
    assert r.run("hget", "h", "f") is None


def test_hpexpireat_sets_absolute(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    at = int(time.time() * 1000) + 60_000
    assert r.run("hpexpireat", "h", str(at), "FIELDS", "1", "f") == [1]
    assert r.run("hpexpiretime", "h", "FIELDS", "1", "f") == [at]
    assert r.run("hexpiretime", "h", "FIELDS", "1", "f") == [at // 1000]


def test_hpersist_clears_ttl(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    r.run("hexpire", "h", "100", "FIELDS", "1", "f")
    assert r.run("hpersist", "h", "FIELDS", "1", "f") == [1]
    assert r.run("httl", "h", "FIELDS", "1", "f") == [-1]
    # persist on field without TTL returns -1
    assert r.run("hpersist", "h", "FIELDS", "1", "f") == [-1]
    # persist on missing field returns -2
    assert r.run("hpersist", "h", "FIELDS", "1", "missing") == [-2]


def test_hexpire_nx_xx_gt_lt(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    # NX: ok because no TTL set
    assert r.run("hexpire", "h", "100", "NX", "FIELDS", "1", "f") == [1]
    # NX again: fails because TTL exists
    assert r.run("hexpire", "h", "200", "NX", "FIELDS", "1", "f") == [0]
    # XX: ok because TTL exists
    assert r.run("hexpire", "h", "50", "XX", "FIELDS", "1", "f") == [1]
    # GT: only if greater than current
    assert r.run("hexpire", "h", "10", "GT", "FIELDS", "1", "f") == [0]
    assert r.run("hexpire", "h", "500", "GT", "FIELDS", "1", "f") == [1]
    # LT: only if less than current
    assert r.run("hexpire", "h", "1000", "LT", "FIELDS", "1", "f") == [0]
    assert r.run("hexpire", "h", "5", "LT", "FIELDS", "1", "f") == [1]


def test_hset_clears_field_ttl(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    r.run("hexpire", "h", "100", "FIELDS", "1", "f")
    r.run("hset", "h", "f", "new")
    assert r.run("httl", "h", "FIELDS", "1", "f") == [-1]


def test_hdel_clears_field_ttl(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    r.run("hexpire", "h", "100", "FIELDS", "1", "f")
    r.run("hdel", "h", "f")
    assert r.run("httl", "h", "FIELDS", "1", "f") == [-2]


def test_hgetex_with_ex(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    assert r.run("hgetex", "h", "EX", "100", "FIELDS", "1", "f") == [b"v"]
    assert r.run("httl", "h", "FIELDS", "1", "f")[0] > 0


def test_hgetex_persist(r: ValkeyTestClient):
    r.run("hset", "h", "f", "v")
    r.run("hexpire", "h", "100", "FIELDS", "1", "f")
    assert r.run("hgetex", "h", "PERSIST", "FIELDS", "1", "f") == [b"v"]
    assert r.run("httl", "h", "FIELDS", "1", "f") == [-1]


def test_hgetex_missing_field(r: ValkeyTestClient):
    r.run("hset", "h", "f1", "v1")
    assert r.run("hgetex", "h", "EX", "100", "FIELDS", "2", "f1", "missing") == [b"v1", None]


def test_hgetdel_returns_and_deletes(r: ValkeyTestClient):
    r.run("hset", "h", "f1", "v1", "f2", "v2")
    assert r.run("hgetdel", "h", "FIELDS", "2", "f1", "missing") == [b"v1", None]
    assert r.run("hget", "h", "f1") is None
    assert r.run("hget", "h", "f2") == b"v2"


def test_hgetdel_drops_key_when_empty(r: ValkeyTestClient):
    r.run("hset", "hdrop", "f", "v")
    r.run("hgetdel", "hdrop", "FIELDS", "1", "f")
    assert r.run("exists", "hdrop") == 0


def test_hsetex_sets_fields_and_ttl(r: ValkeyTestClient):
    assert r.run("hsetex", "h", "EX", "100", "FIELDS", "2", "f1", "v1", "f2", "v2") == 1
    assert r.run("hget", "h", "f1") == b"v1"
    assert r.run("httl", "h", "FIELDS", "2", "f1", "f2") == [
        pytest.approx(100, abs=2),
        pytest.approx(100, abs=2),
    ]


def test_hsetex_fnx_fxx(r: ValkeyTestClient):
    r.run("hset", "h", "f1", "v1")
    # FNX: fails because f1 exists
    assert r.run("hsetex", "h", "FNX", "EX", "100", "FIELDS", "2", "f1", "x", "f2", "y") == 0
    # FXX: fails because f3 doesn't exist
    assert r.run("hsetex", "h", "FXX", "EX", "100", "FIELDS", "2", "f1", "a", "f3", "b") == 0
    # FXX on only existing field succeeds
    assert r.run("hsetex", "h", "FXX", "EX", "100", "FIELDS", "1", "f1", "a") == 1
    assert r.run("hget", "h", "f1") == b"a"


def test_field_lazy_expiry(r: ValkeyTestClient):
    r.run("del", "hlazy")
    r.run("hset", "hlazy", "f", "v")
    r.run("hpexpire", "hlazy", "50", "FIELDS", "1", "f")
    time.sleep(0.1)
    assert r.run("hget", "hlazy", "f") is None
    assert r.run("hlen", "hlazy") == 0
    assert r.run("exists", "hlazy") == 0
