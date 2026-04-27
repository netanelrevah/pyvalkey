import pytest

from tests.valkey_test_client import ValkeyTestClient

pytestmark = pytest.mark.geo


def test_geoadd_and_geopos(r: ValkeyTestClient):
    assert (
        r.run(
            "geoadd",
            "Sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        )
        == 2
    )
    result = r.run("geopos", "Sicily", "Palermo", "NonExisting", "Catania")
    assert result[1] is None
    assert abs(float(result[0][0]) - 13.361389) < 1e-4
    assert abs(float(result[0][1]) - 38.115556) < 1e-4
    assert abs(float(result[2][0]) - 15.087269) < 1e-4
    assert abs(float(result[2][1]) - 37.502669) < 1e-4


def test_geohash_known_strings(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    result = r.run("geohash", "Sicily", "Palermo", "Catania")
    assert result == [b"sqc8b49rny0", b"sqdtr74hyu0"]


def test_geodist_between_members(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    d_m = float(r.run("geodist", "Sicily", "Palermo", "Catania"))
    d_km = float(r.run("geodist", "Sicily", "Palermo", "Catania", "km"))
    assert 166000 < d_m < 167000
    assert abs(d_km - d_m / 1000) < 0.01


def test_geodist_missing_member(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    assert r.run("geodist", "Sicily", "Palermo", "Missing") is None


def test_geosearch_byradius_fromlonlat(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    result = r.run("geosearch", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
    assert result == [b"Catania", b"Palermo"]


def test_geosearch_frommember(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    result = r.run("geosearch", "Sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC")
    assert result == [b"Palermo", b"Catania"]


def test_geosearch_withcoord_withdist(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    rows = r.run(
        "geosearch",
        "Sicily",
        "FROMLONLAT",
        "15",
        "37",
        "BYRADIUS",
        "200",
        "km",
        "ASC",
        "WITHCOORD",
        "WITHDIST",
    )
    assert len(rows) == 2
    assert rows[0][0] == b"Catania"
    assert float(rows[0][1]) < float(rows[1][1])


def test_geosearch_bybox(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    r.run("geoadd", "Sicily", "2.349014", "48.864716", "Paris")
    result = r.run("geosearch", "Sicily", "FROMLONLAT", "14", "37.5", "BYBOX", "500", "500", "km", "ASC")
    assert set(result) == {b"Catania", b"Palermo"}


def test_geoadd_nx_and_xx(r: ValkeyTestClient):
    r.run("geoadd", "k", "1", "2", "a")
    assert r.run("geoadd", "k", "NX", "3", "4", "a") == 0
    pos = r.run("geopos", "k", "a")[0]
    assert abs(float(pos[0]) - 1) < 1e-4
    assert r.run("geoadd", "k", "XX", "5", "6", "b") == 0
    assert r.run("geopos", "k", "b") == [None]


def test_geosearchstore(r: ValkeyTestClient):
    r.run("geoadd", "Sicily", "13.361389", "38.115556", "Palermo")
    r.run("geoadd", "Sicily", "15.087269", "37.502669", "Catania")
    n = r.run(
        "geosearchstore",
        "Dest",
        "Sicily",
        "FROMLONLAT",
        "15",
        "37",
        "BYRADIUS",
        "200",
        "km",
        "ASC",
    )
    assert n == 2
    assert r.run("zcard", "Dest") == 2
