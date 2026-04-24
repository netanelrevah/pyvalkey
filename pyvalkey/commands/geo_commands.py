from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.geohash import (
    GEO_LATITUDE_MAXIMUM,
    GEO_LATITUDE_MINIMUM,
    GEO_LONGITUDE_MAXIMUM,
    GEO_LONGITUDE_MINIMUM,
    UNITS_TO_METERS,
    GeoHashBits,
    align_52_bits,
    decode_to_lonlat,
    encode,
    estimate_steps_by_radius,
    geohash_string,
    haversine_distance,
    neighbors,
    score_range_from_hash,
)
from pyvalkey.enums import NotificationType
from pyvalkey.utils.dependencies import dependency

if TYPE_CHECKING:
    from pyvalkey.database_objects.databases import Database
    from pyvalkey.database_objects.scored_sorted_set import ScoredSortedSet
    from pyvalkey.resp import ValueType


class AddMode(Enum):
    ALL = b"ALL"
    UPDATE_ONLY = b"XX"
    INSERT_ONLY = b"NX"


def _encode_score(lon: float, lat: float) -> float:
    h = encode(lon, lat)
    if h is None:
        raise ServerError(f"ERR invalid longitude,latitude pair {lon:.6f},{lat:.6f}".encode())
    return float(align_52_bits(h))


def _unit_to_meters(unit: bytes) -> float:
    u = unit.lower()
    if u not in UNITS_TO_METERS:
        raise ServerError(b"ERR unsupported unit provided. please use M, KM, FT, MI")
    return UNITS_TO_METERS[u]


@command(
    b"geoadd",
    {b"geo", b"slow"},
    flags={b"denyoom", b"write"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class GeoAdd(Command):
    """
    summary: >-
      Adds one or more members to a geospatial index. The key is created if it doesn't exist.
    complexity: >-
      O(log(N)) for each item added, where N is the number of elements in the sorted set.
    since: 3.2.0
    function: geoaddCommand
    reply_schema:
      description: >-
        When used without optional arguments, the number of elements added to the sorted set (excluding score updates).
        If the CH option is specified, the number of elements that were changed (added or updated).
      type: integer
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        args = self.args
        i = 0
        add_mode = AddMode.ALL
        return_changed_elements = False
        while i < len(args) and args[i].upper() in (b"XX", b"NX", b"CH"):
            tok = args[i].upper()
            if tok == b"NX":
                if add_mode == AddMode.UPDATE_ONLY:
                    raise ServerError(b"ERR syntax error")
                add_mode = AddMode.INSERT_ONLY
            elif tok == b"XX":
                if add_mode == AddMode.INSERT_ONLY:
                    raise ServerError(b"ERR syntax error")
                add_mode = AddMode.UPDATE_ONLY
            elif tok == b"CH":
                return_changed_elements = True
            i += 1

        rest = args[i:]
        if not rest or len(rest) % 3 != 0:
            raise ServerError(b"ERR syntax error")

        triples: list[tuple[float, float, bytes]] = []
        for j in range(0, len(rest), 3):
            try:
                lon = float(rest[j])
                lat = float(rest[j + 1])
            except ValueError:
                raise ServerError(b"ERR value is not a valid float") from None
            triples.append((lon, lat, rest[j + 2]))

        value = self.database.sorted_set_database.get_value_or_create(self.key)
        length_before = len(value)
        changed_elements = 0
        for lon, lat, member in triples:
            if (
                lon < GEO_LONGITUDE_MINIMUM
                or lon > GEO_LONGITUDE_MAXIMUM
                or lat < GEO_LATITUDE_MINIMUM
                or lat > GEO_LATITUDE_MAXIMUM
            ):
                raise ServerError(f"ERR invalid longitude,latitude pair {lon:.6f},{lat:.6f}".encode())
            score = _encode_score(lon, lat)
            exists = member in value.members_scores
            if add_mode == AddMode.UPDATE_ONLY and not exists:
                continue
            if add_mode == AddMode.INSERT_ONLY and exists:
                continue
            current = value.members_scores.get(member)
            if current != score:
                changed_elements += 1
            value.add(score, member)

        if return_changed_elements:
            return changed_elements
        added = len(value) - length_before
        if added:
            self.database.notify(NotificationType.ZSET, b"geoadd", self.key)
        return added


@command(b"geopos", {b"geo", b"read", b"slow"}, flags={b"readonly"})
class GeoPos(Command):
    """
    summary: Returns the longitude and latitude of members from a geospatial index.
    complexity: O(1) for each member requested.
    since: 3.2.0
    function: geoposCommand
    reply_schema:
      description: >-
        An array where each element is a two elements array representing longitude and latitude (x,y) of each member
        name passed as argument to the command
      type: array
      items:
        oneOf:
        - description: Element does not exist
          type: 'null'
        - type: array
          minItems: 2
          maxItems: 2
          items:
          - description: Latitude (x)
            type: number
          - description: Longitude (y)
            type: number
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    members: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_none(self.key)
        result: list[ValueType] = []
        for member in self.members:
            if value is None or member not in value.members_scores:
                result.append(None)
                continue
            score = value.members_scores[member]
            lon, lat = decode_to_lonlat(GeoHashBits(bits=int(score), step=26))
            result.append([f"{lon:.17f}".encode(), f"{lat:.17f}".encode()])
        return result


@command(b"geodist", {b"geo", b"read", b"slow"}, flags={b"readonly"})
class GeoDist(Command):
    """
    summary: Returns the distance between two members of a geospatial index.
    complexity: O(1)
    since: 3.2.0
    function: geodistCommand
    reply_schema:
      oneOf:
      - description: One or both of elements are missing.
        type: 'null'
      - description: Distance as a double (represented as a string) in the specified units.
        type: string
        pattern: ^[0-9]*(.[0-9]*)?$
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    member1: bytes = positional_parameter()
    member2: bytes = positional_parameter()
    unit: bytes = positional_parameter(default=b"m")

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_none(self.key)
        if value is None:
            return None
        s1 = value.members_scores.get(self.member1)
        s2 = value.members_scores.get(self.member2)
        if s1 is None or s2 is None:
            return None
        to_m = _unit_to_meters(self.unit)
        lon1, lat1 = decode_to_lonlat(GeoHashBits(bits=int(s1), step=26))
        lon2, lat2 = decode_to_lonlat(GeoHashBits(bits=int(s2), step=26))
        d = haversine_distance(lon1, lat1, lon2, lat2) / to_m
        return f"{d:.4f}".encode()


@command(b"geohash", {b"geo", b"read", b"slow"}, flags={b"readonly"})
class GeoHashCommand(Command):
    """
    summary: Returns members from a geospatial index as geohash strings.
    complexity: O(1) for each member requested.
    since: 3.2.0
    function: geohashCommand
    reply_schema:
      description: >-
        An array where each element is the Geohash corresponding to each member name passed as argument to the command.
      type: array
      items:
        type: string
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    members: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_none(self.key)
        result: list[ValueType] = []
        for member in self.members:
            if value is None or member not in value.members_scores:
                result.append(None)
                continue
            result.append(geohash_string(value.members_scores[member]))
        return result


def _members_in_hash_range(value: ScoredSortedSet, h: GeoHashBits) -> list[tuple[bytes, float]]:
    if h.bits == 0 and h.step == 0:
        return []
    min_score, max_score = score_range_from_hash(h)
    out = []
    for member_tuple in value.members.irange((float(min_score), b""), (float(max_score) + 1, b""), (True, False)):
        score, member = member_tuple
        if min_score <= int(score) <= max_score:
            out.append((member, float(score)))
    return out


def _members_in_shape(
    value: ScoredSortedSet,
    center_lon: float,
    center_lat: float,
    radius_m: float | None,
    width_m: float | None,
    height_m: float | None,
) -> list[tuple[bytes, float, float, float]]:
    """Return list of (member, distance_m, lon, lat) inside the circular/box shape around center."""
    if radius_m is not None:
        step_radius = radius_m
    else:
        assert width_m is not None and height_m is not None
        step_radius = ((width_m / 2) ** 2 + (height_m / 2) ** 2) ** 0.5
    step = estimate_steps_by_radius(step_radius, center_lat)
    h = encode(center_lon, center_lat, step)
    if h is None:
        return []
    n = neighbors(h)
    areas = [h, n.north, n.south, n.east, n.west, n.north_east, n.north_west, n.south_east, n.south_west]

    seen: set[bytes] = set()
    results: list[tuple[bytes, float, float, float]] = []
    for area in areas:
        for member, score in _members_in_hash_range(value, area):
            if member in seen:
                continue
            seen.add(member)
            lon, lat = decode_to_lonlat(GeoHashBits(bits=int(score), step=26))
            if radius_m is not None:
                d = haversine_distance(center_lon, center_lat, lon, lat)
                if d <= radius_m:
                    results.append((member, d, lon, lat))
            else:
                assert width_m is not None and height_m is not None
                lat_d = haversine_distance(lon, lat, lon, center_lat)
                if lat_d > height_m / 2:
                    continue
                lon_d = haversine_distance(lon, lat, center_lon, lat)
                if lon_d > width_m / 2:
                    continue
                d = haversine_distance(center_lon, center_lat, lon, lat)
                results.append((member, d, lon, lat))
    return results


def _parse_geosearch_args(
    args: list[bytes],
    *,
    is_store: bool,
) -> dict:
    """Parse geosearch / geosearchstore tail args. Returns a dict of parsed options."""
    i = 0
    opts: dict = {
        "from_member": None,
        "from_lonlat": None,
        "by_radius": None,  # (radius_m, unit)
        "by_box": None,  # (width_m, height_m, unit)
        "sort": None,  # b"ASC" / b"DESC"
        "count": None,
        "count_any": False,
        "withcoord": False,
        "withdist": False,
        "withhash": False,
        "storedist": False,
    }
    while i < len(args):
        tok = args[i].upper()
        if tok == b"FROMMEMBER":
            if i + 1 >= len(args):
                raise ServerError(b"ERR syntax error")
            opts["from_member"] = args[i + 1]
            i += 2
        elif tok == b"FROMLONLAT":
            if i + 2 >= len(args):
                raise ServerError(b"ERR syntax error")
            opts["from_lonlat"] = (float(args[i + 1]), float(args[i + 2]))
            i += 3
        elif tok == b"BYRADIUS":
            if i + 2 >= len(args):
                raise ServerError(b"ERR syntax error")
            unit = args[i + 2]
            opts["by_radius"] = (float(args[i + 1]) * _unit_to_meters(unit), unit)
            i += 3
        elif tok == b"BYBOX":
            if i + 3 >= len(args):
                raise ServerError(b"ERR syntax error")
            unit = args[i + 3]
            m = _unit_to_meters(unit)
            opts["by_box"] = (float(args[i + 1]) * m, float(args[i + 2]) * m, unit)
            i += 4
        elif tok in (b"ASC", b"DESC"):
            opts["sort"] = tok
            i += 1
        elif tok == b"COUNT":
            if i + 1 >= len(args):
                raise ServerError(b"ERR syntax error")
            opts["count"] = int(args[i + 1])
            i += 2
            if i < len(args) and args[i].upper() == b"ANY":
                opts["count_any"] = True
                i += 1
        elif tok == b"WITHCOORD" and not is_store:
            opts["withcoord"] = True
            i += 1
        elif tok == b"WITHDIST" and not is_store:
            opts["withdist"] = True
            i += 1
        elif tok == b"WITHHASH" and not is_store:
            opts["withhash"] = True
            i += 1
        elif tok == b"STOREDIST" and is_store:
            opts["storedist"] = True
            i += 1
        elif tok == b"BYPOLYGON":
            raise ServerError(b"ERR GEOSEARCH BYPOLYGON must have at least 3 vertices")
        else:
            raise ServerError(b"ERR syntax error")

    if opts["count_any"] and opts["count"] is None:
        raise ServerError(b"ERR the ANY argument requires COUNT argument")
    if opts["from_member"] is not None and opts["from_lonlat"] is not None:
        raise ServerError(b"ERR syntax error")
    if opts["from_member"] is None and opts["from_lonlat"] is None:
        raise ServerError(b"ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH")
    if opts["by_radius"] is not None and opts["by_box"] is not None:
        raise ServerError(b"ERR syntax error")
    if opts["by_radius"] is None and opts["by_box"] is None:
        raise ServerError(b"ERR exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for GEOSEARCH")
    return opts


def _geosearch_execute(database: Database, source_key: bytes, opts: dict) -> list[tuple[bytes, float, float, float]]:
    value = database.sorted_set_database.get_value_or_none(source_key)
    if value is None:
        return []

    if opts["from_member"] is not None:
        score = value.members_scores.get(opts["from_member"])
        if score is None:
            raise ServerError(b"ERR member " + opts["from_member"] + b" does not exist")
        center_lon, center_lat = decode_to_lonlat(GeoHashBits(bits=int(score), step=26))
    else:
        center_lon, center_lat = opts["from_lonlat"]

    radius_m = opts["by_radius"][0] if opts["by_radius"] else None
    width_m = opts["by_box"][0] if opts["by_box"] else None
    height_m = opts["by_box"][1] if opts["by_box"] else None

    results = _members_in_shape(value, center_lon, center_lat, radius_m, width_m, height_m)

    if opts["sort"] == b"ASC":
        results.sort(key=lambda r: r[1])
    elif opts["sort"] == b"DESC":
        results.sort(key=lambda r: r[1], reverse=True)
    elif opts["count"] is not None and not opts["count_any"]:
        results.sort(key=lambda r: r[1])

    if opts["count"] is not None:
        results = results[: opts["count"]]
    return results


@command(b"geosearch", {b"geo", b"read", b"slow"}, flags={b"readonly"})
class GeoSearch(Command):
    """
    summary: >-
      Queries a geospatial index for members inside an area of a box, circle, or a polygon.
    complexity: >-
      O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided
      as the filter and M is the number of items inside the shape
    since: 6.2.0
    function: geosearchCommand
    reply_schema:
      description: Array of matched members information.
      anyOf:
      - description: If no WITH* option is specified, array of matched members names.
        type: array
        items:
          description: Name.
          type: string
      - type: array
        items:
          type: array
          minItems: 1
          maxItems: 4
          items:
          - description: Matched member name.
            type: string
          additionalItems:
            oneOf:
            - description: >-
                If WITHDIST option is specified, the distance from the center as a floating point number, in the
                same unit specified in the radius.
              type: string
            - description: If WITHHASH option is specified, the geohash integer.
              type: integer
            - description: >-
                If WITHCOORD option is specified, the coordinates as a two items x,y array (longitude,latitude).
              type: array
              minItems: 2
              maxItems: 2
              items:
              - description: Latitude (x).
                type: number
              - description: Longitude (y).
                type: number
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        opts = _parse_geosearch_args(self.args, is_store=False)
        results = _geosearch_execute(self.database, self.key, opts)
        unit_div = _unit_to_meters(opts["by_radius"][1]) if opts["by_radius"] else _unit_to_meters(opts["by_box"][2])

        output: list[ValueType] = []
        for member, dist_m, lon, lat in results:
            if not (opts["withcoord"] or opts["withdist"] or opts["withhash"]):
                output.append(member)
                continue
            row: list[ValueType] = [member]
            if opts["withdist"]:
                row.append(f"{dist_m / unit_div:.4f}".encode())
            if opts["withhash"]:
                h = encode(lon, lat)
                row.append(align_52_bits(h) if h else 0)
            if opts["withcoord"]:
                row.append([f"{lon:.17f}".encode(), f"{lat:.17f}".encode()])
            output.append(row)
        return output


@command(b"geosearchstore", {b"geo", b"slow"}, flags={b"denyoom", b"write"})
class GeoSearchStore(Command):
    """
    summary: >-
      Queries a geospatial index for members inside an area of a box, a circle, or a polygon, optionally stores
      the result.
    complexity: >-
      O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided
      as the filter and M is the number of items inside the shape
    since: 6.2.0
    function: geosearchstoreCommand
    reply_schema:
      description: The number of elements in the resulting set.
      type: integer
    """

    database: Database = dependency()

    destination: bytes = positional_parameter(key_mode=b"OW")
    source: bytes = positional_parameter(key_mode=b"R")
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        opts = _parse_geosearch_args(self.args, is_store=True)
        results = _geosearch_execute(self.database, self.source, opts)

        self.database.sorted_set_database.pop(self.destination, default=None)
        if not results:
            return 0
        unit_div = _unit_to_meters(opts["by_radius"][1]) if opts["by_radius"] else _unit_to_meters(opts["by_box"][2])
        dest = self.database.sorted_set_database.get_value_or_create(self.destination)
        for member, dist_m, lon, lat in results:
            if opts["storedist"]:
                dest.add(dist_m / unit_div, member)
            else:
                h = encode(lon, lat)
                dest.add(float(align_52_bits(h)) if h else 0.0, member)
        return len(results)


def _parse_georadius_args(
    args: list[bytes],
    *,
    is_store: bool,
) -> tuple[dict, bytes | None, bool]:
    """Parse georadius/georadiusbymember-shaped tails. Returns (opts, store_key, store_dist)."""
    i = 0
    opts: dict = {
        "from_member": None,
        "from_lonlat": None,
        "by_radius": None,
        "by_box": None,
        "sort": None,
        "count": None,
        "count_any": False,
        "withcoord": False,
        "withdist": False,
        "withhash": False,
        "storedist": False,
    }
    store_key: bytes | None = None
    store_dist = False
    while i < len(args):
        tok = args[i].upper()
        if tok in (b"ASC", b"DESC"):
            opts["sort"] = tok
            i += 1
        elif tok == b"COUNT":
            if i + 1 >= len(args):
                raise ServerError(b"ERR syntax error")
            opts["count"] = int(args[i + 1])
            i += 2
            if i < len(args) and args[i].upper() == b"ANY":
                opts["count_any"] = True
                i += 1
        elif tok == b"WITHCOORD":
            opts["withcoord"] = True
            i += 1
        elif tok == b"WITHDIST":
            opts["withdist"] = True
            i += 1
        elif tok == b"WITHHASH":
            opts["withhash"] = True
            i += 1
        elif tok == b"ANY":
            raise ServerError(b"ERR the ANY argument requires COUNT argument")
        elif tok == b"STORE" and is_store:
            if i + 1 >= len(args):
                raise ServerError(b"ERR syntax error")
            store_key = args[i + 1]
            store_dist = False
            i += 2
        elif tok == b"STOREDIST" and is_store:
            if i + 1 >= len(args):
                raise ServerError(b"ERR syntax error")
            store_key = args[i + 1]
            store_dist = True
            i += 2
        else:
            raise ServerError(b"ERR syntax error")
    if store_key is not None and (opts["withcoord"] or opts["withdist"] or opts["withhash"]):
        raise ServerError(
            b"ERR STORE option in GEORADIUS is not compatible with WITHCOORD, WITHDIST and WITHHASH options"
        )
    if opts["count_any"] and opts["count"] is None:
        raise ServerError(b"ERR the ANY argument requires COUNT argument")
    return opts, store_key, store_dist


def _format_geo_output(
    results: list[tuple[bytes, float, float, float]], opts: dict, unit_div: float
) -> list[ValueType]:
    output: list[ValueType] = []
    for member, dist_m, lon, lat in results:
        if not (opts["withcoord"] or opts["withdist"] or opts["withhash"]):
            output.append(member)
            continue
        row: list[ValueType] = [member]
        if opts["withdist"]:
            row.append(f"{dist_m / unit_div:.4f}".encode())
        if opts["withhash"]:
            h = encode(lon, lat)
            row.append(align_52_bits(h) if h else 0)
        if opts["withcoord"]:
            row.append([f"{lon:.17f}".encode(), f"{lat:.17f}".encode()])
        output.append(row)
    return output


def _run_georadius_common(
    database: Database,
    key: bytes,
    center_lon: float,
    center_lat: float,
    radius: float,
    unit: bytes,
    tail_args: list[bytes],
    is_store: bool,
) -> ValueType:
    opts, store_key, store_dist = _parse_georadius_args(tail_args, is_store=is_store)
    opts["from_lonlat"] = (center_lon, center_lat)
    opts["by_radius"] = (radius * _unit_to_meters(unit), unit)
    results = _geosearch_execute(database, key, opts)

    if store_key is not None:
        database.sorted_set_database.pop(store_key, default=None)
        if not results:
            return 0
        unit_div = _unit_to_meters(unit)
        dest = database.sorted_set_database.get_value_or_create(store_key)
        for member, dist_m, lon, lat in results:
            if store_dist:
                dest.add(dist_m / unit_div, member)
            else:
                h = encode(lon, lat)
                dest.add(float(align_52_bits(h)) if h else 0.0, member)
        return len(results)

    unit_div = _unit_to_meters(unit)
    return _format_geo_output(results, opts, unit_div)


@command(b"georadius", {b"geo", b"slow"}, flags={b"denyoom", b"write"})
class GeoRadius(Command):
    """
    summary: >-
      Queries a geospatial index for members within a distance from a coordinate, optionally stores the result.
    complexity: >-
      O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center
      and radius and M is the number of items inside the index.
    since: 3.2.0
    function: georadiusCommand
    reply_schema:
      description: Array of matched members information.
      anyOf:
      - description: If no WITH* option is specified, array of matched members names.
        type: array
        items:
          description: Name.
          type: string
      - type: array
        items:
          type: array
          minItems: 1
          maxItems: 4
          items:
          - description: Matched member name.
            type: string
          additionalItems:
            oneOf:
            - description: >-
                If WITHDIST option is specified, the distance from the center as a floating point number, in the
                same unit specified in the radius.
              type: string
            - description: If WITHHASH option is specified, the geohash integer.
              type: integer
            - description: >-
                If WITHCOORD option is specified, the coordinates as a two items x,y array (longitude,latitude).
              type: array
              minItems: 2
              maxItems: 2
              items:
              - description: Latitude (x).
                type: number
              - description: Longitude (y).
                type: number
      - description: Number of items stored in key.
        type: integer
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    longitude: float = positional_parameter()
    latitude: float = positional_parameter()
    radius: float = positional_parameter()
    unit: bytes = positional_parameter()
    extra: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return _run_georadius_common(
            self.database, self.key, self.longitude, self.latitude, self.radius, self.unit, self.extra, True
        )


@command(b"georadius_ro", {b"geo", b"read", b"slow"}, flags={b"readonly"})
class GeoRadiusReadOnly(Command):
    """
    summary: >-
      Returns members from a geospatial index that are within a distance from a coordinate.
    complexity: >-
      O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center
      and radius and M is the number of items inside the index.
    since: 3.2.10
    function: georadiusroCommand
    reply_schema:
      description: Array of matched members information.
      anyOf:
      - description: If no WITH* option is specified, array of matched members names.
        type: array
        items:
          description: Name.
          type: string
      - type: array
        items:
          type: array
          minItems: 1
          maxItems: 4
          items:
          - description: Matched member name.
            type: string
          additionalItems:
            oneOf:
            - description: >-
                If WITHDIST option is specified, the distance from the center as a floating point number, in the
                same unit specified in the radius.
              type: string
            - description: If WITHHASH option is specified, the geohash integer.
              type: integer
            - description: >-
                If WITHCOORD option is specified, the coordinates as a two items x,y array (longitude,latitude).
              type: array
              minItems: 2
              maxItems: 2
              items:
              - description: Latitude (x).
                type: number
              - description: Longitude (y).
                type: number
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    longitude: float = positional_parameter()
    latitude: float = positional_parameter()
    radius: float = positional_parameter()
    unit: bytes = positional_parameter()
    extra: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return _run_georadius_common(
            self.database, self.key, self.longitude, self.latitude, self.radius, self.unit, self.extra, False
        )


@command(b"georadiusbymember", {b"geo", b"slow"}, flags={b"denyoom", b"write"})
class GeoRadiusByMember(Command):
    """
    summary: >-
      Queries a geospatial index for members within a distance from a member, optionally stores the result.
    complexity: >-
      O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center
      and radius and M is the number of items inside the index.
    since: 3.2.0
    function: georadiusbymemberCommand
    reply_schema:
      description: Array of matched members information.
      anyOf:
      - description: If no WITH* option is specified, array of matched members names.
        type: array
        items:
          description: Name
          type: string
      - type: array
        items:
          type: array
          minItems: 1
          maxItems: 4
          items:
          - description: Matched member name.
            type: string
          additionalItems:
            oneOf:
            - description: >-
                If WITHDIST option is specified, the distance from the center as a floating point number, in the
                same unit specified in the radius.
              type: string
            - description: If WITHHASH option is specified, the geohash integer.
              type: integer
            - description: >-
                If WITHCOORD option is specified, the coordinates as a two items x,y array (longitude,latitude).
              type: array
              minItems: 2
              maxItems: 2
              items:
              - description: Latitude (x).
                type: number
              - description: Longitude (y).
                type: number
      - description: Number of items stored in key.
        type: integer
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    member: bytes = positional_parameter()
    radius: float = positional_parameter()
    unit: bytes = positional_parameter()
    extra: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_none(self.key)
        if value is None:
            return []
        if self.member not in value.members_scores:
            raise ServerError(b"ERR member " + self.member + b" does not exist")
        lon, lat = decode_to_lonlat(GeoHashBits(bits=int(value.members_scores[self.member]), step=26))
        return _run_georadius_common(self.database, self.key, lon, lat, self.radius, self.unit, self.extra, True)


@command(b"georadiusbymember_ro", {b"geo", b"read", b"slow"}, flags={b"readonly"})
class GeoRadiusByMemberReadOnly(Command):
    """
    summary: >-
      Returns members from a geospatial index that are within a distance from a member.
    complexity: >-
      O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center
      and radius and M is the number of items inside the index.
    since: 3.2.10
    function: georadiusbymemberroCommand
    reply_schema:
      description: Array of matched members information.
      anyOf:
      - description: If no WITH* option is specified, array of matched members names.
        type: array
        items:
          description: Name.
          type: string
      - type: array
        items:
          type: array
          minItems: 1
          maxItems: 4
          items:
          - description: Matched member name.
            type: string
          additionalItems:
            oneOf:
            - description: >-
                If WITHDIST option is specified, the distance from the center as a floating point number, in the
                same unit specified in the radius.
              type: string
            - description: If WITHHASH option is specified, the geohash integer.
              type: integer
            - description: >-
                If WITHCOORD option is specified, the coordinates as a two items x,y array (longitude,latitude).
              type: array
              minItems: 2
              maxItems: 2
              items:
              - description: Latitude (x).
                type: number
              - description: Longitude (y).
                type: number
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    member: bytes = positional_parameter()
    radius: float = positional_parameter()
    unit: bytes = positional_parameter()
    extra: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_none(self.key)
        if value is None:
            return []
        if self.member not in value.members_scores:
            raise ServerError(b"ERR member " + self.member + b" does not exist")
        lon, lat = decode_to_lonlat(GeoHashBits(bits=int(value.members_scores[self.member]), step=26))
        return _run_georadius_common(self.database, self.key, lon, lat, self.radius, self.unit, self.extra, False)
