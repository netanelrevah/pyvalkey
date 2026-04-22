from __future__ import annotations

import math
from dataclasses import dataclass

GEO_STEP_MAXIMUM = 26
GEO_LATITUDE_MINIMUM = -85.05112878
GEO_LATITUDE_MAXIMUM = 85.05112878
GEO_LONGITUDE_MINIMUM = -180.0
GEO_LONGITUDE_MAXIMUM = 180.0

EARTH_RADIUS_IN_METERS = 6372797.560856
MERCATOR_MAXIMUM = 20037726.37

_MAXIMUM_STEP = 32
_POLAR_LATITUDE_STEP_1 = 66
_POLAR_LATITUDE_STEP_2 = 80
_GEOHASH_STRING_LENGTH = 11
_GEO_EPSILON = 1e-15

_MASK_64 = 0xFFFFFFFFFFFFFFFF
_BX = 0xAAAAAAAAAAAAAAAA
_BY = 0x5555555555555555

UNITS_TO_METERS = {
    b"m": 1.0,
    b"km": 1000.0,
    b"mi": 1609.34,
    b"ft": 0.3048,
}


_INTERLEAVE_MASKS = (
    0x5555555555555555,
    0x3333333333333333,
    0x0F0F0F0F0F0F0F0F,
    0x00FF00FF00FF00FF,
    0x0000FFFF0000FFFF,
)
_INTERLEAVE_SHIFTS = (1, 2, 4, 8, 16)
_DEINTERLEAVE_MASKS = (*_INTERLEAVE_MASKS, 0x00000000FFFFFFFF)
_DEINTERLEAVE_SHIFTS = (0, 1, 2, 4, 8, 16)


def _interleave64(xlo: int, ylo: int) -> int:
    """
    Takes the low 32 bits of xlo (x31..x0) and ylo (y31..y0) and returns the
    64-bit value y31 x31 y30 x30 ... y0 x0 (x in even positions, y in odd).
    """
    x = xlo & 0xFFFFFFFF
    y = ylo & 0xFFFFFFFF
    for i in range(4, -1, -1):
        x = (x | (x << _INTERLEAVE_SHIFTS[i])) & _INTERLEAVE_MASKS[i]
        y = (y | (y << _INTERLEAVE_SHIFTS[i])) & _INTERLEAVE_MASKS[i]
    return (x | (y << 1)) & _MASK_64


def _deinterleave64(interleaved: int) -> int:
    """
    Inverse of _interleave64: takes a 64-bit value y31 x31 ... y0 x0 and returns
    a 64-bit value with y31..y0 in the high 32 bits and x31..x0 in the low 32.
    """
    x = interleaved & _MASK_64
    y = (interleaved >> 1) & _MASK_64
    for i in range(6):
        x = (x | (x >> _DEINTERLEAVE_SHIFTS[i])) & _DEINTERLEAVE_MASKS[i]
        y = (y | (y >> _DEINTERLEAVE_SHIFTS[i])) & _DEINTERLEAVE_MASKS[i]
    return (x | (y << 32)) & _MASK_64


@dataclass
class GeoHashBits:
    bits: int
    step: int


@dataclass
class GeoHashArea:
    hash: GeoHashBits
    latitude_minimum: float
    latitude_maximum: float
    longitude_minimum: float
    longitude_maximum: float


def encode(longitude: float, latitude: float, step: int = GEO_STEP_MAXIMUM) -> GeoHashBits | None:
    if step < 1 or step > _MAXIMUM_STEP:
        return None
    if (
        longitude > GEO_LONGITUDE_MAXIMUM
        or longitude < GEO_LONGITUDE_MINIMUM
        or latitude > GEO_LATITUDE_MAXIMUM
        or latitude < GEO_LATITUDE_MINIMUM
    ):
        return None
    lat_offset = (latitude - GEO_LATITUDE_MINIMUM) / (GEO_LATITUDE_MAXIMUM - GEO_LATITUDE_MINIMUM)
    lon_offset = (longitude - GEO_LONGITUDE_MINIMUM) / (GEO_LONGITUDE_MAXIMUM - GEO_LONGITUDE_MINIMUM)
    lat_offset = int(lat_offset * (1 << step))
    lon_offset = int(lon_offset * (1 << step))
    return GeoHashBits(bits=_interleave64(lat_offset, lon_offset), step=step)


def decode(hash_: GeoHashBits) -> GeoHashArea:
    step = hash_.step
    hash_sep = _deinterleave64(hash_.bits)
    lat_scale = GEO_LATITUDE_MAXIMUM - GEO_LATITUDE_MINIMUM
    lon_scale = GEO_LONGITUDE_MAXIMUM - GEO_LONGITUDE_MINIMUM
    ilato = hash_sep & 0xFFFFFFFF
    ilono = (hash_sep >> 32) & 0xFFFFFFFF
    scale = 1 << step
    return GeoHashArea(
        hash=hash_,
        latitude_minimum=GEO_LATITUDE_MINIMUM + (ilato / scale) * lat_scale,
        latitude_maximum=GEO_LATITUDE_MINIMUM + ((ilato + 1) / scale) * lat_scale,
        longitude_minimum=GEO_LONGITUDE_MINIMUM + (ilono / scale) * lon_scale,
        longitude_maximum=GEO_LONGITUDE_MINIMUM + ((ilono + 1) / scale) * lon_scale,
    )


def decode_to_lonlat(hash_: GeoHashBits) -> tuple[float, float]:
    area = decode(hash_)
    longitude = (area.longitude_minimum + area.longitude_maximum) / 2
    latitude = (area.latitude_minimum + area.latitude_maximum) / 2
    longitude = max(GEO_LONGITUDE_MINIMUM, min(GEO_LONGITUDE_MAXIMUM, longitude))
    latitude = max(GEO_LATITUDE_MINIMUM, min(GEO_LATITUDE_MAXIMUM, latitude))
    return longitude, latitude


def align_52_bits(hash_: GeoHashBits) -> int:
    return (hash_.bits << (52 - hash_.step * 2)) & ((1 << 52) - 1)


def score_range_from_hash(hash_: GeoHashBits) -> tuple[int, int]:
    """Return [min, max] inclusive 52-bit score range covered by this hash."""
    if hash_.bits == 0 and hash_.step == 0:
        return 0, 0
    shift = 52 - hash_.step * 2
    min_score = hash_.bits << shift
    max_score = ((hash_.bits + 1) << shift) - 1
    return min_score, max_score


def _move_x(hash_: GeoHashBits, d: int) -> GeoHashBits:
    """
    Return the neighbor cell one step east (d=+1) or west (d=-1) of hash_,
    at this hash's resolution (cell width = 1/2^hash_.step of the longitude range).
    """
    if d == 0:
        return GeoHashBits(hash_.bits, hash_.step)
    bits = hash_.bits
    x = bits & _BX
    y = bits & _BY
    shift = 64 - hash_.step * 2
    zz = (_BY >> shift) & _MASK_64
    if d > 0:
        x = (x + (zz + 1)) & _MASK_64
    else:
        x = (x | zz) & _MASK_64
        x = (x - (zz + 1)) & _MASK_64
    x &= (_BX >> shift) & _MASK_64
    return GeoHashBits(bits=x | y, step=hash_.step)


def _move_y(hash_: GeoHashBits, d: int) -> GeoHashBits:
    """
    Return the neighbor cell one step north (d=+1) or south (d=-1) of hash_,
    at this hash's resolution (cell height = 1/2^hash_.step of the latitude range).
    """
    if d == 0:
        return GeoHashBits(hash_.bits, hash_.step)
    bits = hash_.bits
    x = bits & _BX
    y = bits & _BY
    shift = 64 - hash_.step * 2
    zz = (_BX >> shift) & _MASK_64
    if d > 0:
        y = (y + (zz + 1)) & _MASK_64
    else:
        y = (y | zz) & _MASK_64
        y = (y - (zz + 1)) & _MASK_64
    y &= (_BY >> shift) & _MASK_64
    return GeoHashBits(bits=x | y, step=hash_.step)


@dataclass
class GeoHashNeighbors:
    north: GeoHashBits
    south: GeoHashBits
    east: GeoHashBits
    west: GeoHashBits
    north_east: GeoHashBits
    north_west: GeoHashBits
    south_east: GeoHashBits
    south_west: GeoHashBits


def neighbors(hash_: GeoHashBits) -> GeoHashNeighbors:
    return GeoHashNeighbors(
        east=_move_y(_move_x(hash_, 1), 0),
        west=_move_y(_move_x(hash_, -1), 0),
        north=_move_y(_move_x(hash_, 0), 1),
        south=_move_y(_move_x(hash_, 0), -1),
        north_east=_move_y(_move_x(hash_, 1), 1),
        north_west=_move_y(_move_x(hash_, -1), 1),
        south_east=_move_y(_move_x(hash_, 1), -1),
        south_west=_move_y(_move_x(hash_, -1), -1),
    )


def estimate_steps_by_radius(range_meters: float, lat: float) -> int:
    if range_meters == 0:
        return 26
    step = 1
    r = range_meters
    while r < MERCATOR_MAXIMUM:
        r *= 2
        step += 1
    step -= 2
    if lat > _POLAR_LATITUDE_STEP_1 or lat < -_POLAR_LATITUDE_STEP_1:
        step -= 1
        if lat > _POLAR_LATITUDE_STEP_2 or lat < -_POLAR_LATITUDE_STEP_2:
            step -= 1
    step = max(step, 1)
    step = min(step, 26)
    return step


_DEG_TO_RAD = math.pi / 180.0


def haversine_distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    lon1r = lon1 * _DEG_TO_RAD
    lon2r = lon2 * _DEG_TO_RAD
    v = math.sin((lon2r - lon1r) / 2)
    if abs(v) <= _GEO_EPSILON:
        return EARTH_RADIUS_IN_METERS * abs(lat2 * _DEG_TO_RAD - lat1 * _DEG_TO_RAD)
    lat1r = lat1 * _DEG_TO_RAD
    lat2r = lat2 * _DEG_TO_RAD
    u = math.sin((lat2r - lat1r) / 2)
    a = u * u + math.cos(lat1r) * math.cos(lat2r) * v * v
    return 2.0 * EARTH_RADIUS_IN_METERS * math.asin(math.sqrt(a))


_GEO_ALPHABET = b"0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_string(score: float) -> bytes:
    """Compute the 11-char standard geohash string (latitude range -90/90) from a stored 52-bit score."""
    bits52 = int(score)
    longitude, latitude = decode_to_lonlat(GeoHashBits(bits=bits52, step=26))
    # Re-encode with standard ranges (-90/90 for latitude).
    latitude_off = (latitude - -90.0) / (90.0 - -90.0)
    longitude_off = (longitude - -180.0) / (180.0 - -180.0)
    latitude_off = int(latitude_off * (1 << 26))
    longitude_off = int(longitude_off * (1 << 26))
    bits = _interleave64(latitude_off, longitude_off)
    buf = bytearray(_GEOHASH_STRING_LENGTH)
    for i in range(_GEOHASH_STRING_LENGTH):
        if i == _GEOHASH_STRING_LENGTH - 1:
            idx = 0
        else:
            idx = (bits >> (52 - (i + 1) * 5)) & 0x1F
        buf[i] = _GEO_ALPHABET[idx]
    return bytes(buf)
