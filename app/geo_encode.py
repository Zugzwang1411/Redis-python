MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

def encode(latitude: float, longitude: float) -> int:
    normalized_latitude = 2**26*((latitude - MIN_LATITUDE) / LATITUDE_RANGE)
    normalized_longitude = 2**26*((longitude - MIN_LONGITUDE) / LONGITUDE_RANGE)

    normalized_latitude = int(normalized_latitude)
    normalized_longitude = int(normalized_longitude)

    return interleave(normalized_latitude, normalized_longitude)

def interleave(latitude: int, longitude: int) -> int:
    latitude = spread_int32_to_int64(latitude)
    longitude = spread_int32_to_int64(longitude)

    y_shifted = longitude << 1
    return latitude | y_shifted

def spread_int32_to_int64(v: int) -> int:
    v = v & 0xFFFFFFFF

    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555

    return v