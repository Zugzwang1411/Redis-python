"""Geo command handlers: GEOADD, GEOPOS, GEODIST, GEOSEARCH."""
import math

from app.geo_encode import encode
from app.geo_decode import decode
from app.replication import is_replica_connection, propagate_command_to_replicas

EARTH_RADIUS_M = 6372797.560856


def handle_geoadd(connection, command, arguments, Database):
    if len(arguments) != 4:
        connection.sendall(b"-ERR wrong number of arguments for 'geoadd' command\r\n")
        return
    try:
        longitude = float(arguments[1])
        latitude = float(arguments[2])
    except (ValueError, TypeError):
        connection.sendall(b"-ERR invalid longitude,latitude pair\r\n")
        return
    LONGITUDE_MIN, LONGITUDE_MAX = -180.0, 180.0
    LATITUDE_MIN, LATITUDE_MAX = -85.05112878, 85.05112878
    if not (LONGITUDE_MIN <= longitude <= LONGITUDE_MAX) or not (
        LATITUDE_MIN <= latitude <= LATITUDE_MAX
    ):
        pair_str = f"{longitude:.6f},{latitude:.6f}"
        connection.sendall(f"-ERR invalid longitude,latitude pair {pair_str}\r\n".encode())
        return
    score = encode(latitude, longitude)
    member = arguments[3]
    key = arguments[0]
    if key not in Database:
        Database[key] = {"type": "zset", "members": [(score, member)]}
        connection.sendall(b":1\r\n")
    else:
        entry = Database[key]
        if entry["type"] != "zset":
            connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            return
        members = entry["members"]
        member_exists = False
        for i, (_, existing_member) in enumerate(members):
            if existing_member == member:
                member_exists = True
                members[i] = (score, member)
                members.sort(key=lambda x: (x[0], x[1]))
                break
        if not member_exists:
            members.append((score, member))
            members.sort(key=lambda x: (x[0], x[1]))
            connection.sendall(b":1\r\n")
        else:
            connection.sendall(b":0\r\n")
    if not is_replica_connection(connection):
        propagate_command_to_replicas(command, arguments)


def handle_geopos(connection, arguments, Database):
    if len(arguments) < 2:
        connection.sendall(b"-ERR wrong number of arguments for 'geopos' command\r\n")
        return
    key = arguments[0]
    members_to_lookup = arguments[1:]
    n = len(members_to_lookup)
    if key not in Database:
        response = f"*{n}\r\n" + (n * "*-1\r\n")
        connection.sendall(response.encode())
        return
    entry = Database[key]
    if entry["type"] != "zset":
        response = f"*{n}\r\n" + (n * "*-1\r\n")
        connection.sendall(response.encode())
        return
    zset_members = {}
    for score, m in entry["members"]:
        k = m if isinstance(m, str) else m.decode("utf-8")
        zset_members[k] = score
    parts = [f"*{n}\r\n"]
    for member in members_to_lookup:
        m = member if isinstance(member, str) else member.decode("utf-8")
        if m in zset_members:
            latitude, longitude = decode(int(zset_members[m]))
            lon_str = str(longitude)
            lat_str = str(latitude)
            parts.append(f"*2\r\n${len(lon_str)}\r\n{lon_str}\r\n${len(lat_str)}\r\n{lat_str}\r\n")
        else:
            parts.append("*-1\r\n")
    connection.sendall("".join(parts).encode())


def handle_geodist(connection, arguments, Database):
    if len(arguments) != 3:
        connection.sendall(b"-ERR wrong number of arguments for 'geodist' command\r\n")
        return
    key = arguments[0]
    member1 = arguments[1]
    member2 = arguments[2]
    if key not in Database:
        connection.sendall(b"$-1\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b"$-1\r\n")
        return
    zset_members = {}
    for score, m in entry["members"]:
        k = m if isinstance(m, str) else m.decode("utf-8")
        zset_members[k] = score
    m1 = member1 if isinstance(member1, str) else member1.decode("utf-8")
    m2 = member2 if isinstance(member2, str) else member2.decode("utf-8")
    if m1 not in zset_members or m2 not in zset_members:
        connection.sendall(b"$-1\r\n")
        return
    lat1, lon1 = decode(int(zset_members[m1]))
    lat2, lon2 = decode(int(zset_members[m2]))
    lat1_r = math.radians(lat1)
    lon1_r = math.radians(lon1)
    lat2_r = math.radians(lat2)
    lon2_r = math.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = EARTH_RADIUS_M * c
    dist_str = f"{round(distance, 4)}"
    connection.sendall(f"${len(dist_str)}\r\n{dist_str}\r\n".encode())


def handle_geosearch(connection, arguments, Database):
    if len(arguments) < 7:
        connection.sendall(b"-ERR wrong number of arguments for 'geosearch' command\r\n")
        return
    key = arguments[0]
    if arguments[1].upper() != "FROMLONLAT" or arguments[4].upper() != "BYRADIUS":
        connection.sendall(b"-ERR invalid GEOSEARCH options\r\n")
        return
    try:
        lon_center = float(arguments[2])
        lat_center = float(arguments[3])
        radius_val = float(arguments[5])
    except (ValueError, TypeError):
        connection.sendall(b"-ERR invalid argument for GEOSEARCH\r\n")
        return
    unit = (arguments[6] or "m").lower()
    if unit == "m":
        radius_m = radius_val
    elif unit == "km":
        radius_m = radius_val * 1000
    elif unit == "mi":
        radius_m = radius_val * 1609.34
    elif unit == "ft":
        radius_m = radius_val * 0.3048
    else:
        radius_m = radius_val
    if key not in Database:
        connection.sendall(b"*0\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b"*0\r\n")
        return
    results = []
    for score, member in entry["members"]:
        lat, lon = decode(int(score))
        lat_r = math.radians(lat)
        lon_r = math.radians(lon)
        lat_c_r = math.radians(lat_center)
        lon_c_r = math.radians(lon_center)
        dlat = lat_r - lat_c_r
        dlon = lon_r - lon_c_r
        a = math.sin(dlat / 2) ** 2 + math.cos(lat_c_r) * math.cos(lat_r) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        dist = EARTH_RADIUS_M * c
        if dist <= radius_m:
            m = member if isinstance(member, str) else member.decode("utf-8")
            results.append(m)
    response = f"*{len(results)}\r\n"
    for m in results:
        response += f"${len(m)}\r\n{m}\r\n"
    connection.sendall(response.encode())
