"""Sorted set command handlers: ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM."""
from app.replication import is_replica_connection, propagate_command_to_replicas


def handle_zadd(connection, command, arguments, Database):
    if len(arguments) != 3:
        connection.sendall(b"-ERR wrong number of arguments for 'zadd' command\r\n")
        return
    key = arguments[0]
    try:
        score = float(arguments[1])
    except ValueError:
        connection.sendall(b"-ERR value is not a valid float\r\n")
        return
    member = arguments[2]
    if key not in Database:
        Database[key] = {"type": "zset", "members": [(score, member)]}
        new_members_count = 1
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
            new_members_count = 1
        else:
            new_members_count = 0
    connection.sendall(f":{new_members_count}\r\n".encode())
    if not is_replica_connection(connection):
        propagate_command_to_replicas(command, arguments)


def handle_zrank(connection, arguments, Database):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'zrank' command\r\n")
        return
    key = arguments[0]
    member = arguments[1]
    if key not in Database:
        connection.sendall(b"$-1\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b"$-1\r\n")
        return
    members = entry["members"]
    rank = None
    for i, (_, m) in enumerate(members):
        if m == member:
            rank = i
            break
    if rank is None:
        connection.sendall(b"$-1\r\n")
    else:
        connection.sendall(f":{rank}\r\n".encode())


def handle_zrange(connection, arguments, Database):
    if len(arguments) != 3:
        connection.sendall(b"-ERR wrong number of arguments for 'zrange' command\r\n")
        return
    key = arguments[0]
    start = int(arguments[1])
    stop = int(arguments[2])
    if key not in Database:
        connection.sendall(b"*0\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b"*0\r\n")
        return
    members = entry["members"]
    if start < 0:
        start = max(0, len(members) + start)
    if stop < 0:
        stop = max(0, len(members) + stop)
    range_members = members[start : stop + 1]
    response = f"*{len(range_members)}\r\n"
    for _, member in range_members:
        response += f"${len(member)}\r\n{member}\r\n"
    connection.sendall(response.encode())


def handle_zcard(connection, arguments, Database):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'zcard' command\r\n")
        return
    key = arguments[0]
    if key not in Database:
        connection.sendall(b":0\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b":0\r\n")
        return
    connection.sendall(f":{len(entry['members'])}\r\n".encode())


def handle_zscore(connection, arguments, Database):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'zscore' command\r\n")
        return
    key = arguments[0]
    member = arguments[1]
    if key not in Database:
        connection.sendall(b"$-1\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b"$-1\r\n")
        return
    for score, m in entry["members"]:
        if m == member:
            score_str = str(score)
            connection.sendall(f"${len(score_str)}\r\n{score_str}\r\n".encode())
            return
    connection.sendall(b"$-1\r\n")


def handle_zrem(connection, arguments, Database):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'zrem' command\r\n")
        return
    key = arguments[0]
    member = arguments[1]
    if key not in Database:
        connection.sendall(b":0\r\n")
        return
    entry = Database[key]
    if entry["type"] != "zset":
        connection.sendall(b":0\r\n")
        return
    members = entry["members"]
    removed_count = 0
    for i, (_, m) in enumerate(members):
        if m == member:
            members.pop(i)
            removed_count += 1
    connection.sendall(f":{removed_count}\r\n".encode())
    if removed_count > 0:
        Database[key]["members"] = members
        if len(members) == 0:
            del Database[key]
