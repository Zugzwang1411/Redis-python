"""List command handlers: RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP."""
from collections import deque
import threading

from app.state import blpop_waiters, global_database_lock
from app.replication import is_replica_connection, propagate_command_to_replicas


def _send_blpop_response(conn, list_key, value):
    v_str = value if isinstance(value, str) else value.decode("utf-8")
    key_str = list_key if isinstance(list_key, str) else list_key.decode("utf-8")
    resp = f"*2\r\n${len(key_str)}\r\n{key_str}\r\n${len(v_str)}\r\n{v_str}\r\n"
    conn.sendall(resp.encode())


def handle_rpush(connection, command, arguments, Database):
    if len(arguments) < 2:
        connection.sendall(b"-ERR wrong number of arguments for 'rpush' command\r\n")
        return
    key = arguments[0]
    values = arguments[1:]
    waiter_cond = None
    with global_database_lock:
        if key not in Database:
            Database[key] = {"type": "list", "values": values}
        else:
            Database[key]["values"].extend(values)
        connection.sendall(f":{len(Database[key]['values'])}\r\n".encode())
        if not is_replica_connection(connection):
            propagate_command_to_replicas(command, arguments)
        if key in blpop_waiters and len(blpop_waiters[key]) > 0:
            _, cond, result_holder = blpop_waiters[key].popleft()
            entry = Database[key]
            list_values = entry["values"]
            value = list_values.pop(0)
            if not list_values:
                del Database[key]
            result_holder[0] = (key, value)
            waiter_cond = cond
    if waiter_cond is not None:
        waiter_cond.acquire()
        waiter_cond.notify()
        waiter_cond.release()


def handle_lpush(connection, command, arguments, Database):
    if len(arguments) < 2:
        connection.sendall(b"-ERR wrong number of arguments for 'lpush' command\r\n")
        return
    key = arguments[0]
    values = arguments[1:]
    waiter_cond = None
    with global_database_lock:
        if key not in Database:
            Database[key] = {"type": "list", "values": list(values)}
        else:
            for v in values:
                Database[key]["values"].insert(0, v)
        connection.sendall(f":{len(Database[key]['values'])}\r\n".encode())
        if not is_replica_connection(connection):
            propagate_command_to_replicas(command, arguments)
        if key in blpop_waiters and len(blpop_waiters[key]) > 0:
            _, cond, result_holder = blpop_waiters[key].popleft()
            entry = Database[key]
            list_values = entry["values"]
            value = list_values.pop(0)
            if not list_values:
                del Database[key]
            result_holder[0] = (key, value)
            waiter_cond = cond
    if waiter_cond is not None:
        waiter_cond.acquire()
        waiter_cond.notify()
        waiter_cond.release()


def handle_lrange(connection, arguments, Database):
    if len(arguments) != 3:
        connection.sendall(b"-ERR wrong number of arguments for 'lrange' command\r\n")
        return
    key = arguments[0]
    start = int(arguments[1])
    stop = int(arguments[2])
    if key not in Database:
        connection.sendall(b"*0\r\n")
        return
    entry = Database[key]
    if entry["type"] != "list":
        connection.sendall(b"*0\r\n")
        return
    values = entry["values"]
    size_of_list = len(values)
    if start < 0:
        start = max(0, size_of_list + start)
    if stop < 0:
        stop = max(0, size_of_list + stop)
    range_values = values[start : stop + 1]
    response = f"*{len(range_values)}\r\n"
    for value in range_values:
        response += f"${len(value)}\r\n{value}\r\n"
    connection.sendall(response.encode())


def handle_llen(connection, arguments, Database):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'llen' command\r\n")
        return
    key = arguments[0]
    if key not in Database:
        connection.sendall(b":0\r\n")
        return
    entry = Database[key]
    if entry["type"] != "list":
        connection.sendall(b":0\r\n")
        return
    connection.sendall(f":{len(entry['values'])}\r\n".encode())


def handle_lpop(connection, command, arguments, Database):
    if len(arguments) < 1 or len(arguments) > 2:
        connection.sendall(b"-ERR wrong number of arguments for 'lpop' command\r\n")
        return
    key = arguments[0]
    count = int(arguments[1]) if len(arguments) == 2 else None
    if key not in Database:
        if count is not None:
            connection.sendall(b"*0\r\n")
        else:
            connection.sendall(b"$-1\r\n")
        return
    entry = Database[key]
    if entry["type"] != "list":
        if count is not None:
            connection.sendall(b"*0\r\n")
        else:
            connection.sendall(b"$-1\r\n")
        return
    values = entry["values"]
    if count is not None:
        n = min(count, len(values))
        popped = [values.pop(0) for _ in range(n)]
        if not values:
            del Database[key]
        response = f"*{len(popped)}\r\n"
        for v in popped:
            v_str = v if isinstance(v, str) else v.decode("utf-8")
            response += f"${len(v_str)}\r\n{v_str}\r\n"
        connection.sendall(response.encode())
    else:
        if not values:
            connection.sendall(b"$-1\r\n")
        else:
            v = values.pop(0)
            if not values:
                del Database[key]
            v_str = v if isinstance(v, str) else v.decode("utf-8")
            connection.sendall(f"${len(v_str)}\r\n{v_str}\r\n".encode())
    if not is_replica_connection(connection):
        propagate_command_to_replicas(command, arguments)


def handle_blpop(connection, command, arguments, Database):
    if len(arguments) < 2:
        connection.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
        return
    key = arguments[0]
    try:
        timeout = float(arguments[1])
    except (ValueError, TypeError):
        connection.sendall(b"-ERR value is not an integer or out of range\r\n")
        return
    if timeout < 0:
        connection.sendall(b"-ERR timeout is negative\r\n")
        return
    with global_database_lock:
        if key in Database:
            entry = Database[key]
            if entry["type"] != "list":
                connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                return
            values = entry["values"]
            if len(values) > 0:
                v = values.pop(0)
                if not values:
                    del Database[key]
                _send_blpop_response(connection, key, v)
                if not is_replica_connection(connection):
                    propagate_command_to_replicas(command, arguments)
                return
        result_holder = [None]
        cond = threading.Condition()
        if key not in blpop_waiters:
            blpop_waiters[key] = deque()
        blpop_waiters[key].append((connection, cond, result_holder))
    cond.acquire()
    try:
        if timeout == 0:
            cond.wait()
        else:
            cond.wait(timeout=timeout)
        pair = result_holder[0]
        if pair is not None:
            list_key, value = pair
            _send_blpop_response(connection, list_key, value)
        else:
            connection.sendall(b"*-1\r\n")
    finally:
        cond.release()
