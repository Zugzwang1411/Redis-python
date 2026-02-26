"""String command handlers: SET, GET, INCR."""
import time

from app.replication import is_replica_connection, propagate_command_to_replicas


def handle_set(connection, command, arguments, Database):
    if len(arguments) < 2:
        connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
    elif len(arguments) == 2:
        key = arguments[0]
        value = arguments[1]
        Database[key] = {"type": "string", "value": value, "expiry": None}
        connection.sendall(b"+OK\r\n")
        if not is_replica_connection(connection):
            propagate_command_to_replicas(command, arguments)
    elif len(arguments) == 4:
        key = arguments[0]
        value = arguments[1]
        expiry_type = arguments[2].upper()
        try:
            expiry_time = int(arguments[3])
        except ValueError:
            connection.sendall(b"-ERR value is not an integer or out of range\r\n")
            return

        if expiry_type == "EX":
            expiry_timestamp = time.time() + expiry_time
            Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
            connection.sendall(b"+OK\r\n")
            if not is_replica_connection(connection):
                propagate_command_to_replicas(command, arguments)
        elif expiry_type == "PX":
            expiry_timestamp = time.time() + (expiry_time / 1000.0)
            Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
            connection.sendall(b"+OK\r\n")
            if not is_replica_connection(connection):
                propagate_command_to_replicas(command, arguments)
        else:
            connection.sendall(b"-ERR syntax error\r\n")
    else:
        connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")


def handle_get(connection, arguments, Database):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
        return
    key = arguments[0]
    if key not in Database:
        connection.sendall(b"$-1\r\n")
    else:
        entry = Database[key]
        if entry["type"] == "string":
            if entry.get("expiry") is not None and time.time() > entry["expiry"]:
                del Database[key]
                connection.sendall(b"$-1\r\n")
            else:
                value = entry["value"]
                msg = f"${len(value)}\r\n{value}\r\n"
                connection.sendall(msg.encode())
        else:
            connection.sendall(b"$-1\r\n")


def handle_incr(connection, command, arguments, Database):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'incr' command\r\n")
    else:
        key = arguments[0]
        if key not in Database:
            Database[key] = {"type": "string", "value": "0", "expiry": None}
            new_value = 1
        else:
            entry = Database[key]
            if entry["type"] != "string":
                connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                return
            if entry.get("expiry") is not None and time.time() > entry["expiry"]:
                del Database[key]
                Database[key] = {"type": "string", "value": "0", "expiry": None}
                new_value = 0
            else:
                try:
                    current_value = int(entry["value"])
                    new_value = current_value + 1
                except ValueError:
                    connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                    return

        Database[key]["value"] = str(new_value)
        connection.sendall(f":{new_value}\r\n".encode())
        if not is_replica_connection(connection):
            propagate_command_to_replicas(command, arguments)
