"""Admin command handlers: AUTH, PING, ECHO, TYPE, KEYS, CONFIG, ACL, INFO."""
import hashlib

from app.state import (
    acl_users,
    authenticated_connections,
    authenticated_connections_lock,
    global_database_lock,
    rdb_dir,
    rdb_dbfilename,
)


def handle_auth(connection, arguments):
    if len(arguments) == 1:
        username = "default"
        password = str(arguments[0])
    elif len(arguments) == 2:
        username = str(arguments[0])
        password = str(arguments[1])
    else:
        connection.sendall(b"-ERR wrong number of arguments for 'auth' command\r\n")
        return
    if username not in acl_users:
        connection.sendall(b"-ERR invalid username-password pair\r\n")
    else:
        password_bytes = password.encode("utf-8")
        h = hashlib.sha256(password_bytes).hexdigest()
        if h in acl_users[username]:
            with authenticated_connections_lock:
                authenticated_connections.add(connection)
            connection.sendall(b"+OK\r\n")
        else:
            connection.sendall(b"-WRONGPASS invalid username-password pair or user is disabled.\r\n")


def handle_ping(connection, in_subscribed_mode):
    if not in_subscribed_mode[0]:
        connection.sendall(b"+PONG\r\n")
    else:
        connection.sendall(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")


def handle_echo(connection, arguments):
    if arguments and len(arguments) > 0:
        message = arguments[0]
        response = f"${len(message)}\r\n{message}\r\n"
        connection.sendall(response.encode())
    else:
        connection.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")


def handle_type(connection, arguments, Database):
    import time
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'type' command\r\n")
    else:
        key = arguments[0]
        if key not in Database:
            connection.sendall(b"+none\r\n")
        else:
            entry = Database[key]
            if entry["type"] == "string" and entry["expiry"] is not None and time.time() > entry["expiry"]:
                del Database[key]
                connection.sendall(b"+none\r\n")
            else:
                if entry["type"] == "string":
                    connection.sendall(b"+string\r\n")
                elif entry["type"] == "stream":
                    connection.sendall(b"+stream\r\n")
                elif entry["type"] == "zset":
                    connection.sendall(b"+zset\r\n")
                else:
                    connection.sendall(b"+none\r\n")


def handle_keys(connection, arguments, Database):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'keys' command\r\n")
    else:
        pattern = arguments[0]
        if pattern != "*":
            connection.sendall(b"-ERR pattern not supported\r\n")
        else:
            with global_database_lock:
                keys = list(Database.keys())
            response_parts = [f"*{len(keys)}\r\n".encode()]
            for key in keys:
                key_bytes = key.encode('utf-8')
                response_parts.append(f"${len(key_bytes)}\r\n{key}\r\n".encode())
            connection.sendall(b"".join(response_parts))


def handle_config(connection, arguments):
    if len(arguments) < 1:
        connection.sendall(b"-ERR wrong number of arguments for 'config' command\r\n")
    else:
        subcommand = str(arguments[0]).upper()
        if subcommand == "GET":
            if len(arguments) != 2:
                connection.sendall(b"-ERR wrong number of arguments for 'config get' command\r\n")
            else:
                param_name = str(arguments[1]).lower()
                param_value = None
                if param_name == "dir":
                    param_value = rdb_dir
                elif param_name == "dbfilename":
                    param_value = rdb_dbfilename
                if param_value is not None:
                    name_bytes = param_name.encode('utf-8')
                    value_bytes = str(param_value).encode('utf-8')
                    response = f"*2\r\n${len(name_bytes)}\r\n{param_name}\r\n${len(value_bytes)}\r\n{param_value}\r\n"
                    connection.sendall(response.encode())
                else:
                    connection.sendall(b"*0\r\n")
        else:
            connection.sendall(b"-ERR unknown subcommand or wrong number of arguments for 'config' command\r\n")


def handle_acl(connection, arguments):
    if len(arguments) < 1:
        connection.sendall(b"-ERR wrong number of arguments for 'acl' command\r\n")
    else:
        subcommand = str(arguments[0]).upper()
        if subcommand == "WHOAMI" and len(arguments) == 1:
            connection.sendall(b"$7\r\ndefault\r\n")
        elif subcommand == "GETUSER" and len(arguments) == 2:
            username = str(arguments[1])
            if username not in acl_users:
                connection.sendall(f"-ERR User '{username}' does not exist\r\n".encode())
            else:
                passwords = acl_users[username]
                if not passwords:
                    connection.sendall(b"*4\r\n$5\r\nflags\r\n*1\r\n$6\r\nnopass\r\n$9\r\npasswords\r\n*0\r\n")
                else:
                    parts = [b"*4\r\n$5\r\nflags\r\n*0\r\n$9\r\npasswords\r\n"]
                    parts.append(f"*{len(passwords)}\r\n".encode())
                    for h in passwords:
                        parts.append(f"${len(h)}\r\n{h}\r\n".encode())
                    connection.sendall(b"".join(parts))
        elif subcommand == "SETUSER" and len(arguments) >= 2:
            username = str(arguments[1])
            if username not in acl_users:
                acl_users[username] = []
            for rule in arguments[2:]:
                rule_str = str(rule)
                if rule_str.startswith(">"):
                    password = rule_str[1:]
                    password_bytes = password.encode("utf-8") if isinstance(password, str) else password
                    h = hashlib.sha256(password_bytes).hexdigest()
                    acl_users[username].append(h)
            connection.sendall(b"+OK\r\n")
        else:
            connection.sendall(b"-ERR unknown command\r\n")


def handle_info(connection):
    server_port = connection.getsockname()[1]
    if server_port == 6379:
        info_text = "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0"
    else:
        info_text = "role:slave"
    response = f"${len(info_text)}\r\n{info_text}\r\n"
    connection.sendall(response.encode())
