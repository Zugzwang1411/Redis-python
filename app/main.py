import socket
import sys
import os
import threading

from app.state import (
    global_database,
    channel_subscribers,
    channel_subscribers_lock,
    acl_users,
    authenticated_connections,
    authenticated_connections_lock,
)
from app.rdb import load_rdb_file
from app.resp import parse_command, BufferConnection
from app.validation import validate_command_syntax
from app.replication import is_replica_connection, connect_to_master_and_ping, handle_master_commands
from app.commands.string import handle_set, handle_get, handle_incr
from app.commands.admin import (
    handle_auth,
    handle_ping,
    handle_echo,
    handle_type,
    handle_keys,
    handle_config,
    handle_acl,
    handle_info,
)
from app.commands.stream import handle_xadd, handle_xrange, handle_xread
from app.commands.zset import (
    handle_zadd,
    handle_zrank,
    handle_zrange,
    handle_zcard,
    handle_zscore,
    handle_zrem,
)
from app.commands.geo import handle_geoadd, handle_geopos, handle_geodist, handle_geosearch
from app.commands.pubsub import handle_subscribe, handle_publish, handle_unsubscribe
from app.commands.list import (
    handle_rpush,
    handle_lpush,
    handle_lrange,
    handle_llen,
    handle_lpop,
    handle_blpop,
)
from app.commands.replication_cmds import handle_replconf, handle_psync, handle_wait
from app.commands.transaction import handle_multi, handle_exec, handle_discard


def execute_single_command(connection, command, arguments, Database, stream_last_ids, subscribed_channels=None, in_subscribed_mode=None):
    """Execute a single command against Database, writing responses to the provided connection."""
    # Enforce authentication for non-replica connections
    if not is_replica_connection(connection):
        with authenticated_connections_lock:
            if connection not in authenticated_connections:
                if not acl_users.get("default", []):
                    # Default user has nopass: auto-authenticate this connection
                    authenticated_connections.add(connection)
                else:
                    # Allow AUTH so unauthenticated clients can authenticate
                    if command != "AUTH":
                        connection.sendall(b"-NOAUTH Authentication required.\r\n")
                        return

    if command == "AUTH":
        handle_auth(connection, arguments)
    elif command == "PING":
        handle_ping(connection, in_subscribed_mode)
    elif command == "ECHO":
        handle_echo(connection, arguments)
    elif command == "SET":
        handle_set(connection, command, arguments, Database)
    elif command == "GET":
        handle_get(connection, arguments, Database)
    elif command == "XADD":
        handle_xadd(connection, command, arguments, Database, stream_last_ids)
    elif command == "TYPE":
        handle_type(connection, arguments, Database)
    elif command == "XRANGE":
        handle_xrange(connection, arguments, Database)
    elif command == "XREAD":
        handle_xread(connection, arguments, Database)
    elif command == "INCR":
        handle_incr(connection, command, arguments, Database)
    elif command == "INFO":
        handle_info(connection)
    elif command == "REPLCONF":
        handle_replconf(connection, arguments)
    elif command == "PSYNC":
        handle_psync(connection, arguments)
    elif command == "WAIT":
        handle_wait(connection, arguments)
    elif command == "KEYS":
        handle_keys(connection, arguments, Database)
    elif command == "CONFIG":
        handle_config(connection, arguments)
    elif command == "SUBSCRIBE":
        handle_subscribe(connection, arguments, subscribed_channels, in_subscribed_mode)
    elif command == "PUBLISH":
        handle_publish(connection, arguments)
    elif command == "UNSUBSCRIBE":
        handle_unsubscribe(connection, arguments, subscribed_channels, in_subscribed_mode)
    elif command == "ZADD":
        handle_zadd(connection, command, arguments, Database)
    elif command == "ZRANK":
        handle_zrank(connection, arguments, Database)
    elif command == "ZRANGE":
        handle_zrange(connection, arguments, Database)
    elif command == "ZCARD":
        handle_zcard(connection, arguments, Database)
    elif command == "ZSCORE":
        handle_zscore(connection, arguments, Database)
    elif command == "ZREM":
        handle_zrem(connection, arguments, Database)
    elif command == "GEOADD":
        handle_geoadd(connection, command, arguments, Database)
    elif command == "GEOPOS":
        handle_geopos(connection, arguments, Database)
    elif command == "GEODIST":
        handle_geodist(connection, arguments, Database)
    elif command == "GEOSEARCH":
        handle_geosearch(connection, arguments, Database)
    elif command == "ACL":
        handle_acl(connection, arguments)
    elif command == "RPUSH":
        handle_rpush(connection, command, arguments, Database)
    elif command == "LRANGE":
        handle_lrange(connection, arguments, Database)
    elif command == "LPUSH":
        handle_lpush(connection, command, arguments, Database)
    elif command == "LLEN":
        handle_llen(connection, arguments, Database)
    elif command == "LPOP":
        handle_lpop(connection, command, arguments, Database)
    elif command == "BLPOP":
        handle_blpop(connection, command, arguments, Database)
    else:
        connection.sendall(b"-ERR unknown command\r\n")


def handle_client(connection):
    """Handle a single client connection - can receive multiple commands"""
    # Use global database shared across all clients
    Database = global_database
    subscribed_channels = set()
    stream_last_ids = {}
    in_transaction = False
    transaction_queue = []
    transaction_error = False
    in_subscribed_mode = [False]  # Use list for mutability
    # Define allowed commands in subscribed mode
    allowed_in_subscribed_mode = {"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"}
    while True:
        data = connection.recv(1024)
        if not data:
            break
        
        print("msg found: ", data)
        
        # Parse the RESP command
        command, arguments = parse_command(data)
        if command is None:
            continue
        arguments = arguments or []

        # Check if in subscribed mode and command is not allowed
        if in_subscribed_mode[0] and command not in allowed_in_subscribed_mode:
            cmd_lower = command.lower()
            error_msg = f"-ERR Can't execute '{cmd_lower}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
            connection.sendall(error_msg.encode())
            continue  # Skip executing this command

        if command == "MULTI":
            in_transaction, transaction_queue, transaction_error = handle_multi(
                connection, arguments, in_transaction, transaction_queue, transaction_error
            )
            continue

        if command == "EXEC":
            in_transaction, transaction_queue, transaction_error = handle_exec(
                connection, in_transaction, transaction_queue, transaction_error,
                Database, stream_last_ids, subscribed_channels, in_subscribed_mode,
                execute_single_command,
            )
            continue

        if command == "DISCARD":
            in_transaction, transaction_queue, transaction_error = handle_discard(
                connection, in_transaction, transaction_queue, transaction_error
            )
            continue

        if in_transaction:
            # Validate only syntax during queuing; runtime errors will be caught during EXEC
            is_valid, syntax_error = validate_command_syntax(command, arguments)
            if not is_valid:
                # Syntax error - abort transaction
                transaction_error = True
                connection.sendall(syntax_error)
            else:
                # Syntax OK - queue the command (even if it might fail at runtime)
                transaction_queue.append((command, arguments))
                connection.sendall(b"+QUEUED\r\n")
            continue

        # Not in transaction: execute immediately
        execute_single_command(connection, command, arguments, Database, stream_last_ids, subscribed_channels, in_subscribed_mode)
    
    # Clean up: remove connection from authenticated set and channel subscriptions
    with authenticated_connections_lock:
        authenticated_connections.discard(connection)
    with channel_subscribers_lock:
        for channel in list(channel_subscribers.keys()):
            channel_subscribers[channel].discard(connection)
            # Remove empty channel entries
            if len(channel_subscribers[channel]) == 0:
                del channel_subscribers[channel]
    
    connection.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    port = 6379
    args = sys.argv[1:]
    master_host = None
    master_port = None

    if '--port' in args:
        port_index = args.index('--port')
        try:
            port = int(args[port_index + 1])
        except (ValueError, IndexError):
            print("Invalid port number. Using default port 6379.")
            sys.exit(1)

    # Parse --replicaof flag
    if '--replicaof' in args:
        replicaof_index = args.index('--replicaof')
        try:
            if replicaof_index + 1 >= len(args):
                print("Invalid --replicaof format. Expected: --replicaof \"<HOST> <PORT>\"")
                sys.exit(1)
            
            replicaof_value = args[replicaof_index + 1]
            
            # Remove any quotes (handling partial quotes from shell splitting)
            replicaof_value = replicaof_value.strip('"').strip("'")
            
            # Try to split by space first (handles "localhost 6379" as one argument)
            parts = replicaof_value.split()
            
            if len(parts) >= 2:
                master_host = parts[0]
                master_port = int(parts[1])
            else:
                print("Invalid --replicaof format. Expected: --replicaof \"<HOST> <PORT>\"")
                sys.exit(1)
        except (ValueError, IndexError) as e:
            print(f"Invalid --replicaof argument: {e}")
            sys.exit(1)

    # Parse --dir flag
    import app.state as state_module
    if '--dir' in args:
        dir_index = args.index('--dir')
        try:
            if dir_index + 1 >= len(args):
                print("Invalid --dir format. Expected: --dir <DIRECTORY>")
                sys.exit(1)
            state_module.rdb_dir = args[dir_index + 1]
        except (ValueError, IndexError) as e:
            print(f"Invalid --dir argument: {e}")
            sys.exit(1)

    # Parse --dbfilename flag
    if '--dbfilename' in args:
        dbfilename_index = args.index('--dbfilename')
        try:
            if dbfilename_index + 1 >= len(args):
                print("Invalid --dbfilename format. Expected: --dbfilename <FILENAME>")
                sys.exit(1)
            state_module.rdb_dbfilename = args[dbfilename_index + 1]
        except (ValueError, IndexError) as e:
            print(f"Invalid --dbfilename argument: {e}")
            sys.exit(1)

    # Load RDB file if it exists
    if state_module.rdb_dir:
        rdb_filepath = os.path.join(state_module.rdb_dir, state_module.rdb_dbfilename)
    else:
        rdb_filepath = state_module.rdb_dbfilename
    
    load_rdb_file(rdb_filepath)

    # If replica mode, connect to master and send PING, then REPLCONF commands
    if master_host and master_port:
        master_socket, rdb_buffer = connect_to_master_and_ping(master_host, master_port, port)
        if master_socket:
            # Start a thread to handle commands from master
            master_thread = threading.Thread(target=handle_master_commands, args=(master_socket, rdb_buffer))
            master_thread.daemon = True
            master_thread.start()

    # Uncomment the code below to pass the first stage
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    
    while True:
        connection, _ = server_socket.accept()
        
        client_thread = threading.Thread(target=handle_client, args=(connection,))
        client_thread.daemon = True
        client_thread.start()


if __name__ == "__main__":
    main()
