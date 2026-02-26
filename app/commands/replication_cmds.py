"""Replication command handlers: REPLCONF, PSYNC, WAIT."""
import time
import threading

from app.state import (
    EMPTY_RDB_BINARY,
    replica_connections,
    replica_connections_lock,
    replica_offset_map,
    replica_offset_map_lock,
    replica_socket_locks,
    replica_socket_locks_lock,
    master_repl_offset,
    master_repl_offset_lock,
)
from app.replication import is_replica_connection, send_getack_to_replica

import app.state as state_module


def handle_replconf(connection, arguments):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'replconf' command\r\n")
        return
    arg0 = str(arguments[0]).upper()
    arg1 = str(arguments[1])
    if arg0 == "ACK" and is_replica_connection(connection):
        try:
            offset_value = int(arg1)
        except ValueError:
            return
        with replica_offset_map_lock:
            replica_offset_map[connection] = offset_value
        return
    connection.sendall(b"+OK\r\n")


def handle_psync(connection, arguments):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'psync' command\r\n")
        return
    repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    response = f"+FULLRESYNC {repl_id} 0\r\n"
    connection.sendall(response.encode())
    rdb_length = len(EMPTY_RDB_BINARY)
    rdb_header = f"${rdb_length}\r\n".encode()
    connection.sendall(rdb_header + EMPTY_RDB_BINARY)
    with replica_connections_lock:
        replica_connections.add(connection)
    with replica_socket_locks_lock:
        replica_socket_locks[connection] = threading.Lock()
    with replica_offset_map_lock:
        replica_offset_map[connection] = 0


def handle_wait(connection, arguments):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'wait' command\r\n")
        return
    try:
        numreplicas = int(arguments[0])
        timeout_ms = int(arguments[1])
    except ValueError:
        connection.sendall(b"-ERR value is not an integer or out of range\r\n")
        return
    with replica_connections_lock:
        replicas = list(replica_connections)
    if numreplicas == 0:
        connection.sendall(b":0\r\n")
        return
    with master_repl_offset_lock:
        current_master_offset = state_module.master_repl_offset
    if current_master_offset == 0:
        num_connected = len(replicas)
        connection.sendall(f":{num_connected}\r\n".encode())
        return
    for replica_conn in replicas:
        sent = send_getack_to_replica(replica_conn)
        if not sent:
            with replica_connections_lock:
                replica_connections.discard(replica_conn)
            with replica_offset_map_lock:
                replica_offset_map.pop(replica_conn, None)
            with replica_socket_locks_lock:
                replica_socket_locks.pop(replica_conn, None)
    start_time = time.time()
    timeout_seconds = timeout_ms / 1000.0
    check_interval = 0.01
    while True:
        elapsed = time.time() - start_time
        remaining_time = timeout_seconds - elapsed
        if remaining_time <= 0:
            break
        acknowledged_count = 0
        with replica_offset_map_lock:
            for _, offset in replica_offset_map.items():
                if offset >= current_master_offset:
                    acknowledged_count += 1
        if acknowledged_count >= numreplicas:
            break
        time.sleep(min(check_interval, remaining_time))
    acknowledged_count = 0
    with replica_offset_map_lock:
        for _, offset in replica_offset_map.items():
            if offset >= current_master_offset:
                acknowledged_count += 1
    connection.sendall(f":{acknowledged_count}\r\n".encode())
