"""
Replication: master-replica sync, propagation, and replica command execution.
"""
import socket
import threading
import time

import app.state as state_module
from app.state import (
    global_database,
    replica_connections,
    replica_connections_lock,
    replica_offset_map,
    replica_offset_map_lock,
    replica_socket_locks,
    replica_socket_locks_lock,
    master_repl_offset_lock,
    stream_conditions,
    stream_conditions_lock,
)
from app.resp import encode_command_as_resp, parse_resp


def is_replica_connection(connection):
    """
    Check if a connection is a replica connection.

    Args:
        connection: The socket connection to check

    Returns:
        bool: True if the connection is from a replica, False otherwise
    """
    with replica_connections_lock:
        return connection in replica_connections


def send_getack_to_replica(replica_conn):
    """
    Send REPLCONF GETACK * to a replica.

    Args:
        replica_conn: The replica connection socket

    Returns:
        bool: True if sent successfully, False otherwise
    """
    # Get or create lock for this replica connection
    with replica_socket_locks_lock:
        if replica_conn not in replica_socket_locks:
            replica_socket_locks[replica_conn] = threading.Lock()
        sock_lock = replica_socket_locks[replica_conn]

    # Acquire lock to ensure thread-safe socket access
    with sock_lock:
        try:
            # Send REPLCONF GETACK * command
            getack_command = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
            replica_conn.sendall(getack_command)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            return False


def propagate_command_to_replicas(command, arguments):
    """
    Propagate a write command to all connected replicas.

    This function sends the command as a RESP array to all replica connections.
    Replicas process these commands silently (they don't send responses back).

    Args:
        command: The command name (e.g., "SET")
        arguments: List of command arguments (e.g., ["foo", "bar"])
    """
    # Encode the command as a RESP array
    resp_command = encode_command_as_resp(command, arguments)

    # Update master replication offset (total bytes sent to replicas)
    command_bytes = len(resp_command)
    with master_repl_offset_lock:
        state_module.master_repl_offset += command_bytes

    # Send to all replica connections
    with replica_connections_lock:
        # Create a copy of the set to avoid modification during iteration
        replicas = list(replica_connections)

    for replica_conn in replicas:
        try:
            # Send command without waiting for response
            # Replicas process commands silently and don't send responses
            replica_conn.sendall(resp_command)
        except (BrokenPipeError, ConnectionResetError, OSError):
            # Replica connection is broken, remove it from the set
            with replica_connections_lock:
                replica_connections.discard(replica_conn)
            # Also remove from offset map and socket locks
            with replica_offset_map_lock:
                replica_offset_map.pop(replica_conn, None)
            with replica_socket_locks_lock:
                replica_socket_locks.pop(replica_conn, None)


def execute_command_for_replica(connection, command, arguments, Database, stream_last_ids, replica_offset):
    """
    Execute a command without sending a response back.
    This is used for processing propagated commands from the master.
    Exception: REPLCONF GETACK * should send a response.

    Args:
        replica_offset: List containing the current offset (modified in place for GETACK)
    """
    # Handle REPLCONF GETACK - this is the only command that gets a response after handshake
    if command == "REPLCONF" and len(arguments) == 2:
        # Convert arguments to strings and uppercase for comparison
        arg0_str = str(arguments[0]).upper()
        arg1_str = str(arguments[1])

        if arg0_str == "GETACK" and arg1_str == "*":
            # Send REPLCONF ACK <offset> as RESP array
            # Format: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<offset_len>\r\n<offset>\r\n
            offset_value = replica_offset[0]
            offset_str = str(offset_value)
            response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(offset_str)}\r\n{offset_str}\r\n"
            connection.sendall(response.encode())
            return

    # All other commands are processed silently (no response)
    if command == "PING":
        # PING is processed silently (no response sent back to master)
        pass

    elif command == "SET":
        if len(arguments) >= 2:
            key = arguments[0]
            value = arguments[1]
            if len(arguments) == 2:
                # SET key value (no expiry)
                Database[key] = {"type": "string", "value": value, "expiry": None}
            elif len(arguments) == 4:
                # SET key value EX seconds or SET key value PX milliseconds
                expiry_type = arguments[2].upper()
                try:
                    expiry_time = int(arguments[3])
                    if expiry_type == "EX":
                        # Expiry in seconds
                        expiry_timestamp = time.time() + expiry_time
                        Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
                    elif expiry_type == "PX":
                        # Expiry in milliseconds
                        expiry_timestamp = time.time() + (expiry_time / 1000.0)
                        Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
                except ValueError:
                    pass  # Ignore errors for propagated commands

    elif command == "XADD":
        # XADD stream_key entry_id field1 value1 field2 value2 ...
        # Master sends the final entry_id (no * or auto-generation needed)
        if len(arguments) >= 4 and len(arguments) % 2 == 0:
            stream_key = arguments[0]
            entry_id = arguments[1]  # Master sends the final entry_id

            # Parse field-value pairs
            fields = {}
            for i in range(2, len(arguments), 2):
                field = arguments[i]
                value = arguments[i + 1]
                fields[field] = value

            # Create stream if it doesn't exist
            if stream_key not in Database:
                Database[stream_key] = {"type": "stream", "entries": []}

            entry = Database[stream_key]

            # Check if it's actually a stream (not overwriting a string)
            if entry["type"] == "stream":
                # Add entry to stream
                stream_entry = {"id": entry_id, "fields": fields}
                entry["entries"].append(stream_entry)

                # Update the last ID for this stream
                stream_last_ids[stream_key] = entry_id

                # Notify any waiting XREAD commands
                with stream_conditions_lock:
                    if stream_key in stream_conditions:
                        condition = stream_conditions[stream_key]
                        condition.acquire()
                        condition.notify_all()
                        condition.release()

    elif command == "INCR":
        if len(arguments) == 1:
            key = arguments[0]

            if key not in Database:
                Database[key] = {"type": "string", "value": "0", "expiry": None}
                new_value = 1
            else:
                entry = Database[key]

                # Check if it's a string type
                if entry["type"] != "string":
                    return  # Ignore wrong type for propagated commands

                # Check expiry
                if entry.get("expiry") is not None and time.time() > entry["expiry"]:
                    # Key expired, treat as if it doesn't exist
                    del Database[key]
                    Database[key] = {"type": "string", "value": "0", "expiry": None}
                    new_value = 1
                else:
                    # Key exists - try to convert value to integer
                    try:
                        current_value = int(entry["value"])
                        new_value = current_value + 1
                    except ValueError:
                        return  # Ignore errors for propagated commands

            # Store the new value as a string
            Database[key]["value"] = str(new_value)

    elif command == "ZADD":
        if len(arguments) == 3:
            key = arguments[0]
            try:
                score = float(arguments[1])  # 64-bit floating point
            except ValueError:
                return  # Ignore errors for propagated commands
            member = arguments[2]

            # Create sorted set if it doesn't exist
            if key not in Database:
                # Create new sorted set with the member
                Database[key] = {"type": "zset", "members": [(score, member)]}
            else:
                entry = Database[key]

                # Check if it's actually a sorted set
                if entry["type"] != "zset":
                    return  # Ignore wrong type for propagated commands

                # Check if member already exists
                members = entry["members"]
                member_exists = False
                for i, (existing_score, existing_member) in enumerate(members):
                    if existing_member == member:
                        member_exists = True
                        # Update the score and re-sort
                        members[i] = (score, member)
                        members.sort(key=lambda x: (x[0], x[1]))  # Sort by score, then member for stability
                        break

                if not member_exists:
                    # Add new member and keep sorted
                    members.append((score, member))
                    members.sort(key=lambda x: (x[0], x[1]))  # Sort by score, then member for stability


def connect_to_master_and_ping(master_host, master_port, replica_port):
    """Connect to master server and send PING, then REPLCONF commands as RESP arrays."""
    try:
        # Create client socket
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to master
        master_socket.connect((master_host, master_port))

        # Step 1: Send PING command as RESP array: *1\r\n$4\r\nPING\r\n
        ping_command = b"*1\r\n$4\r\nPING\r\n"
        master_socket.sendall(ping_command)

        # Read PING response (+PONG\r\n)
        response = master_socket.recv(1024)

        # Step 2: Send REPLCONF listening-port <PORT>
        # Format: *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$<port_len>\r\n<port>\r\n
        port_str = str(replica_port)
        replconf_listening_port = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(port_str)}\r\n{port_str}\r\n"
        master_socket.sendall(replconf_listening_port.encode())

        # Read REPLCONF listening-port response (+OK\r\n)
        response = master_socket.recv(1024)

        # Step 3: Send REPLCONF capa psync2
        # Format: *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
        replconf_capa = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        master_socket.sendall(replconf_capa)

        # Read REPLCONF capa response (+OK\r\n)
        response = master_socket.recv(1024)

        # Step 4: Send PSYNC 0
        # Format: *2\r\n$5\r\nPSYNC\r\n$1\r\n0\r\n
        psync_0 = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_socket.sendall(psync_0)

        # Read PSYNC 0 response (+FULLRESYNC\r\n)
        # The RDB file header might be in the same recv, so read carefully
        response_buffer = b""
        while b"\r\n" not in response_buffer:
            chunk = master_socket.recv(1024)
            if not chunk:
                return None, b""
            response_buffer += chunk

        # Find the end of PSYNC response
        psync_end = response_buffer.find(b"\r\n") + 2
        # Any remaining data after PSYNC response is the RDB file header
        remaining_after_psync = response_buffer[psync_end:]

        # Return both socket and remaining buffer
        return master_socket, remaining_after_psync
    except Exception as e:
        print(f"Error connecting to master: {e}")
        return None, b""


def read_rdb_file(master_socket, initial_buffer=b""):
    """
    Read the RDB file from master after PSYNC response.
    Format: $<length>\r\n<binary_data> (no trailing \r\n after binary data)
    Returns: (success: bool, remaining_buffer: bytes)
    """
    # Use provided initial buffer (may contain RDB header from PSYNC response)
    buffer = initial_buffer

    # Read until we get the $ character
    while b"$" not in buffer:
        chunk = master_socket.recv(1024)
        if not chunk:
            return False, b""
        buffer += chunk

    # Find the $ position
    dollar_pos = buffer.find(b"$")
    buffer = buffer[dollar_pos:]

    # Find the \r\n after the length
    length_end = buffer.find(b"\r\n")
    if length_end == -1:
        # Need more data
        while b"\r\n" not in buffer:
            chunk = master_socket.recv(1024)
            if not chunk:
                return False, b""
            buffer += chunk
        length_end = buffer.find(b"\r\n")

    # Parse the length
    length_str = buffer[1:length_end].decode('utf-8')
    rdb_length = int(length_str)

    # Get the data after \r\n
    data_start = length_end + 2
    buffer = buffer[data_start:]

    # Read the RDB file data
    while len(buffer) < rdb_length:
        chunk = master_socket.recv(1024)
        if not chunk:
            return False, b""
        buffer += chunk

    # Extract RDB data (we don't need to process it for empty RDB)
    rdb_data = buffer[:rdb_length]
    # Remaining data might contain commands, so we keep it
    remaining = buffer[rdb_length:]

    return True, remaining


def handle_master_commands(master_socket, initial_rdb_buffer=b""):
    """
    Continuously read and process commands from the master.
    Commands are processed without sending responses back.
    """
    Database = global_database
    stream_last_ids = {}
    buffer = b""
    # Track offset: total bytes of commands processed
    # Use a list to allow modification in execute_command_for_replica
    replica_offset = [0]

    # First, read the RDB file
    try:
        success, remaining = read_rdb_file(master_socket, initial_rdb_buffer)
        if not success:
            return
        buffer = remaining
    except Exception as e:
        print(f"Error reading RDB file: {e}")
        return

    # Now continuously read and process commands
    while True:
        try:
            # If there's no buffered data, read from master
            if not buffer:
                chunk = master_socket.recv(1024)
                if not chunk:
                    break
                buffer += chunk

            # Parse and process all complete commands in buffer
            pos = 0
            while pos < len(buffer):
                # Store start position to calculate command byte size
                start_pos = pos

                # Try to parse a command from current position
                parsed, new_pos = parse_resp(buffer, pos)

                if parsed is None:
                    # Incomplete command, wait for more data
                    break

                # Calculate the byte size of this command
                command_bytes = new_pos - start_pos

                # Update position
                pos = new_pos

                # Process the command
                if isinstance(parsed, list) and len(parsed) > 0:
                    command = parsed[0].upper() if isinstance(parsed[0], str) else parsed[0]
                    arguments = parsed[1:] if len(parsed) > 1 else []

                    # Execute command (REPLCONF GETACK will send response, others won't)
                    # For GETACK, the offset used in response is the current offset (before this command)
                    execute_command_for_replica(
                        master_socket, command, arguments, Database, stream_last_ids, replica_offset
                    )

                    # Update offset after processing
                    # Add the command bytes to offset for all commands (including GETACK)
                    replica_offset[0] += command_bytes

            # Keep remaining unparsed data in buffer
            buffer = buffer[pos:]

        except Exception as e:
            print(f"Error handling master commands: {e}")
            break

    master_socket.close()
