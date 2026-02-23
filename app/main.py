import hashlib
import socket
import threading
import time
import copy
import sys
import base64
import os
import struct
import math
from app.geo_encode import encode
from app.geo_decode import decode

# Global dictionary to store Condition objects for each stream (for blocking XREAD)
stream_conditions = {}
stream_conditions_lock = threading.Lock()

# Global database shared across all client threads
global_database = {}
global_database_lock = threading.Lock()

# Empty RDB file (base64 encoded)
EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
EMPTY_RDB_BINARY = base64.b64decode(EMPTY_RDB_BASE64)

# Global set to track replica connections (connections from replicas)
# These are the connections that replicas use to connect to the master
replica_connections = set()
replica_connections_lock = threading.Lock()

# Master replication offset: total bytes of write commands sent to replicas
master_repl_offset = 0
master_repl_offset_lock = threading.Lock()

# Map replica connections to their current acknowledged offset
# Key: replica connection socket, Value: acknowledged offset (int)
replica_offset_map = {}
replica_offset_map_lock = threading.Lock()

# Map replica connections to locks for thread-safe socket access
replica_socket_locks = {}
replica_socket_locks_lock = threading.Lock()

# Global map to track channel subscriptions
# Key: channel_name (str), Value: set of connections subscribed to that channel
channel_subscribers = {}
channel_subscribers_lock = threading.Lock()

# ACL user table: username (str) -> list of SHA-256 password hashes (empty = nopass)
# "default" must always exist (created at startup).
acl_users = {"default": []}

# RDB configuration
rdb_dir = ""
rdb_dbfilename = "dump.rdb"


def parse_size_encoding(data, pos):
    """
    Parse size-encoded value from RDB file.
    Returns: (size_value, new_position, is_special)
    is_special indicates if this is a special encoding (0b11) that needs special handling
    """
    if pos >= len(data):
        return None, pos, False
    
    first_byte = data[pos]
    first_two_bits = (first_byte >> 6) & 0b11
    
    if first_two_bits == 0b00:
        # 6-bit length (0-63)
        size = first_byte & 0x3F
        return size, pos + 1, False
    elif first_two_bits == 0b01:
        # 14-bit length (0-16383), big-endian
        if pos + 1 >= len(data):
            return None, pos, False
        second_byte = data[pos + 1]
        size = ((first_byte & 0x3F) << 8) | second_byte
        return size, pos + 2, False
    elif first_two_bits == 0b10:
        # 32-bit length (0-4294967295), big-endian
        if pos + 4 >= len(data):
            return None, pos, False
        size = struct.unpack('>I', data[pos + 1:pos + 5])[0]
        return size, pos + 5, False
    else:  # first_two_bits == 0b11
        # Special encoding - return the byte value and indicate it's special
        return first_byte, pos + 1, True


def parse_string_encoding(data, pos):
    """
    Parse string-encoded value from RDB file.
    Returns: (string_value, new_position)
    """
    if pos >= len(data):
        return None, pos
    
    # Parse size encoding
    size_or_special, new_pos, is_special = parse_size_encoding(data, pos)
    
    if size_or_special is None:
        return None, pos
    
    if is_special:
        # Handle special integer encodings
        special_type = size_or_special & 0x3F
        
        if special_type == 0x00:  # 0xC0: 8-bit integer
            if new_pos >= len(data):
                return None, pos
            int_value = data[new_pos]
            return str(int_value), new_pos + 1
        elif special_type == 0x01:  # 0xC1: 16-bit integer, little-endian
            if new_pos + 1 >= len(data):
                return None, pos
            int_value = struct.unpack('<H', data[new_pos:new_pos + 2])[0]
            return str(int_value), new_pos + 2
        elif special_type == 0x02:  # 0xC2: 32-bit integer, little-endian
            if new_pos + 3 >= len(data):
                return None, pos
            int_value = struct.unpack('<I', data[new_pos:new_pos + 4])[0]
            return str(int_value), new_pos + 4
        elif special_type == 0x03:  # 0xC3: LZF compression (not supported)
            return None, pos
        else:
            return None, pos
    else:
        # Regular string: read size bytes
        size = size_or_special
        if new_pos + size > len(data):
            return None, pos
        string_bytes = data[new_pos:new_pos + size]
        try:
            string_value = string_bytes.decode('utf-8')
            return string_value, new_pos + size
        except UnicodeDecodeError:
            return None, pos


def load_rdb_file(filepath):
    """
    Load RDB file and populate global_database.
    Returns: True if successful, False otherwise
    """
    try:
        if not os.path.exists(filepath):
            # File doesn't exist - start with empty database
            return True
        
        with open(filepath, 'rb') as f:
            data = f.read()
        
        if len(data) < 9:
            # File too short to have header
            return False
        
        pos = 0
        
        # Parse header: "REDIS0011" (9 bytes)
        header = data[pos:pos + 9]
        if header != b'REDIS0011':
            return False
        pos += 9
        
        # Skip metadata section (FA followed by string-encoded name/value pairs)
        while pos < len(data):
            if data[pos] == 0xFA:  # Metadata subsection start
                pos += 1
                # Read metadata name (string encoded)
                name, pos = parse_string_encoding(data, pos)
                if name is None:
                    return False
                # Read metadata value (string encoded)
                value, pos = parse_string_encoding(data, pos)
                if value is None:
                    return False
                # Continue to next metadata or database section
            elif data[pos] == 0xFE:  # Database subsection start
                break
            elif data[pos] == 0xFF:  # EOF marker
                # End of file reached before database section
                return True
            else:
                # Unexpected byte - might be database section
                break
        
        # Parse database section
        while pos < len(data):
            if data[pos] == 0xFF:  # EOF marker
                # Skip checksum (8 bytes)
                pos += 9
                break
            
            if data[pos] != 0xFE:  # Database subsection start
                return False
            
            pos += 1
            
            # Read database index (size encoded)
            db_index, pos, _ = parse_size_encoding(data, pos)
            if db_index is None:
                return False
            
            # Read hash table size info (FB)
            if pos >= len(data) or data[pos] != 0xFB:
                return False
            pos += 1
            
            # Read hash table sizes (size encoded, but we don't need them)
            hash_table_size, pos, _ = parse_size_encoding(data, pos)
            if hash_table_size is None:
                return False
            expire_table_size, pos, _ = parse_size_encoding(data, pos)
            if expire_table_size is None:
                return False
            
            # Parse key-value pairs
            for _ in range(hash_table_size):
                if pos >= len(data):
                    break
                
                # Check for expire timestamp
                expiry = None
                if data[pos] == 0xFD:  # Expire in seconds (4-byte unsigned int, little-endian)
                    pos += 1
                    if pos + 3 >= len(data):
                        break
                    expire_seconds = struct.unpack('<I', data[pos:pos + 4])[0]
                    expiry = expire_seconds
                    pos += 4
                elif data[pos] == 0xFC:  # Expire in milliseconds (8-byte unsigned long, little-endian)
                    pos += 1
                    if pos + 7 >= len(data):
                        break
                    expire_milliseconds = struct.unpack('<Q', data[pos:pos + 8])[0]
                    expiry = expire_milliseconds / 1000.0  # Convert to seconds
                    pos += 8
                
                # Read value type (1 byte)
                if pos >= len(data):
                    break
                value_type = data[pos]
                pos += 1
                
                # Only handle string type (0) for now
                if value_type != 0:
                    # Skip unknown types for now
                    continue
                
                # Read key (string encoded)
                key, pos = parse_string_encoding(data, pos)
                if key is None:
                    break
                
                # Read value (string encoded)
                value, pos = parse_string_encoding(data, pos)
                if value is None:
                    break
                
                # Store in global database
                with global_database_lock:
                    global_database[key] = {
                        "type": "string",
                        "value": value,
                        "expiry": expiry
                    }
        
        return True
        
    except Exception as e:
        print(f"Error loading RDB file: {e}")
        return False


def parse_resp(data, pos=0):
    """
    Parse RESP (Redis Serialization Protocol) data.
    Returns: (parsed_value, new_position)
    """
    if pos >= len(data):
        return None, pos
    
    # Parse array: *<number>\r\n
    if data[pos] == ord('*'):
        pos += 1
        # Find the number
        num_end = data.find(b'\r\n', pos)
        if num_end == -1:
            return None, pos
        num = int(data[pos:num_end])
        pos = num_end + 2
        
        # Parse each element in the array
        elements = []
        for _ in range(num):
            element, pos = parse_resp(data, pos)
            if element is None:
                return None, pos
            elements.append(element)
        return elements, pos
    
    # Parse bulk string: $<length>\r\n<data>\r\n
    elif data[pos] == ord('$'):
        pos += 1
        # Find the length
        len_end = data.find(b'\r\n', pos)
        if len_end == -1:
            return None, pos
        length = int(data[pos:len_end])
        pos = len_end + 2
        
        # Handle null bulk string
        if length == -1:
            return None, pos
        
        # Extract the string data
        if pos + length + 2 > len(data):
            return None, pos
        
        string_data = data[pos:pos + length]
        pos += length + 2  # Skip \r\n after data
        return string_data.decode('utf-8'), pos
    
    # Parse simple string: +<string>\r\n
    elif data[pos] == ord('+'):
        pos += 1
        end = data.find(b'\r\n', pos)
        if end == -1:
            return None, pos
        string_data = data[pos:end].decode('utf-8')
        pos = end + 2
        return string_data, pos
    
    # Parse error: -<string>\r\n
    elif data[pos] == ord('-'):
        pos += 1
        end = data.find(b'\r\n', pos)
        if end == -1:
            return None, pos
        error_data = data[pos:end].decode('utf-8')
        pos = end + 2
        return error_data, pos
    
    # Parse integer: :<number>\r\n
    elif data[pos] == ord(':'):
        pos += 1
        end = data.find(b'\r\n', pos)
        if end == -1:
            return None, pos
        int_data = int(data[pos:end])
        pos = end + 2
        return int_data, pos
    
    return None, pos


def parse_command(data):
    """
    Parse a RESP command from received data.
    Returns: (command, arguments) or (None, None) if parsing fails
    """
    try:
        parsed, _ = parse_resp(data)
        if parsed and isinstance(parsed, list) and len(parsed) > 0:
            command = parsed[0].upper() if isinstance(parsed[0], str) else parsed[0]
            arguments = parsed[1:] if len(parsed) > 1 else []
            return command, arguments
        return None, None
    except Exception as e:
        print(f"Error parsing command: {e}")
        return None, None


def parse_entry_id(entry_id, is_start=True, max_seq=None):
    """
    Parse an entry ID, handling optional sequence numbers.
    For start ID: if sequence missing, defaults to 0
    For end ID: if sequence missing, defaults to max_seq (or very large number)
    Returns: (time, seq) tuple
    """

    if entry_id == "-":
        return (0,0)
    
    elif "-" in entry_id:
        parts = entry_id.split("-")
        time_part = int(parts[0])
        if len(parts) > 1 and parts[1]:
            seq_part = int(parts[1])
        else:
            # Sequence number is missing
            if is_start:
                seq_part = 0
            else:
                # For end ID, use max sequence if provided, otherwise use a very large number
                seq_part = max_seq if max_seq is not None else 999999999
        return (time_part, seq_part)
    else:
        # Just a timestamp
        time_part = int(entry_id)
        if is_start:
            return (time_part, 0)
        else:
            return (time_part, max_seq if max_seq is not None else 999999999)


def validate_entry_id(entry_id, stream_last_ids, stream_key, connection):
    # Don't validate if entry_id contains * (it will be auto-generated)
    if "*" in entry_id:
        return True
    
    time, seq_no = entry_id.split("-")
    time = int(time)
    seq_no = int(seq_no)

    if time == 0 and seq_no == 0:
        connection.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
        return False

    if stream_key not in stream_last_ids:
        if time > 0 or (time == 0 and seq_no > 0):
            return True
        else:
            connection.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            return False
    else:
        last_id = stream_last_ids[stream_key]
        last_time, last_seq_no = last_id.split("-")
        last_time = int(last_time)
        last_seq_no = int(last_seq_no)
        if time > last_time or (time == last_time and seq_no > last_seq_no):
            return True
        else:
            connection.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            return False


class BufferConnection:
    """Lightweight connection that captures sendall bytes for transaction replay."""
    def __init__(self):
        self.chunks = []

    def sendall(self, data):
        if isinstance(data, str):
            data = data.encode()
        self.chunks.append(data)

    def get_response(self):
        return b"".join(self.chunks)


def validate_command_syntax(command, arguments):
    """Validate command syntax (argument count, command existence) without executing."""
    # Define expected argument counts for commands
    command_arg_counts = {
        "PING": (0,),
        "ECHO": (1,),
        "SET": (2, 4),  # SET key value or SET key value EX/PX time
        "GET": (1,),
        "INCR": (1,),
        "TYPE": (1,),
        "XADD": None,  # Variable: at least 4, must be even (key, id, field-value pairs)
        "XRANGE": (3,),
        "XREAD": None,  # Variable: complex parsing needed
        "MULTI": (0,),
        "EXEC": (0,),
        "DISCARD": (0,),
        "SUBSCRIBE": (1,),
        "PUBLISH": (2,),
        "ZADD": (3,),  # ZADD key score member
        "ZRANK": (2,),  # ZRANK key member
        "ZRANGE": (3,),  # ZRANGE key start stop
        "ZCARD": (1,),  # ZCARD key
        "ZSCORE": (2,),  # ZSCORE key member
        "ZREM": (2,),  # ZREM key member
        "GEOADD": (4,),  # GEOADD key longitude latitude member
        "GEOPOS": None,  # GEOPOS key member [member ...] - at least 2 args
        "GEODIST": (3,),  # GEODIST key member1 member2
        "GEOSEARCH": None,  # GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit - at least 7 args
    }
    
    if command not in command_arg_counts:
        return False, b"-ERR unknown command\r\n"
    
    expected_counts = command_arg_counts[command]
    
    if expected_counts is None:
        # Variable argument commands - do basic checks
        if command == "XADD":
            if len(arguments) < 4 or len(arguments) % 2 != 0:
                return False, b"-ERR wrong number of arguments for 'xadd' command\r\n"
        elif command == "XREAD":
            # XREAD has complex syntax, minimal check: at least 3 args (BLOCK timeout STREAMS...)
            if len(arguments) < 1:
                return False, b"-ERR wrong number of arguments for 'xread' command\r\n"
        elif command == "GEOPOS":
            if len(arguments) < 2:
                return False, b"-ERR wrong number of arguments for 'geopos' command\r\n"
        elif command == "GEOSEARCH":
            if len(arguments) < 7:
                return False, b"-ERR wrong number of arguments for 'geosearch' command\r\n"
        return True, None
    else:
        # Fixed argument count commands
        if len(arguments) not in expected_counts:
            cmd_name = command.lower()
            return False, f"-ERR wrong number of arguments for '{cmd_name}' command\r\n".encode()
    
    return True, None


def encode_command_as_resp(command, arguments):
    """
    Encode a command and its arguments as a RESP array.
    
    Args:
        command: The command name (e.g., "SET")
        arguments: List of command arguments (e.g., ["foo", "bar"])
    
    Returns:
        bytes: The command encoded as a RESP array
        Example: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    """
    # RESP array format: *<number_of_elements>\r\n<element1><element2>...
    # Each element is a bulk string: $<length>\r\n<data>\r\n
    
    # Convert command to string if it's bytes
    if isinstance(command, bytes):
        command_str = command.decode('utf-8')
    else:
        command_str = str(command)
    
    # Total number of elements: command + arguments
    num_elements = 1 + len(arguments)
    resp_parts = [f"*{num_elements}\r\n".encode('utf-8')]
    
    # Add command as bulk string
    command_bytes = command_str.encode('utf-8')
    resp_parts.append(f"${len(command_bytes)}\r\n".encode('utf-8'))
    resp_parts.append(command_bytes)
    resp_parts.append(b"\r\n")
    
    # Add each argument as bulk string
    for arg in arguments:
        # Convert argument to string if it's bytes
        if isinstance(arg, bytes):
            arg_str = arg.decode('utf-8')
        else:
            arg_str = str(arg)
        arg_bytes = arg_str.encode('utf-8')
        resp_parts.append(f"${len(arg_bytes)}\r\n".encode('utf-8'))
        resp_parts.append(arg_bytes)
        resp_parts.append(b"\r\n")
    
    return b"".join(resp_parts)


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
        global master_repl_offset
        master_repl_offset += command_bytes
    
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


def execute_single_command(connection, command, arguments, Database, stream_last_ids, subscribed_channels=None, in_subscribed_mode=None):
    """Execute a single command against Database, writing responses to the provided connection."""
    if command == "PING":
        if not in_subscribed_mode[0]:
            connection.sendall(b"+PONG\r\n")
        else:
            connection.sendall(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")

    elif command == "ECHO":
        if arguments and len(arguments) > 0:
            message = arguments[0]
            response = f"${len(message)}\r\n{message}\r\n"
            connection.sendall(response.encode())
        else:
            connection.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")

    elif command == "SET":
        if len(arguments) < 2:
            connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
        elif len(arguments) == 2:
            # SET key value (no expiry)
            key = arguments[0]
            value = arguments[1]
            Database[key] = {"type": "string", "value": value, "expiry": None}
            connection.sendall(b"+OK\r\n")
            
            # Propagate SET command to replicas (only if this is not a replica connection)
            # Replicas send commands to master, but we don't propagate those back
            if not is_replica_connection(connection):
                propagate_command_to_replicas(command, arguments)
        elif len(arguments) == 4:
            # SET key value EX seconds or SET key value PX milliseconds
            key = arguments[0]
            value = arguments[1]
            expiry_type = arguments[2].upper()
            try:
                expiry_time = int(arguments[3])
            except ValueError:
                connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                return

            if expiry_type == "EX":
                # Expiry in seconds
                expiry_timestamp = time.time() + expiry_time
                Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
                connection.sendall(b"+OK\r\n")
                
                # Propagate SET command to replicas
                if not is_replica_connection(connection):
                    propagate_command_to_replicas(command, arguments)
            elif expiry_type == "PX":
                # Expiry in milliseconds
                expiry_timestamp = time.time() + (expiry_time / 1000.0)
                Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
                connection.sendall(b"+OK\r\n")
                
                # Propagate SET command to replicas
                if not is_replica_connection(connection):
                    propagate_command_to_replicas(command, arguments)
            else:
                connection.sendall(b"-ERR syntax error\r\n")
        else:
            connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")

    elif command == "GET":
        if len(arguments) != 1:
            connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
            return
        key = arguments[0]
        if key not in Database:
            connection.sendall(b"$-1\r\n")
        else:
            entry = Database[key]

            # Check expiry only for strings
            if entry["type"] == "string":
                if entry.get("expiry") is not None and time.time() > entry["expiry"]:
                    del Database[key]
                    connection.sendall(b"$-1\r\n")
                else:
                    value = entry["value"]
                    msg = f"${len(value)}\r\n{value}\r\n"
                    connection.sendall(msg.encode())
            else:
                # Stream or other types - GET only works on strings
                connection.sendall(b"$-1\r\n")

    elif command == "XADD":
        # XADD stream_key entry_id field1 value1 field2 value2 ...
        # Minimum: stream_key and entry_id (2 args), plus at least one field-value pair (2 more args) = 4 args minimum
        if len(arguments) < 4 or len(arguments) % 2 != 0:
            connection.sendall(b"-ERR wrong number of arguments for 'xadd' command\r\n")
        else:
            stream_key = arguments[0]
            entry_id_input = arguments[1]  # Original input (might contain *)

            # Check if sequence number needs to be auto-generated
            if entry_id_input.endswith("-*"):
                time_part_str = entry_id_input.split("-")[0]
                try:
                    time_part_int = int(time_part_str)
                except ValueError:
                    connection.sendall(b"-ERR Invalid ID format\r\n")
                    return

                # Find the last entry ID for this stream with the same time part
                if stream_key in Database and Database[stream_key]["type"] == "stream":
                    entries = Database[stream_key]["entries"]
                    if entries:
                        # Find the last entry with the same time part
                        last_seq = -1
                        for entry in entries:
                            entry_id = entry["id"]
                            entry_time_str, entry_seq_str = entry_id.split("-")
                            entry_time = int(entry_time_str)
                            entry_seq = int(entry_seq_str)

                            if entry_time == time_part_int and entry_seq > last_seq:
                                last_seq = entry_seq

                        if last_seq >= 0:
                            # Found entries with same time part, increment
                            seq_no = last_seq + 1
                        else:
                            # No entries with this time part
                            if time_part_int == 0:
                                seq_no = 1  # Special case: time 0 starts at 1
                            else:
                                seq_no = 0
                    else:
                        # Stream is empty
                        if time_part_int == 0:
                            seq_no = 1  # Special case: time 0 starts at 1
                        else:
                            seq_no = 0
                else:
                    # Stream doesn't exist yet
                    if time_part_int == 0:
                        seq_no = 1  # Special case: time 0 starts at 1
                    else:
                        seq_no = 0

                entry_id = f"{time_part_int}-{seq_no}"

            elif entry_id_input == "*":
                entry_id_time = int(time.time()*1000)
                if stream_key in Database and Database[stream_key]["type"] == "stream":
                    entries = Database[stream_key]["entries"]
                    if entries:
                        last_seq = -1
                        for entry in entries:
                            entry_id = entry["id"]
                            entry_time_str, entry_seq_str = entry_id.split("-")
                            entry_time = int(entry_time_str)
                            entry_seq = int(entry_seq_str)
                            if entry_time == entry_id_time and entry_seq > last_seq:
                                last_seq = entry_seq
                        if last_seq >= 0:
                            seq_no = last_seq + 1
                        else:
                            seq_no = 0
                    else:
                        seq_no = 0
                else:
                    seq_no = 0
                entry_id = f"{entry_id_time}-{seq_no}"

            else:
                # Explicit ID provided
                entry_id = entry_id_input
                if not validate_entry_id(entry_id, stream_last_ids, stream_key, connection):
                    return

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
            if entry["type"] != "stream":
                connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
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

                # Return entry ID as bulk string
                response = f"${len(entry_id)}\r\n{entry_id}\r\n"
                connection.sendall(response.encode())
                
                # Propagate XADD command to replicas (only if this is not a replica connection)
                # Note: We propagate with the final entry_id (not the original input with *)
                if not is_replica_connection(connection):
                    # Create arguments with the final entry_id for propagation
                    xadd_args = [stream_key, entry_id] + arguments[2:]
                    propagate_command_to_replicas(command, xadd_args)

    elif command == "TYPE":
        if len(arguments) != 1:
            connection.sendall(b"-ERR wrong number of arguments for 'type' command\r\n")
        else:
            key = arguments[0]
            if key not in Database:
                connection.sendall(b"+none\r\n")
            else:
                entry = Database[key]

                # Check expiry for strings
                if entry["type"] == "string" and entry["expiry"] is not None and time.time() > entry["expiry"]:
                    del Database[key]
                    connection.sendall(b"+none\r\n")
                else:
                    # Return the type
                    if entry["type"] == "string":
                        connection.sendall(b"+string\r\n")
                    elif entry["type"] == "stream":
                        connection.sendall(b"+stream\r\n")
                    elif entry["type"] == "zset":
                        connection.sendall(b"+zset\r\n")
                    else:
                        connection.sendall(b"+none\r\n")

    elif command == "XRANGE":
        if len(arguments) != 3:
            connection.sendall(b"-ERR wrong number of arguments for 'xrange' command\r\n")
        else:
            stream_key = arguments[0]
            start_id = arguments[1]
            end_id = arguments[2]

            if stream_key not in Database or Database[stream_key]["type"] != "stream":
                connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                entries = Database[stream_key]["entries"]
                if end_id == "+":
                    end_time = 999999999
                    end_seq = 999999999
                    end_id = f"{end_time}-{end_seq}"
                end_time_str = end_id.split("-")[0] if "-" in end_id else end_id
                try:
                    end_time_for_max = int(end_time_str)
                except ValueError:
                    connection.sendall(b"-ERR Invalid ID format\r\n")
                    return

                max_seq = None
                if entries:
                    for entry in entries:
                        entry_id = entry["id"]
                        entry_time_str, entry_seq_str = entry_id.split("-")
                        entry_time = int(entry_time_str)
                        entry_seq = int(entry_seq_str)
                        if entry_time == end_time_for_max:
                            if max_seq is None or entry_seq > max_seq:
                                max_seq = entry_seq

                start_time, start_seq = parse_entry_id(start_id, is_start=True)
                end_time, end_seq = parse_entry_id(end_id, is_start=False, max_seq=max_seq)

                result_entries = []
                for entry in entries:
                    entry_id = entry["id"]
                    entry_time_str, entry_seq_str = entry_id.split("-")
                    entry_time = int(entry_time_str)
                    entry_seq = int(entry_seq_str)

                    entry_after_start = (entry_time > start_time) or \
                                      (entry_time == start_time and entry_seq >= start_seq)

                    entry_before_end = (entry_time < end_time) or \
                                     (entry_time == end_time and entry_seq <= end_seq)

                    if not entry_after_start:
                        continue
                    elif not entry_before_end:
                        break
                    else:
                        result_entries.append(entry)

                response_parts = []
                response_parts.append(f"*{len(result_entries)}\r\n")

                for entry in result_entries:
                    entry_id = entry["id"]
                    fields = entry["fields"]

                    response_parts.append("*2\r\n")
                    response_parts.append(f"${len(entry_id)}\r\n{entry_id}\r\n")

                    field_count = len(fields) * 2
                    response_parts.append(f"*{field_count}\r\n")

                    for field, value in fields.items():
                        response_parts.append(f"${len(field)}\r\n{field}\r\n")
                        response_parts.append(f"${len(value)}\r\n{value}\r\n")

                response = "".join(response_parts)
                connection.sendall(response.encode())

    elif command == "XREAD":
        # XREAD [BLOCK <milliseconds>] STREAMS <key1> <key2> ... <id1> <id2> ...
        # Format: XREAD [BLOCK timeout] STREAMS stream_key1 stream_key2 ... entry_id1 entry_id2 ...

        # Parse BLOCK parameter if present
        block_timeout = None
        streams_index = 0

        if len(arguments) >= 2 and arguments[0].upper() == "BLOCK":
            try:
                block_timeout = int(arguments[1])
                streams_index = 2
            except ValueError:
                connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                return

        # Find STREAMS keyword
        streams_found = False
        for i in range(streams_index, len(arguments)):
            if arguments[i].upper() == "STREAMS":
                streams_index = i
                streams_found = True
                break

        if not streams_found or len(arguments) < streams_index + 3 or (len(arguments) - streams_index - 1) % 2 != 0:
            connection.sendall(b"-ERR wrong number of arguments for 'xread' command\r\n")
        else:
            # Parse key-id pairs
            # After "STREAMS", we have: key1, key2, ..., id1, id2, ...
            num_streams = (len(arguments) - streams_index - 1) // 2
            key_id_pairs = []
            for i in range(num_streams):
                key_id_pairs.append({
                    "key": arguments[streams_index + 1 + i],
                    "id": arguments[streams_index + 1 + num_streams + i]
                })

            # Resolve $ to actual max entry IDs before processing
            resolved_key_id_pairs = []
            for key_id_pair in key_id_pairs:
                stream_key = key_id_pair["key"]
                start_id = key_id_pair["id"]

                # Resolve $ to the maximum entry ID at this moment
                if start_id == "$":
                    if stream_key not in Database or Database[stream_key]["type"] != "stream":
                        # Stream doesn't exist, skip it
                        continue

                    entries = Database[stream_key]["entries"]
                    if len(entries) == 0:
                        # Empty stream, use 0-0 as the ID (will wait for any new entry)
                        resolved_id = "0-0"
                    else:
                        # Find maximum entry ID
                        max_time = -1
                        max_seq = -1
                        for entry in entries:
                            entry_id = entry["id"]
                            entry_time_str, entry_seq_str = entry_id.split("-")
                            entry_time = int(entry_time_str)
                            entry_seq = int(entry_seq_str)
                            if entry_time > max_time or (entry_time == max_time and entry_seq > max_seq):
                                max_time = entry_time
                                max_seq = entry_seq
                        resolved_id = f"{max_time}-{max_seq}"

                    resolved_key_id_pairs.append({
                        "key": stream_key,
                        "id": resolved_id
                    })
                else:
                    # Keep original ID
                    resolved_key_id_pairs.append(key_id_pair)

            # Use resolved IDs from now on
            key_id_pairs = resolved_key_id_pairs

            # Helper function to get entries for streams
            def get_entries_for_streams(key_id_pairs, Database):
                streams_with_entries = []
                for key_id_pair in key_id_pairs:
                    stream_key = key_id_pair["key"]
                    start_id = key_id_pair["id"]

                    if stream_key not in Database or Database[stream_key]["type"] != "stream":
                        # Stream doesn't exist, skip it
                        continue

                    entries = Database[stream_key]["entries"]

                    # Parse start ID (should be resolved now, no $)
                    try:
                        if "-" not in start_id:
                            continue
                        start_time_str, start_seq_str = start_id.split("-")
                        start_time = int(start_time_str)
                        start_seq = int(start_seq_str)
                    except (ValueError, IndexError):
                        continue

                    # XREAD is exclusive - get entries with ID > start_id
                    result_entries = []
                    for entry in entries:
                        entry_id = entry["id"]
                        entry_time_str, entry_seq_str = entry_id.split("-")
                        entry_time = int(entry_time_str)
                        entry_seq = int(entry_seq_str)

                        # Check if entry ID is greater than start_id (exclusive)
                        entry_greater = (entry_time > start_time) or \
                                       (entry_time == start_time and entry_seq > start_seq)

                        if entry_greater:
                            result_entries.append(entry)

                    # Only include streams that have matching entries
                    if len(result_entries) > 0:
                        streams_with_entries.append({
                            "key": stream_key,
                            "entries": result_entries
                        })
                return streams_with_entries

            # Check for immediate results
            streams_with_entries = get_entries_for_streams(key_id_pairs, Database)

            # If we have results or not blocking, return immediately
            if len(streams_with_entries) > 0 or block_timeout is None:
                # Build and send response
                response_parts = []

                if len(streams_with_entries) == 0:
                    # No streams with entries, return empty array
                    connection.sendall(b"*0\r\n")
                else:
                    # Outer array: number of streams
                    response_parts.append(f"*{len(streams_with_entries)}\r\n")

                    # Process each stream
                    for stream_data in streams_with_entries:
                        stream_key = stream_data["key"]
                        result_entries = stream_data["entries"]

                        # Stream array: [key, entries]
                        response_parts.append("*2\r\n")

                        # Stream key
                        response_parts.append(f"${len(stream_key)}\r\n{stream_key}\r\n")

                        # Entries array
                        response_parts.append(f"*{len(result_entries)}\r\n")

                        # Each entry: [id, [field1, value1, ...]]
                        for entry in result_entries:
                            entry_id = entry["id"]
                            fields = entry["fields"]

                            # Entry array has 2 elements
                            response_parts.append("*2\r\n")

                            # Entry ID
                            response_parts.append(f"${len(entry_id)}\r\n{entry_id}\r\n")

                            # Field-value pairs array
                            field_count = len(fields) * 2
                            response_parts.append(f"*{field_count}\r\n")

                            # Add fields in order
                            for field, value in fields.items():
                                response_parts.append(f"${len(field)}\r\n{field}\r\n")
                                response_parts.append(f"${len(value)}\r\n{value}\r\n")

                    # Send response once after processing all streams
                    response = "".join(response_parts)
                    connection.sendall(response.encode())
            else:
                # Blocking mode: wait for new entries
                # Get or create condition for each stream
                conditions = []
                for key_id_pair in key_id_pairs:
                    stream_key = key_id_pair["key"]
                    with stream_conditions_lock:
                        if stream_key not in stream_conditions:
                            stream_conditions[stream_key] = threading.Condition()
                        conditions.append((stream_key, stream_conditions[stream_key]))

                # Wait on conditions with timeout
                # Handle block_timeout == 0 as infinite wait
                if block_timeout == 0:
                    timeout_seconds = float('inf')  # Wait indefinitely
                else:
                    timeout_seconds = block_timeout / 1000.0

                start_time = time.time()

                # Use the first condition for waiting (or wait on all)
                if conditions:
                    condition = conditions[0][1]
                    condition.acquire()
                    try:
                        # Wait with timeout, checking for new entries when notified
                        while True:
                            elapsed = time.time() - start_time

                            # Handle timeout (but 0 means infinite wait)
                            if timeout_seconds != float('inf'):
                                remaining = timeout_seconds - elapsed
                                if remaining <= 0:
                                    # Timeout expired
                                    connection.sendall(b"*-1\r\n")
                                    break
                                wait_time = min(remaining, 0.1)
                            else:
                                # Infinite wait, check every 100ms
                                wait_time = 0.1

                            # Wait with timeout (will be notified when XADD adds entry)
                            condition.wait(wait_time)

                            # Check if new entries are available
                            streams_with_entries = get_entries_for_streams(key_id_pairs, Database)
                            if len(streams_with_entries) > 0:
                                # New entries available, build and send response
                                response_parts = []
                                response_parts.append(f"*{len(streams_with_entries)}\r\n")

                                for stream_data in streams_with_entries:
                                    stream_key = stream_data["key"]
                                    result_entries = stream_data["entries"]

                                    response_parts.append("*2\r\n")
                                    response_parts.append(f"${len(stream_key)}\r\n{stream_key}\r\n")
                                    response_parts.append(f"*{len(result_entries)}\r\n")

                                    for entry in result_entries:
                                        entry_id = entry["id"]
                                        fields = entry["fields"]

                                        response_parts.append("*2\r\n")
                                        response_parts.append(f"${len(entry_id)}\r\n{entry_id}\r\n")

                                        field_count = len(fields) * 2
                                        response_parts.append(f"*{field_count}\r\n")

                                        for field, value in fields.items():
                                            response_parts.append(f"${len(field)}\r\n{field}\r\n")
                                            response_parts.append(f"${len(value)}\r\n{value}\r\n")

                                response = "".join(response_parts)
                                connection.sendall(response.encode())
                                break
                    finally:
                        condition.release()
                else:
                    # No valid streams, return null
                    connection.sendall(b"*-1\r\n")

    elif command == "INCR":
        if len(arguments) != 1:
            connection.sendall(b"-ERR wrong number of arguments for 'incr' command\r\n")
        else:
            key = arguments[0]

            if key not in Database:
                Database[key] = {"type": "string", "value": "0", "expiry": None}
                new_value = 1
            else:
                entry = Database[key]

                # Check if it's a string type
                if entry["type"] != "string":
                    connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                    return

                # Check expiry
                if entry.get("expiry") is not None and time.time() > entry["expiry"]:
                    # Key expired, treat as if it doesn't exist
                    del Database[key]
                    Database[key] = {"type": "string", "value": "0", "expiry": None}
                    new_value = 0
                else:
                    # Key exists - try to convert value to integer
                    try:
                        current_value = int(entry["value"])
                        new_value = current_value + 1
                    except ValueError:
                        # Value cannot be converted to integer (e.g., "xyz")
                        connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                        return

            # Store the new value as a string
            Database[key]["value"] = str(new_value)

            # Return the new value as integer RESP format
            connection.sendall(f":{new_value}\r\n".encode())
            
            # Propagate INCR command to replicas (only if this is not a replica connection)
            if not is_replica_connection(connection):
                propagate_command_to_replicas(command, arguments)

    elif command == "INFO":
        #if server is master that is running on port 6379 then return role as  master else return role as slave
        server_port = connection.getsockname()[1]
        if server_port == 6379:
            # Master response with role, master_replid, and master_repl_offset
            info_text = "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0"
        else:
            info_text = "role:slave"
        
        # Return as bulk string: $<length>\r\n<data>\r\n
        response = f"${len(info_text)}\r\n{info_text}\r\n"
        connection.sendall(response.encode())

    elif command == "REPLCONF":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'replconf' command\r\n")
        else:
            arg0 = str(arguments[0]).upper()
            arg1 = str(arguments[1])
            # Handle REPLCONF ACK <offset> from replicas without responding
            if arg0 == "ACK" and is_replica_connection(connection):
                try:
                    offset_value = int(arg1)
                except ValueError:
                    return
                with replica_offset_map_lock:
                    replica_offset_map[connection] = offset_value
                return
            # For other REPLCONF commands, respond OK
            connection.sendall(b"+OK\r\n")

    elif command == "PSYNC":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'psync' command\r\n")
        else:
            repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            response = f"+FULLRESYNC {repl_id} 0\r\n"
            connection.sendall(response.encode())
            
            # Send empty RDB file: $<length>\r\n<binary_contents>
            # Note: This is NOT a RESP bulk string - no trailing \r\n
            rdb_length = len(EMPTY_RDB_BINARY)
            rdb_header = f"${rdb_length}\r\n".encode()
            connection.sendall(rdb_header + EMPTY_RDB_BINARY)
            
            # Mark this connection as a replica connection
            # After the handshake is complete, this connection will receive propagated commands
            with replica_connections_lock:
                replica_connections.add(connection)
            # Initialize socket lock for this replica
            with replica_socket_locks_lock:
                replica_socket_locks[connection] = threading.Lock()
            # Initialize replica offset to 0
            with replica_offset_map_lock:
                replica_offset_map[connection] = 0

    elif command == "WAIT":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'wait' command\r\n")
        else:
            try:
                numreplicas = int(arguments[0])
                timeout_ms = int(arguments[1])
            except ValueError:
                connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                return
            
            # Get list of replica connections
            with replica_connections_lock:
                replicas = list(replica_connections)
            
            if numreplicas == 0:
                # Don't need any replicas, return 0 immediately
                response = ":0\r\n"
                connection.sendall(response.encode())
                return
            
            # Get master replication offset
            with master_repl_offset_lock:
                current_master_offset = master_repl_offset
            
            # If no write commands have been sent, all replicas have "acknowledged" everything
            # Return the number of connected replicas
            if current_master_offset == 0:
                num_connected = len(replicas)
                response = f":{num_connected}\r\n"
                connection.sendall(response.encode())
                return
            
            # Send REPLCONF GETACK * to all replicas
            for replica_conn in replicas:
                sent = send_getack_to_replica(replica_conn)
                if not sent:
                    # Remove broken replica connections
                    with replica_connections_lock:
                        replica_connections.discard(replica_conn)
                    with replica_offset_map_lock:
                        replica_offset_map.pop(replica_conn, None)
                    with replica_socket_locks_lock:
                        replica_socket_locks.pop(replica_conn, None)
            
            # Wait until we have enough acknowledgements or timeout expires
            start_time = time.time()
            timeout_seconds = timeout_ms / 1000.0
            check_interval = 0.01  # Check every 10ms
            
            while True:
                elapsed = time.time() - start_time
                remaining_time = timeout_seconds - elapsed
                
                # Check if timeout expired
                if remaining_time <= 0:
                    break
                
                # Count how many replicas have acknowledged all previous write commands
                acknowledged_count = 0
                with replica_offset_map_lock:
                    for replica_conn, offset in replica_offset_map.items():
                        if offset >= current_master_offset:
                            acknowledged_count += 1
                
                # If we have enough acknowledgements, return early
                if acknowledged_count >= numreplicas:
                    break
                
                # Wait a short time before checking again
                time.sleep(min(check_interval, remaining_time))
            
            # Final count of acknowledged replicas
            acknowledged_count = 0
            with replica_offset_map_lock:
                for replica_conn, offset in replica_offset_map.items():
                    if offset >= current_master_offset:
                        acknowledged_count += 1
            
            # Return the number of replicas that acknowledged all previous write commands
            response = f":{acknowledged_count}\r\n"
            connection.sendall(response.encode())

    elif command == "KEYS":
        if len(arguments) != 1:
            connection.sendall(b"-ERR wrong number of arguments for 'keys' command\r\n")
        else:
            pattern = arguments[0]
            # Only support "*" pattern for now
            if pattern != "*":
                connection.sendall(b"-ERR pattern not supported\r\n")
            else:
                # Get all keys from database
                with global_database_lock:
                    keys = list(Database.keys())
                
                # Return as RESP array: *<count>\r\n$<key_len>\r\n<key>\r\n...
                response_parts = [f"*{len(keys)}\r\n".encode()]
                for key in keys:
                    key_bytes = key.encode('utf-8')
                    response_parts.append(f"${len(key_bytes)}\r\n{key}\r\n".encode())
                
                connection.sendall(b"".join(response_parts))

    elif command == "CONFIG":
        if len(arguments) < 1:
            connection.sendall(b"-ERR wrong number of arguments for 'config' command\r\n")
        else:
            subcommand = str(arguments[0]).upper()
            if subcommand == "GET":
                if len(arguments) != 2:
                    connection.sendall(b"-ERR wrong number of arguments for 'config get' command\r\n")
                else:
                    # Get the parameter name
                    param_name = str(arguments[1]).lower()
                    
                    # Look up the parameter value
                    param_value = None
                    if param_name == "dir":
                        param_value = rdb_dir
                    elif param_name == "dbfilename":
                        param_value = rdb_dbfilename
                    
                    # Return RESP array with parameter name and value
                    if param_value is not None:
                        # Format: *2\r\n$<name_len>\r\n<name>\r\n$<value_len>\r\n<value>\r\n
                        name_bytes = param_name.encode('utf-8')
                        value_bytes = str(param_value).encode('utf-8')
                        response = f"*2\r\n${len(name_bytes)}\r\n{param_name}\r\n${len(value_bytes)}\r\n{param_value}\r\n"
                        connection.sendall(response.encode())
                    else:
                        # Unknown parameter - return empty array
                        connection.sendall(b"*0\r\n")
            else:
                connection.sendall(b"-ERR unknown subcommand or wrong number of arguments for 'config' command\r\n")

    elif command == "SUBSCRIBE":
        if len(arguments) != 1:
            connection.sendall(b"-ERR wrong number of arguments for 'subscribe' command\r\n")
        else:
            channel = arguments[0]
            if subscribed_channels is None:
                subscribed_channels = set()
            subscribed_channels.add(channel)
            
            # Add connection to global channel subscribers map
            with channel_subscribers_lock:
                if channel not in channel_subscribers:
                    channel_subscribers[channel] = set()
                channel_subscribers[channel].add(connection)
            
            channel_bytes = channel.encode('utf-8')
            count = len(subscribed_channels)
            response = f"*3\r\n$9\r\nsubscribe\r\n${len(channel_bytes)}\r\n{channel}\r\n:{count}\r\n"
            connection.sendall(response.encode())
            # Set subscribed mode flag
            if in_subscribed_mode is not None:
                in_subscribed_mode[0] = True

    elif command == "PUBLISH":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'publish' command\r\n")
        else:
            channel = arguments[0]
            message = arguments[1]  # Not used yet, but required for command syntax
            
            # Count subscribers for this channel
            subscribers = []
            with channel_subscribers_lock:
                if channel in channel_subscribers:
                    subscribers = list(channel_subscribers[channel])
                    subscriber_count = len(channel_subscribers[channel])
                else:
                    subscriber_count = 0

            channel_bytes = channel.encode('utf-8')
            message_bytes = message.encode('utf-8')
            pub_message = f"*3\r\n$7\r\nmessage\r\n${len(channel_bytes)}\r\n{channel}\r\n${len(message_bytes)}\r\n{message}\r\n"
            pub_message_bytes = pub_message.encode('utf-8')

            for subscriber_conns in subscribers:
                try:
                    subscriber_conns.sendall(pub_message_bytes)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    with channel_subscribers_lock:
                        if channel in channel_subscribers:
                            channel_subscribers[channel].discard(subscriber_conns)
                            if len(channel_subscribers[channel]) == 0:
                                del channel_subscribers[channel]
            
            # Return subscriber count as RESP integer
            response = f":{subscriber_count}\r\n"
            connection.sendall(response.encode())
    
    elif command == "UNSUBSCRIBE":
        if len(arguments)!=1:
            connection.sendall(b"-ERR wrong number of arguments for 'unsubscribe' command\r\n")
        else:
            channel = arguments[0]
            with channel_subscribers_lock:
                if channel in channel_subscribers:
                    channel_subscribers[channel].discard(connection)
                    if len(channel_subscribers[channel]) == 0:
                        del channel_subscribers[channel]
            channel_bytes = channel.encode('utf-8')
            count = len(channel_subscribers[channel])
            response = f"*3\r\n$11\r\nunsubscribe\r\n${len(channel_bytes)}\r\n{channel}\r\n:{count}\r\n"
            connection.sendall(response.encode())
            if count == 0:
                in_subscribed_mode[0] = False

    elif command == "ZADD":
        if len(arguments) != 3:
            connection.sendall(b"-ERR wrong number of arguments for 'zadd' command\r\n")
        else:
            key = arguments[0]
            try:
                score = float(arguments[1])  # 64-bit floating point
            except ValueError:
                connection.sendall(b"-ERR value is not a valid float\r\n")
                return
            member = arguments[2]
            
            # Create sorted set if it doesn't exist
            if key not in Database:
                # Create new sorted set with the member
                # Store as list of (score, member) tuples, sorted by score
                Database[key] = {"type": "zset", "members": [(score, member)]}
                new_members_count = 1
            else:
                entry = Database[key]
                
                # Check if it's actually a sorted set
                if entry["type"] != "zset":
                    connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                    return
                
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
                    new_members_count = 1
                else:
                    # Member already exists, so no new members added
                    new_members_count = 0
            
            # Return the number of new members added as RESP integer
            response = f":{new_members_count}\r\n"
            connection.sendall(response.encode())
            
            # Propagate ZADD command to replicas (only if this is not a replica connection)
            if not is_replica_connection(connection):
                propagate_command_to_replicas(command, arguments)

    elif command == "ZRANK":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'zrank' command\r\n")
        else:
            key = arguments[0]
            member = arguments[1]
            if key not in Database:
                connection.sendall(b"$-1\r\n")
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b"$-1\r\n")
                else:
                    members = entry["members"]  # Already sorted by (score, member)
                    rank = None
                    for i, (_, m) in enumerate(members):
                        if m == member:
                            rank = i
                            break
                    if rank is None:
                        connection.sendall(b"$-1\r\n")
                    else:
                        response = f":{rank}\r\n"
                        connection.sendall(response.encode())

    elif command == "ZRANGE":
        if len(arguments) != 3:
            connection.sendall(b"-ERR wrong number of arguments for 'zrange' command\r\n")
        else:
            key = arguments[0]
            start = int(arguments[1])
            stop = int(arguments[2])
            if key not in Database:
                connection.sendall(b"*0\r\n")
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b"*0\r\n")
                else:
                    members = entry["members"]  # Already sorted by (score, member)
                    if start < 0:
                        start = max(0, len(members) + start)
                    if stop < 0:
                        stop = max(0, len(members) + stop)
                    range_members = members[start:stop+1]
                    response = f"*{len(range_members)}\r\n"
                    for score, member in range_members:
                        response += f"${len(member)}\r\n{member}\r\n"
                    connection.sendall(response.encode())
    
    elif command == "ZCARD":
        if len(arguments) != 1:
            connection.sendall(b"-ERR wrong number of arguments for 'zcard' command\r\n")
        else:
            key = arguments[0]
            if key not in Database:
                connection.sendall(b":0\r\n")
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b":0\r\n")
                else:
                    response = f":{len(entry['members'])}\r\n"
                    connection.sendall(response.encode())
    
    elif command == "ZSCORE":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'zscore' command\r\n")
        else:
            key = arguments[0]
            member = arguments[1]
            if key not in Database:
                connection.sendall(b"$-1\r\n")
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b"$-1\r\n")
                else:
                    members = entry["members"]
                    for score, m in members:
                        if m == member:
                            response = f"${len(str(score))}\r\n{str(score)}\r\n"
                            connection.sendall(response.encode())
                            break
                    else:
                        connection.sendall(b"$-1\r\n")
    
    elif command == "ZREM":
        if len(arguments) != 2:
            connection.sendall(b"-ERR wrong number of arguments for 'zrem' command\r\n")
        else:
            key = arguments[0]
            member = arguments[1]
            if key not in Database:
                connection.sendall(b":0\r\n")
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b":0\r\n")
                else:
                    members = entry["members"]
                    removed_count = 0
                    for i, (score, m) in enumerate(members):
                        if m == member:
                            members.pop(i)
                            removed_count += 1
                    response = f":{removed_count}\r\n"
                    connection.sendall(response.encode())
                    if removed_count > 0:
                        Database[key]["members"] = members
                        if len(members) == 0:
                            del Database[key]

    elif command == "GEOADD":
        if len(arguments) != 4:
            connection.sendall(b"-ERR wrong number of arguments for 'geoadd' command\r\n")
        else:
            try:
                longitude = float(arguments[1])
                latitude = float(arguments[2])
            except (ValueError, TypeError):
                connection.sendall(b"-ERR invalid longitude,latitude pair\r\n")
                return
            # Valid longitude: -180 to +180 (inclusive). Valid latitude: -85.05112878 to +85.05112878 (inclusive)
            LONGITUDE_MIN, LONGITUDE_MAX = -180.0, 180.0
            LATITUDE_MIN, LATITUDE_MAX = -85.05112878, 85.05112878
            lon_ok = LONGITUDE_MIN <= longitude <= LONGITUDE_MAX
            lat_ok = LATITUDE_MIN <= latitude <= LATITUDE_MAX
            if not lon_ok or not lat_ok:
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
                    new_members_count = 1
                else:
                    # Member already exists, so no new members added
                    new_members_count = 0
                response = f":{new_members_count}\r\n"
                connection.sendall(response.encode())
            if not is_replica_connection(connection):
                propagate_command_to_replicas(command, arguments)

    elif command == "GEOPOS":
        if len(arguments) < 2:
            connection.sendall(b"-ERR wrong number of arguments for 'geopos' command\r\n")
        else:
            key = arguments[0]
            members_to_lookup = arguments[1:]
            n = len(members_to_lookup)
            if key not in Database:
                # Return array of n null arrays (spec: *-1\r\n per missing element)
                response = f"*{n}\r\n" + (n * "*-1\r\n")
                connection.sendall(response.encode())
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    response = f"*{n}\r\n" + (n * "*-1\r\n")
                    connection.sendall(response.encode())
                else:
                    # Normalize member names to str for lookup (avoid bytes/str mismatch)
                    zset_members = {}
                    for score, m in entry["members"]:
                        k = m if isinstance(m, str) else m.decode("utf-8")
                        zset_members[k] = score
                    parts = [f"*{n}\r\n"]
                    for member in members_to_lookup:
                        m = member if isinstance(member, str) else member.decode("utf-8")
                        if m in zset_members:
                            score_val = zset_members[m]
                            latitude, longitude = decode(int(score_val))
                            # GEOPOS returns [longitude, latitude] per Redis
                            lon_str = str(longitude)
                            lat_str = str(latitude)
                            parts.append(f"*2\r\n${len(lon_str)}\r\n{lon_str}\r\n${len(lat_str)}\r\n{lat_str}\r\n")
                        else:
                            # Missing location: null array (*-1\r\n) per spec
                            parts.append("*-1\r\n")
                    connection.sendall("".join(parts).encode())

    elif command == "GEODIST":
        if len(arguments) != 3:
            connection.sendall(b"-ERR wrong number of arguments for 'geodist' command\r\n")
        else:
            key = arguments[0]
            member1 = arguments[1]
            member2 = arguments[2]
            if key not in Database:
                connection.sendall(b"$-1\r\n")
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b"$-1\r\n")
                else:
                    zset_members = {}
                    for score, m in entry["members"]:
                        k = m if isinstance(m, str) else m.decode("utf-8")
                        zset_members[k] = score
                    m1 = member1 if isinstance(member1, str) else member1.decode("utf-8")
                    m2 = member2 if isinstance(member2, str) else member2.decode("utf-8")
                    if m1 not in zset_members or m2 not in zset_members:
                        connection.sendall(b"$-1\r\n")
                    else:
                        lat1, lon1 = decode(int(zset_members[m1]))
                        lat2, lon2 = decode(int(zset_members[m2]))
                        # Haversine formula; Earth radius in meters (Redis value)
                        R = 6372797.560856
                        lat1_r = math.radians(lat1)
                        lon1_r = math.radians(lon1)
                        lat2_r = math.radians(lat2)
                        lon2_r = math.radians(lon2)
                        dlat = lat2_r - lat1_r
                        dlon = lon2_r - lon1_r
                        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
                        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                        distance = R * c
                        dist_str = f"{round(distance, 4)}"
                        connection.sendall(f"${len(dist_str)}\r\n{dist_str}\r\n".encode())

    elif command == "GEOSEARCH":
        if len(arguments) < 7:
            connection.sendall(b"-ERR wrong number of arguments for 'geosearch' command\r\n")
        else:
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
            else:
                entry = Database[key]
                if entry["type"] != "zset":
                    connection.sendall(b"*0\r\n")
                else:
                    R = 6372797.560856
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
                        dist = R * c
                        if dist <= radius_m:
                            m = member if isinstance(member, str) else member.decode("utf-8")
                            results.append(m)
                    response = f"*{len(results)}\r\n"
                    for m in results:
                        response += f"${len(m)}\r\n{m}\r\n"
                    connection.sendall(response.encode())

    elif command == "ACL":
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

    else:
        connection.sendall(b"-ERR unknown command\r\n")

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

        # Check if in subscribed mode and command is not allowed
        if in_subscribed_mode[0] and command not in allowed_in_subscribed_mode:
            cmd_lower = command.lower()
            error_msg = f"-ERR Can't execute '{cmd_lower}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
            connection.sendall(error_msg.encode())
            continue  # Skip executing this command

        if command == "MULTI":
            if in_transaction:
                connection.sendall(b"-ERR MULTI calls can not be nested\r\n")
            elif len(arguments) != 0:
                connection.sendall(b"-ERR wrong number of arguments for 'multi' command\r\n")
            else:
                in_transaction = True
                transaction_queue = []
                transaction_error = False
                connection.sendall(b"+OK\r\n")
            continue

        if command == "EXEC":
            if not in_transaction:
                connection.sendall(b"-ERR EXEC without MULTI\r\n")
            else:
                if transaction_error:
                    # Syntax error occurred during queuing - abort transaction
                    connection.sendall(b"-EXECABORT Transaction discarded because of previous errors.\r\n")
                else:
                    # Execute all queued commands, capturing both success and error responses
                    responses = []
                    for queued_command, queued_arguments in transaction_queue:
                        buffer_conn = BufferConnection()
                        execute_single_command(buffer_conn, queued_command, queued_arguments, Database, stream_last_ids, subscribed_channels, in_subscribed_mode)
                        response = buffer_conn.get_response()
                        # Include all responses (both success and error) in the array
                        responses.append(response)

                    # Build RESP array response: *N\r\n<response1><response2>...
                    resp_parts = [f"*{len(responses)}\r\n".encode()]
                    for resp in responses:
                        resp_parts.append(resp)
                    connection.sendall(b"".join(resp_parts))

                # Reset transaction state after EXEC
                in_transaction = False
                transaction_queue = []
                transaction_error = False
            continue

        if command == "DISCARD":
            if not in_transaction:
                connection.sendall(b"-ERR DISCARD without MULTI\r\n")
            else:
                in_transaction = False
                transaction_queue = []
                transaction_error = False
                connection.sendall(b"+OK\r\n")
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
    
    # Clean up: remove connection from all channel subscriptions
    with channel_subscribers_lock:
        for channel in list(channel_subscribers.keys()):
            channel_subscribers[channel].discard(connection)
            # Remove empty channel entries
            if len(channel_subscribers[channel]) == 0:
                del channel_subscribers[channel]
    
    connection.close()


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
    global rdb_dir
    if '--dir' in args:
        dir_index = args.index('--dir')
        try:
            if dir_index + 1 >= len(args):
                print("Invalid --dir format. Expected: --dir <DIRECTORY>")
                sys.exit(1)
            rdb_dir = args[dir_index + 1]
        except (ValueError, IndexError) as e:
            print(f"Invalid --dir argument: {e}")
            sys.exit(1)

    # Parse --dbfilename flag
    global rdb_dbfilename
    if '--dbfilename' in args:
        dbfilename_index = args.index('--dbfilename')
        try:
            if dbfilename_index + 1 >= len(args):
                print("Invalid --dbfilename format. Expected: --dbfilename <FILENAME>")
                sys.exit(1)
            rdb_dbfilename = args[dbfilename_index + 1]
        except (ValueError, IndexError) as e:
            print(f"Invalid --dbfilename argument: {e}")
            sys.exit(1)

    # Load RDB file if it exists
    if rdb_dir:
        rdb_filepath = os.path.join(rdb_dir, rdb_dbfilename)
    else:
        rdb_filepath = rdb_dbfilename
    
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
