import socket
import threading
import time


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

def handle_client(connection):
    """Handle a single client connection - can receive multiple commands"""
    Database = {}
    stream_last_ids = {}
    while True:
        data = connection.recv(1024)
        if not data:
            break
        
        print("msg found: ", data)
        
        # Parse the RESP command
        command, arguments = parse_command(data)
        
        if command == "PING":
            connection.sendall(b"+PONG\r\n")
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
            elif len(arguments) == 4:
                # SET key value EX seconds or SET key value PX milliseconds
                key = arguments[0]
                value = arguments[1]
                expiry_type = arguments[2].upper()
                try:
                    expiry_time = int(arguments[3])
                except ValueError:
                    connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                    continue
                
                if expiry_type == "EX":
                    # Expiry in seconds
                    expiry_timestamp = time.time() + expiry_time
                    Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
                    connection.sendall(b"+OK\r\n")
                elif expiry_type == "PX":
                    # Expiry in milliseconds
                    expiry_timestamp = time.time() + (expiry_time / 1000.0)
                    Database[key] = {"type": "string", "value": value, "expiry": expiry_timestamp}
                    connection.sendall(b"+OK\r\n")
                else:
                    connection.sendall(b"-ERR syntax error\r\n")
            else:
                connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
        elif command == "GET":
            if len(arguments) != 1:
                connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                continue
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
                        continue
                    
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
                        continue
                
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
                    
                    # Return entry ID as bulk string
                    response = f"${len(entry_id)}\r\n{entry_id}\r\n"
                    connection.sendall(response.encode())
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
                    
                    end_time_str = end_id.split("-")[0] if "-" in end_id else end_id
                    try:
                        end_time_for_max = int(end_time_str)
                    except ValueError:
                        connection.sendall(b"-ERR Invalid ID format\r\n")
                        continue
                    
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
        elif command:
            print(f"Received unknown command: {command}, arguments: {arguments}")
    
    connection.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        connection, _ = server_socket.accept()
        
        client_thread = threading.Thread(target=handle_client, args=(connection,))
        client_thread.daemon = True
        client_thread.start()


if __name__ == "__main__":
    main()
