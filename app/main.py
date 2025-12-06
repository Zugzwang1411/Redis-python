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


def handle_client(connection):
    """Handle a single client connection - can receive multiple commands"""
    Database = {}
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
                Database[key] = {"value": value, "expiry": None}
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
                    Database[key] = {"value": value, "expiry": expiry_timestamp}
                    connection.sendall(b"+OK\r\n")
                elif expiry_type == "PX":
                    # Expiry in milliseconds
                    expiry_timestamp = time.time() + (expiry_time / 1000.0)
                    Database[key] = {"value": value, "expiry": expiry_timestamp}
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
                # Check if key has expired
                entry = Database[key]
                if entry["expiry"] is not None and time.time() > entry["expiry"]:
                    # Key has expired, remove it
                    del Database[key]
                    connection.sendall(b"$-1\r\n")
                else:
                    # Key is valid, return the value
                    value = entry["value"]
                    msg = f"${len(value)}\r\n{value}\r\n"
                    connection.sendall(msg.encode())
        elif command == "TYPE":
            if len(arguments) != 1:
                connection.sendall(b"-ERR wrong number of arguments for 'type' command\r\n")
            else:
                key = arguments[0]
                if key not in Database:
                    # Key doesn't exist
                    connection.sendall(b"+none\r\n")
                else:
                    # Check if key has expired
                    entry = Database[key]
                    if entry["expiry"] is not None and time.time() > entry["expiry"]:
                        # Key has expired, remove it
                        del Database[key]
                        connection.sendall(b"+none\r\n")
                    else:
                        # Key exists and is a string (Redis TYPE command returns "string" for our simple values)
                        connection.sendall(b"+string\r\n")
        elif command:
            # Handle other commands here in the future
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
