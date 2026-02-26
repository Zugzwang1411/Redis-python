"""
RESP (Redis Serialization Protocol) parsing and encoding.
"""


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
