"""
RDB file parsing and loading.
"""
import os
import struct

from app.state import global_database, global_database_lock


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
