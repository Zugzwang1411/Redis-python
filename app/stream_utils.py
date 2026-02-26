"""
Stream ID parsing and validation helpers.
"""


def parse_entry_id(entry_id, is_start=True, max_seq=None):
    """
    Parse an entry ID, handling optional sequence numbers.
    For start ID: if sequence missing, defaults to 0
    For end ID: if sequence missing, defaults to max_seq (or very large number)
    Returns: (time, seq) tuple
    """

    if entry_id == "-":
        return (0, 0)

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
