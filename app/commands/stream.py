"""Stream command handlers: XADD, XRANGE, XREAD."""
import threading
import time

from app.state import stream_conditions, stream_conditions_lock
from app.stream_utils import parse_entry_id, validate_entry_id
from app.replication import is_replica_connection, propagate_command_to_replicas


def _get_entries_for_streams(key_id_pairs, Database):
    """Helper: get entries for each stream where ID > start_id (exclusive)."""
    streams_with_entries = []
    for key_id_pair in key_id_pairs:
        stream_key = key_id_pair["key"]
        start_id = key_id_pair["id"]
        if stream_key not in Database or Database[stream_key]["type"] != "stream":
            continue
        entries = Database[stream_key]["entries"]
        try:
            if "-" not in start_id:
                continue
            start_time_str, start_seq_str = start_id.split("-")
            start_time = int(start_time_str)
            start_seq = int(start_seq_str)
        except (ValueError, IndexError):
            continue
        result_entries = []
        for entry in entries:
            entry_id = entry["id"]
            entry_time_str, entry_seq_str = entry_id.split("-")
            entry_time = int(entry_time_str)
            entry_seq = int(entry_seq_str)
            entry_greater = (entry_time > start_time) or (
                entry_time == start_time and entry_seq > start_seq
            )
            if entry_greater:
                result_entries.append(entry)
        if len(result_entries) > 0:
            streams_with_entries.append({"key": stream_key, "entries": result_entries})
    return streams_with_entries


def handle_xadd(connection, command, arguments, Database, stream_last_ids):
    if len(arguments) < 4 or len(arguments) % 2 != 0:
        connection.sendall(b"-ERR wrong number of arguments for 'xadd' command\r\n")
        return
    stream_key = arguments[0]
    entry_id_input = arguments[1]
    if entry_id_input.endswith("-*"):
        time_part_str = entry_id_input.split("-")[0]
        try:
            time_part_int = int(time_part_str)
        except ValueError:
            connection.sendall(b"-ERR Invalid ID format\r\n")
            return
        if stream_key in Database and Database[stream_key]["type"] == "stream":
            entries = Database[stream_key]["entries"]
            if entries:
                last_seq = -1
                for entry in entries:
                    entry_id = entry["id"]
                    entry_time_str, entry_seq_str = entry_id.split("-")
                    entry_time = int(entry_time_str)
                    entry_seq = int(entry_seq_str)
                    if entry_time == time_part_int and entry_seq > last_seq:
                        last_seq = entry_seq
                if last_seq >= 0:
                    seq_no = last_seq + 1
                else:
                    seq_no = 1 if time_part_int == 0 else 0
            else:
                seq_no = 1 if time_part_int == 0 else 0
        else:
            seq_no = 1 if time_part_int == 0 else 0
        entry_id = f"{time_part_int}-{seq_no}"
    elif entry_id_input == "*":
        entry_id_time = int(time.time() * 1000)
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
                seq_no = last_seq + 1 if last_seq >= 0 else 0
            else:
                seq_no = 0
        else:
            seq_no = 0
        entry_id = f"{entry_id_time}-{seq_no}"
    else:
        entry_id = entry_id_input
        if not validate_entry_id(entry_id, stream_last_ids, stream_key, connection):
            return

    fields = {}
    for i in range(2, len(arguments), 2):
        fields[arguments[i]] = arguments[i + 1]

    if stream_key not in Database:
        Database[stream_key] = {"type": "stream", "entries": []}
    entry = Database[stream_key]

    if entry["type"] != "stream":
        connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
        return

    stream_entry = {"id": entry_id, "fields": fields}
    entry["entries"].append(stream_entry)
    stream_last_ids[stream_key] = entry_id

    with stream_conditions_lock:
        if stream_key in stream_conditions:
            cond = stream_conditions[stream_key]
            cond.acquire()
            cond.notify_all()
            cond.release()

    connection.sendall(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())
    if not is_replica_connection(connection):
        xadd_args = [stream_key, entry_id] + arguments[2:]
        propagate_command_to_replicas(command, xadd_args)


def handle_xrange(connection, arguments, Database):
    if len(arguments) != 3:
        connection.sendall(b"-ERR wrong number of arguments for 'xrange' command\r\n")
        return
    stream_key = arguments[0]
    start_id = arguments[1]
    end_id = arguments[2]
    if stream_key not in Database or Database[stream_key]["type"] != "stream":
        connection.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
        return
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
    for e in entries:
        entry_id = e["id"]
        entry_time_str, entry_seq_str = entry_id.split("-")
        entry_time = int(entry_time_str)
        entry_seq = int(entry_seq_str)
        if entry_time == end_time_for_max:
            if max_seq is None or entry_seq > max_seq:
                max_seq = entry_seq
    start_time, start_seq = parse_entry_id(start_id, is_start=True)
    end_time, end_seq = parse_entry_id(end_id, is_start=False, max_seq=max_seq)
    result_entries = []
    for e in entries:
        entry_id = e["id"]
        entry_time_str, entry_seq_str = entry_id.split("-")
        entry_time = int(entry_time_str)
        entry_seq = int(entry_seq_str)
        entry_after_start = (entry_time > start_time) or (
            entry_time == start_time and entry_seq >= start_seq
        )
        entry_before_end = (entry_time < end_time) or (
            entry_time == end_time and entry_seq <= end_seq
        )
        if not entry_after_start:
            continue
        if not entry_before_end:
            break
        result_entries.append(e)
    response_parts = [f"*{len(result_entries)}\r\n"]
    for e in result_entries:
        entry_id = e["id"]
        fields = e["fields"]
        response_parts.append("*2\r\n")
        response_parts.append(f"${len(entry_id)}\r\n{entry_id}\r\n")
        response_parts.append(f"*{len(fields) * 2}\r\n")
        for field, value in fields.items():
            response_parts.append(f"${len(field)}\r\n{field}\r\n")
            response_parts.append(f"${len(value)}\r\n{value}\r\n")
    connection.sendall("".join(response_parts).encode())


def handle_xread(connection, arguments, Database):
    block_timeout = None
    streams_index = 0
    if len(arguments) >= 2 and arguments[0].upper() == "BLOCK":
        try:
            block_timeout = int(arguments[1])
            streams_index = 2
        except ValueError:
            connection.sendall(b"-ERR value is not an integer or out of range\r\n")
            return
    streams_found = False
    for i in range(streams_index, len(arguments)):
        if arguments[i].upper() == "STREAMS":
            streams_index = i
            streams_found = True
            break
    if not streams_found or len(arguments) < streams_index + 3 or (
        len(arguments) - streams_index - 1
    ) % 2 != 0:
        connection.sendall(b"-ERR wrong number of arguments for 'xread' command\r\n")
        return
    num_streams = (len(arguments) - streams_index - 1) // 2
    key_id_pairs = []
    for i in range(num_streams):
        key_id_pairs.append({
            "key": arguments[streams_index + 1 + i],
            "id": arguments[streams_index + 1 + num_streams + i],
        })
    resolved_key_id_pairs = []
    for key_id_pair in key_id_pairs:
        stream_key = key_id_pair["key"]
        start_id = key_id_pair["id"]
        if start_id == "$":
            if stream_key not in Database or Database[stream_key]["type"] != "stream":
                continue
            entries = Database[stream_key]["entries"]
            if len(entries) == 0:
                resolved_id = "0-0"
            else:
                max_time = -1
                max_seq = -1
                for e in entries:
                    entry_id = e["id"]
                    entry_time_str, entry_seq_str = entry_id.split("-")
                    entry_time = int(entry_time_str)
                    entry_seq = int(entry_seq_str)
                    if entry_time > max_time or (
                        entry_time == max_time and entry_seq > max_seq
                    ):
                        max_time = entry_time
                        max_seq = entry_seq
                resolved_id = f"{max_time}-{max_seq}"
            resolved_key_id_pairs.append({"key": stream_key, "id": resolved_id})
        else:
            resolved_key_id_pairs.append(key_id_pair)
    key_id_pairs = resolved_key_id_pairs

    streams_with_entries = _get_entries_for_streams(key_id_pairs, Database)
    if len(streams_with_entries) > 0 or block_timeout is None:
        if len(streams_with_entries) == 0:
            connection.sendall(b"*0\r\n")
        else:
            response_parts = [f"*{len(streams_with_entries)}\r\n"]
            for stream_data in streams_with_entries:
                stream_key = stream_data["key"]
                result_entries = stream_data["entries"]
                response_parts.append("*2\r\n")
                response_parts.append(f"${len(stream_key)}\r\n{stream_key}\r\n")
                response_parts.append(f"*{len(result_entries)}\r\n")
                for e in result_entries:
                    entry_id = e["id"]
                    fields = e["fields"]
                    response_parts.append("*2\r\n")
                    response_parts.append(f"${len(entry_id)}\r\n{entry_id}\r\n")
                    response_parts.append(f"*{len(fields) * 2}\r\n")
                    for field, value in fields.items():
                        response_parts.append(f"${len(field)}\r\n{field}\r\n")
                        response_parts.append(f"${len(value)}\r\n{value}\r\n")
            connection.sendall("".join(response_parts).encode())
    else:
        conditions = []
        for key_id_pair in key_id_pairs:
            stream_key = key_id_pair["key"]
            with stream_conditions_lock:
                if stream_key not in stream_conditions:
                    stream_conditions[stream_key] = threading.Condition()
                conditions.append((stream_key, stream_conditions[stream_key]))
        timeout_seconds = float('inf') if block_timeout == 0 else block_timeout / 1000.0
        start_time = time.time()
        if conditions:
            condition = conditions[0][1]
            condition.acquire()
            try:
                while True:
                    elapsed = time.time() - start_time
                    if timeout_seconds != float('inf'):
                        remaining = timeout_seconds - elapsed
                        if remaining <= 0:
                            connection.sendall(b"*-1\r\n")
                            break
                        wait_time = min(remaining, 0.1)
                    else:
                        wait_time = 0.1
                    condition.wait(wait_time)
                    streams_with_entries = _get_entries_for_streams(key_id_pairs, Database)
                    if len(streams_with_entries) > 0:
                        response_parts = [f"*{len(streams_with_entries)}\r\n"]
                        for stream_data in streams_with_entries:
                            stream_key = stream_data["key"]
                            result_entries = stream_data["entries"]
                            response_parts.append("*2\r\n")
                            response_parts.append(f"${len(stream_key)}\r\n{stream_key}\r\n")
                            response_parts.append(f"*{len(result_entries)}\r\n")
                            for e in result_entries:
                                entry_id = e["id"]
                                fields = e["fields"]
                                response_parts.append("*2\r\n")
                                response_parts.append(f"${len(entry_id)}\r\n{entry_id}\r\n")
                                response_parts.append(f"*{len(fields) * 2}\r\n")
                                for field, value in fields.items():
                                    response_parts.append(f"${len(field)}\r\n{field}\r\n")
                                    response_parts.append(f"${len(value)}\r\n{value}\r\n")
                        connection.sendall("".join(response_parts).encode())
                        break
            finally:
                condition.release()
        else:
            connection.sendall(b"*-1\r\n")
