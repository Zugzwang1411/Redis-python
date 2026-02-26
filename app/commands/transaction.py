"""Transaction command handlers: MULTI, EXEC, DISCARD."""
from app.resp import BufferConnection


def handle_multi(connection, arguments, in_transaction, transaction_queue, transaction_error):
    """Returns (in_transaction, transaction_queue, transaction_error) after handling."""
    if in_transaction:
        connection.sendall(b"-ERR MULTI calls can not be nested\r\n")
        return in_transaction, transaction_queue, transaction_error
    if len(arguments) != 0:
        connection.sendall(b"-ERR wrong number of arguments for 'multi' command\r\n")
        return in_transaction, transaction_queue, transaction_error
    connection.sendall(b"+OK\r\n")
    return True, [], False


def handle_exec(connection, in_transaction, transaction_queue, transaction_error, Database,
                stream_last_ids, subscribed_channels, in_subscribed_mode, execute_cmd):
    """Execute queued commands. Returns (in_transaction, transaction_queue, transaction_error)."""
    if not in_transaction:
        connection.sendall(b"-ERR EXEC without MULTI\r\n")
        return False, [], False
    if transaction_error:
        connection.sendall(b"-EXECABORT Transaction discarded because of previous errors.\r\n")
        return False, [], False
    responses = []
    for queued_command, queued_arguments in transaction_queue:
        buffer_conn = BufferConnection()
        execute_cmd(buffer_conn, queued_command, queued_arguments, Database, stream_last_ids,
                    subscribed_channels, in_subscribed_mode)
        response = buffer_conn.get_response()
        responses.append(response)
    resp_parts = [f"*{len(responses)}\r\n".encode()]
    for resp in responses:
        resp_parts.append(resp)
    connection.sendall(b"".join(resp_parts))
    return False, [], False


def handle_discard(connection, in_transaction, transaction_queue, transaction_error):
    """Returns (in_transaction, transaction_queue, transaction_error) after handling."""
    if not in_transaction:
        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
        return in_transaction, transaction_queue, transaction_error
    connection.sendall(b"+OK\r\n")
    return False, [], False
