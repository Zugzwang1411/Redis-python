"""Pub/sub command handlers: SUBSCRIBE, PUBLISH, UNSUBSCRIBE."""
from app.state import channel_subscribers, channel_subscribers_lock


def handle_subscribe(connection, arguments, subscribed_channels, in_subscribed_mode):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'subscribe' command\r\n")
        return
    channel = arguments[0]
    if subscribed_channels is None:
        subscribed_channels = set()
    subscribed_channels.add(channel)
    with channel_subscribers_lock:
        if channel not in channel_subscribers:
            channel_subscribers[channel] = set()
        channel_subscribers[channel].add(connection)
    channel_bytes = channel.encode('utf-8')
    count = len(subscribed_channels)
    response = f"*3\r\n$9\r\nsubscribe\r\n${len(channel_bytes)}\r\n{channel}\r\n:{count}\r\n"
    connection.sendall(response.encode())
    if in_subscribed_mode is not None:
        in_subscribed_mode[0] = True


def handle_publish(connection, arguments):
    if len(arguments) != 2:
        connection.sendall(b"-ERR wrong number of arguments for 'publish' command\r\n")
        return
    channel = arguments[0]
    message = arguments[1]
    with channel_subscribers_lock:
        if channel in channel_subscribers:
            subscribers = list(channel_subscribers[channel])
            subscriber_count = len(channel_subscribers[channel])
        else:
            subscribers = []
            subscriber_count = 0
    channel_bytes = channel.encode('utf-8')
    message_bytes = message.encode('utf-8')
    pub_message = f"*3\r\n$7\r\nmessage\r\n${len(channel_bytes)}\r\n{channel}\r\n${len(message_bytes)}\r\n{message}\r\n"
    pub_message_bytes = pub_message.encode('utf-8')
    for subscriber_conn in subscribers:
        try:
            subscriber_conn.sendall(pub_message_bytes)
        except (BrokenPipeError, ConnectionResetError, OSError):
            with channel_subscribers_lock:
                if channel in channel_subscribers:
                    channel_subscribers[channel].discard(subscriber_conn)
                    if len(channel_subscribers[channel]) == 0:
                        del channel_subscribers[channel]
    connection.sendall(f":{subscriber_count}\r\n".encode())


def handle_unsubscribe(connection, arguments, subscribed_channels, in_subscribed_mode):
    if len(arguments) != 1:
        connection.sendall(b"-ERR wrong number of arguments for 'unsubscribe' command\r\n")
        return
    channel = arguments[0]
    with channel_subscribers_lock:
        if channel in channel_subscribers:
            channel_subscribers[channel].discard(connection)
            if len(channel_subscribers[channel]) == 0:
                del channel_subscribers[channel]
    if subscribed_channels is not None:
        subscribed_channels.discard(channel)
        count = len(subscribed_channels)
    else:
        count = 0
    channel_bytes = channel.encode('utf-8')
    response = f"*3\r\n$11\r\nunsubscribe\r\n${len(channel_bytes)}\r\n{channel}\r\n:{count}\r\n"
    connection.sendall(response.encode())
    if in_subscribed_mode is not None and count == 0:
        in_subscribed_mode[0] = False
