"""
Command syntax validation for transaction queuing.
"""


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
        "ACL": None,  # ACL command
        "AUTH": (1, 2),  # AUTH password or AUTH username password
        "RPUSH": None,  # Variable: at least 2 (key + values)
        "LPUSH": None,  # Variable: at least 2 (key + values)
        "LRANGE": (3,),  # LRANGE key start stop
        "LLEN": (1,),  # LLEN key
        "LPOP": None,  # LPOP key [count] - 1 or 2 args
        "BLPOP": None,  # BLPOP key [key ...] timeout - at least 2 args
        "REPLCONF": (2,),  # REPLCONF option value
        "PSYNC": (2,),  # PSYNC replicationid offset
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
        elif command == "RPUSH":
            if len(arguments) < 2:
                return False, b"-ERR wrong number of arguments for 'rpush' command\r\n"
        elif command == "LPUSH":
            if len(arguments) < 2:
                return False, b"-ERR wrong number of arguments for 'lpush' command\r\n"
        elif command == "LPOP":
            if len(arguments) < 1 or len(arguments) > 2:
                return False, b"-ERR wrong number of arguments for 'lpop' command\r\n"
        elif command == "BLPOP":
            if len(arguments) < 2:
                return False, b"-ERR wrong number of arguments for 'blpop' command\r\n"
        return True, None
    else:
        # Fixed argument count commands
        if len(arguments) not in expected_counts:
            cmd_name = command.lower()
            return False, f"-ERR wrong number of arguments for '{cmd_name}' command\r\n".encode()

    return True, None
