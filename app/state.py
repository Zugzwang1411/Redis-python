"""
Global state shared across the Redis server.
"""
import base64
import threading
from collections import deque

# Global dictionary to store Condition objects for each stream (for blocking XREAD)
stream_conditions = {}
stream_conditions_lock = threading.Lock()

# BLPOP: per-list-key queue of (connection, condition, result_holder) for FIFO wake order
blpop_waiters = {}

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

# Connections that are authenticated (e.g. as default when nopass, or after AUTH).
# New connections are auto-authenticated only if default user has no passwords.
authenticated_connections = set()
authenticated_connections_lock = threading.Lock()

# RDB configuration
rdb_dir = ""
rdb_dbfilename = "dump.rdb"
