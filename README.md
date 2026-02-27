# Redis Clone in Python

A fully functional, zero-dependency, in-memory data store built from scratch in Python. 

---

## Key Features

* **Zero External Dependencies:** Built entirely using Python's standard library (raw sockets, threads, locks).
* **RESP Protocol Parsing:** Full support for RESP2 serialization and deserialization.
* **Master-Replica Replication:** Asynchronous master-replica synchronization with the full `PSYNC` handshake and continuous command propagation.
* **RDB Persistence:** Ability to parse and load binary RDB (version 7) snapshot files directly into memory.
* **Advanced Data Structures:** Support for Strings, Lists, Streams (with auto-generated IDs), and Sorted Sets.
* **Geospatial Support:** Built-in Geohash encoding/decoding for geospatial indexing and radius queries.
* **Concurrency:** Thread-per-connection architecture with thread-safe global state management for blocking operations (like `BLPOP` and `XREAD`).
* **Transactions:** Support for queuing commands safely with `MULTI`, `EXEC`, and `DISCARD`.
* **Pub/Sub:** Channel-based real-time messaging architecture.

---

## Getting Started

Since this project has zero external dependencies, getting it running is incredibly straightforward.

**Prerequisites:**
* Python 3.8+

**Running the Server:**

```bash
# Clone the repository
git clone [https://github.com/yourusername/redis-clone.git](https://github.com/yourusername/redis-clone.git)
cd redis-clone

# Start the server on the default port (6379)
python main.py
