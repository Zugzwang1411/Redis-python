import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        while True:
            data = connection.recv(1024)
            if not data:
                break
            ping_count = data.count(b"PING")
            for _ in range(ping_count):
                connection.sendall(b"+PONG\r\n")

        connection.close()

if __name__ == "__main__":
    main()
