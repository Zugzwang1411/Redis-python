import socket
import threading


def handle_client(connection):
    """Handle a single client connection - can receive multiple commands"""
    while True:
        data = connection.recv(1024)
        if not data:
            break  # Connection closed by client
        
        print("msg found: ", data)
        message = data.decode().strip()
        if "PING" in message:
            connection.sendall(b"+PONG\r\n")
    
    connection.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        # Accept new connections (handles both same-connection multiple commands AND multiple connections)
        connection, _ = server_socket.accept()
        
        # Handle each connection in a separate thread
        client_thread = threading.Thread(target=handle_client, args=(connection,))
        client_thread.daemon = True
        client_thread.start()

if __name__ == "__main__":
    main()