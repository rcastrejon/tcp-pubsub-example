import socket
import threading
import time


class Publisher:
    def __init__(self, broker_addresses):
        self.broker_addresses = broker_addresses
        self.connections = []
        self.stop_event = threading.Event()

    def start(self):
        self.start_connections()

        print(
            "Publisher started. Enter 'publish topic_name message' to publish a message."
        )

        while not self.stop_event.is_set():
            user_input = input("> ")
            if user_input.startswith("publish"):
                _, topic_name, message = user_input.split(" ", 2)
                event = f"PUBLISH {topic_name} {message}"
                self.send_event(event)

    def start_connections(self):
        for broker_address in self.broker_addresses:
            connection_thread = threading.Thread(
                target=self.connect_to_broker, args=(broker_address,)
            )
            connection_thread.start()

    def connect_to_broker(self, broker_address):
        while not self.stop_event.is_set():
            try:
                broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                broker_socket.connect(broker_address)
                self.connections.append(broker_socket)
                print(f"Connected to broker at {broker_address}")
                break
            except ConnectionRefusedError:
                print(
                    f"Failed to connect to broker at {broker_address}, retrying in 5 seconds..."
                )
                time.sleep(5)

    def send_event(self, event):
        for connection in self.connections:
            try:
                connection.sendall(event.encode())
            except (BrokenPipeError, ConnectionResetError):
                self.connections.remove(connection)
                print("Connection to broker lost, reconnecting...")
                self.start_connections()

    def stop(self):
        self.stop_event.set()
        for connection in self.connections:
            connection.close()


if __name__ == "__main__":
    broker_addresses = [("localhost", 40000)]
    publisher = Publisher(broker_addresses)
    try:
        publisher.start()
    except KeyboardInterrupt:
        publisher.stop()
