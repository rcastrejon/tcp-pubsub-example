import json
import socket
import threading
from typing import Any


class Broker:
    host: str
    port: int
    subscribers: dict[str, list[socket.socket]]
    stop_event: threading.Event
    server_socket: socket.socket

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.subscribers = {}
        self.stop_event = threading.Event()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Broker started on {self.host}:{self.port}")

        while not self.stop_event.is_set():
            client_socket, client_address = self.server_socket.accept()
            client_thread = threading.Thread(
                target=self.handle_client, args=(client_socket, client_address)
            )
            client_thread.start()

    def handle_client(self, client_socket: socket.socket, client_address: Any):
        print(f"New connection from {client_address}")
        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break
                event = json.loads(data.decode())
                self.handle_event(event, client_socket)
        finally:
            client_socket.close()

    def handle_event(self, event: dict, client_socket: socket.socket):
        event_type = event.get("type")
        topic_name = event.get("topic")

        if event_type == "PUBLISH":
            message = event.get("message")
            self.publish(topic_name, message, client_socket)
        elif event_type == "SUBSCRIBE":
            self.subscribe(topic_name, client_socket)

    def publish(self, topic_name: str, message: Any, publisher_socket: socket.socket):
        event = {"type": "PUBLISH", "topic": topic_name, "message": message}
        event_data = json.dumps(event).encode()
        for subscriber_socket in self.subscribers.get(topic_name, []):
            if subscriber_socket != publisher_socket:
                subscriber_socket.sendall(event_data)

    def subscribe(self, topic_name: str, subscriber_socket: socket.socket):
        self.subscribers.setdefault(topic_name, []).append(subscriber_socket)

    def stop(self):
        self.stop_event.set()
        self.server_socket.close()


if __name__ == "__main__":
    broker = Broker("localhost", 40000)
    try:
        broker.start()
    except KeyboardInterrupt:
        broker.stop()
