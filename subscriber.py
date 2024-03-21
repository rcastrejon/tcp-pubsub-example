import socket
import threading


class Subscriber:
    def __init__(self, broker_address):
        self.broker_address = broker_address
        self.stop_event = threading.Event()

    def start(self):
        broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        broker_socket.connect(self.broker_address)

        print(
            "Subscriber started. Enter 'subscribe topic_name' to subscribe to a topic."
        )

        receive_thread = threading.Thread(
            target=self.receive_messages, args=(broker_socket,)
        )
        receive_thread.start()

        while not self.stop_event.is_set():
            user_input = input("> ")
            if user_input.startswith("subscribe"):
                _, topic_name = user_input.split(" ", 1)
                event = f"SUBSCRIBE {topic_name}"
                broker_socket.sendall(event.encode())

    def receive_messages(self, broker_socket):
        try:
            while not self.stop_event.is_set():
                data = broker_socket.recv(1024)
                if not data:
                    break
                event = data.decode().strip()
                self.handle_event(event)
        finally:
            broker_socket.close()

    def handle_event(self, event):
        parts = event.split(" ", 2)
        if len(parts) < 3:
            return

        event_type, topic_name, message = parts
        if event_type == "PUBLISH":
            print(f"Received message on topic '{topic_name}': {message}")

    def stop(self):
        self.stop_event.set()


if __name__ == "__main__":
    broker_address = ("localhost", 40000)
    subscriber = Subscriber(broker_address)
    try:
        subscriber.start()
    except KeyboardInterrupt:
        subscriber.stop()
