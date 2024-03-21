import json
import socket
import threading


class Subscriber:
    def __init__(self, broker_address: tuple[str, int], topic_name: str):
        self.broker_address = broker_address
        self.topic_name = topic_name
        self.stop_event = threading.Event()

    def start(self):
        broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        broker_socket.connect(self.broker_address)

        # Lo primero que hace el Subscriber es enviar un evento de tipo SUBSCRIBE
        # al broker, para que así el broker guarde la conexión en su lista de
        # suscripciones.
        subscribe_event = {"type": "SUBSCRIBE", "topic": self.topic_name}
        event_data = json.dumps(subscribe_event).encode()
        broker_socket.sendall(event_data)

        print(
            f"Subscriber started and listening for messages on topic '{self.topic_name}'"
        )

        # Se inicializa el thread que gestiona los datos que recibe el socket.
        receive_thread = threading.Thread(
            target=self.receive_messages, args=(broker_socket,)
        )
        receive_thread.start()

        while not self.stop_event.is_set():
            # Esperar indefinidamente.
            pass

    def receive_messages(self, broker_socket):
        """
        Este método se ejecuta en un thread y se encarga de recibir los mensajes
        que envía el broker y de llamar al método handle_event para procesarlos.
        """
        try:
            while not self.stop_event.is_set():
                data = broker_socket.recv(1024)
                if not data:
                    break
                event = json.loads(data.decode())
                self.handle_event(event)
        finally:
            broker_socket.close()

    def handle_event(self, event):
        event_type = event.get("type")
        topic_name = event.get("topic")
        message = event.get("message")

        if event_type == "PUBLISH":
            print(f"Received message on topic '{topic_name}': {message}")

    def stop(self):
        self.stop_event.set()


if __name__ == "__main__":
    broker_address = ("localhost", 40000)
    subscriber = Subscriber(broker_address, "vehicle_1")
    try:
        subscriber.start()
    except KeyboardInterrupt:
        subscriber.stop()
