import json
import socket
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import Event


class Broker:
    subscribers: dict[str, list[socket.socket]] = {}
    mutex = threading.Lock()

    def __init__(self, host: str, port: str):
        self.host = host
        self.port = port
        self.stop_event = threading.Event()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.engine = create_engine("sqlite:///local.db", echo=True)

    def start(self):
        # Crea el socket del servidor y lo pone a escuchar
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Broker started on {self.host}:{self.port}")

        while not self.stop_event.is_set():
            # Acepta conexiones de clientes y crea un hilo para manejar cada uno
            # de ellos. El hilo se encarga de recibir y enviar mensajes.
            client_socket, client_address = self.server_socket.accept()
            client_thread = threading.Thread(
                target=self.handle_client, args=(client_socket, client_address)
            )
            client_thread.start()

    def handle_client(self, client_socket, client_address):
        """
        Maneja la conexión con un cliente. Recibe y envía mensajes.
        """
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

    def handle_event(self, event, client_socket):
        """
        Maneja un evento recibido de un cliente. Los eventos pueden ser de dos
        tipos: PUBLISH o SUBSCRIBE. Los eventos de tipo PUBLISH son mensajes que
        un cliente quiere enviar a un tópico. Los eventos de tipo SUBSCRIBE son
        solicitudes de un cliente para suscribirse a un tópico.
        """
        event_type = event.get("type")
        topic_name = event.get("topic")

        print(f"Received {event_type} event for topic {topic_name}")

        if event_type == "PUBLISH":
            message = event.get("message")

            # Guarda el evento en la base de datos
            with Session(self.engine) as session:
                event = Event(
                    type=event_type,
                    topic=topic_name,
                    message=message,
                )
                session.add(event)
                session.commit()

            self.publish(topic_name, message, client_socket)
        elif event_type == "SUBSCRIBE":
            self.subscribe(topic_name, client_socket)

    def publish(self, topic_name, message, publisher_socket):
        """
        Envía un mensaje a todos los clientes suscritos a un tópico. El mensaje
        es enviado a todos los clientes suscritos, excepto al cliente que envió
        el mensaje original.
        """
        event = {
            "type": "PUBLISH",
            "topic": topic_name,
            "message": message,
        }
        event_data = json.dumps(event).encode()
        with self.mutex:
            for subscriber_socket in self.subscribers.get(topic_name, []):
                if subscriber_socket != publisher_socket:
                    subscriber_socket.sendall(event_data)

    def subscribe(self, topic_name, subscriber_socket):
        """
        Agrega un cliente a la lista de suscriptores de un tópico.
        """
        with self.mutex:
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
