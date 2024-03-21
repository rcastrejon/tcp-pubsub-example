import json
import socket
import threading
import time

from random import randint


class Publisher:
    connections: list[socket.socket] = []

    def __init__(self, vehicle_id: str, broker_addresses: list[tuple[str, int]]):
        self.vehicle_id = vehicle_id
        self.broker_addresses = broker_addresses
        self.stop_event = threading.Event()
        self.mutex = threading.Lock()

    def start(self):
        # Inicia una conexión con cada uno de los brokers especificados.
        # Para cada conexión, se crea un hilo que intenta establecer la conexión,
        # si es satisfactoria, el socket se agrega a la lista de conexiones activas.
        self.start_connections()

        print("Publisher started.")

        while not self.stop_event.is_set():
            event = self.generate_sample_event()
            print(f"Sending message: {event.get("message")}")

            # Envía el evento a todos los sockets activos, luego espera un tiempo
            # aleatorio antes de enviar el siguiente evento.
            self.send_event(event)

            sleep_time = randint(1, 5)
            print(f"Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

    def start_connections(self):
        """
        Establece una conexión tcp con cada uno de los brokers especificados.
        """
        for broker_address in self.broker_addresses:
            connection_thread = threading.Thread(
                target=self.connect_to_broker, args=(broker_address,)
            )
            connection_thread.start()

    def connect_to_broker(self, broker_address):
        """
        Inicializa el socket y establece una conexión con el broker. Si la conexión
        es exitosa, el socket se agrega a la lista de conexiones activas.
        """
        while not self.stop_event.is_set():
            try:
                broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                broker_socket.connect(broker_address)
                with self.mutex:
                    self.connections.append(broker_socket)
                print(f"Connected to broker at {broker_address}")
                break
            except ConnectionRefusedError:
                print(
                    f"Failed to connect to broker at {broker_address}, retrying in 5 seconds..."
                )
                time.sleep(5)

    def send_event(self, event: dict):
        """
        Envía el evento a todos los brokers conectados.
        """
        event_data = json.dumps(event).encode()
        with self.mutex:
            for connection in self.connections:
                # Si la conexión se perdió, se elimina de la lista de conexiones activas.
                # Luego, se intenta enviar el evento a la siguiente conexión.
                try:
                    connection.sendall(event_data)
                except (BrokenPipeError, ConnectionResetError):
                    self.connections.remove(connection)

                    # Si no hay más conexiones activas, se intenta restablecer todas las conexiones.
                    print("Connection to broker lost, reconnecting...")
                    self.start_connections()

    def stop(self):
        """
        Detiene el publisher y cierra todas las conexiones activas.
        """
        self.stop_event.set()
        with self.mutex:
            for connection in self.connections:
                connection.close()

    def generate_sample_event(self):
        return {
            "type": "PUBLISH",
            "topic": self.vehicle_id,
            "message": {
                "vehicle_id": self.vehicle_id,
                "speed": randint(0, 100),
                "fuel": randint(0, 100),
                "temperature": randint(-20, 40),
                "tire_pressure": randint(0, 100),
                "engine_warning": randint(0, 1) == 1,
                "location": (randint(0, 100), randint(0, 100)),
                "route": f"route_{randint(0, 100)}",
            },
        }


if __name__ == "__main__":
    broker_addresses = [("localhost", 40000)]
    publisher = Publisher("vehicle_1", broker_addresses)
    try:
        publisher.start()
    except KeyboardInterrupt:
        publisher.stop()
