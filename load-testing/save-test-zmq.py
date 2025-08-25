from locust import User, task, constant, events
import zmq
import time
import json
import random
import string

context = zmq.Context()

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

class ZeroMQUser(User):
    wait_time = constant(0)

    def on_start(self):
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:3077")

    @task
    def guardar_clave_valor(self):
        key = random_string(6)
        value = random_string(12)

        mensaje = {
            "action": "SAVE",
            "key": key,
            "value": value
        }

        start_time = time.time()

        try:
            self.socket.send_json(mensaje)

            if self.socket.poll(5000):  # timeout de 5s
                respuesta = self.socket.recv_json()
                total_time = int((time.time() - start_time) * 1000)

                # Reporte de éxito
                events.request.fire(
                    request_type="ZMQ",
                    name="SAVE",
                    response_time=total_time,
                    response_length=len(json.dumps(respuesta).encode("utf-8")),
                    exception=None
                )
            else:
                raise TimeoutError("No response from server")

        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)

            # Reporte de fallo
            events.request.fire(
                request_type="ZMQ",
                name="SAVE",
                response_time=total_time,
                response_length=0,
                exception=e
            )
