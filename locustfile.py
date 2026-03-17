from locust import HttpUser, task, between
import random

class CompradorTicketmasterVirtual(HttpUser):
    # Sin tiempos de espera entre requests para simular colisiones
    wait_time = between(0, 0)
    host = "http://127.0.0.1:8000"

    @task
    def bombardear_con_compras(self):
        """Tarea de prueba de carga: Simula peticiones masivas al endpoint de compra."""
        usuario_aleatorio = random.randint(1, 100000)
        payload = {
            "usuario_id": usuario_aleatorio,
            "ticket_id": 1
        }
        
        with self.client.post("/api/v1/comprar", json=payload, catch_response=True) as response:
            if response.status_code == 200:
                response.success() 
            elif response.status_code == 400:
                # Rechazo controlado por Redis cuenta como éxito en pruebas funcionales de stress
                response.success()
            else:
                response.failure(f"Fallo HTTP {response.status_code} en servidor")
