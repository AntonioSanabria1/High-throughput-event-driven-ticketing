import asyncio
import time
import json
import redis.asyncio as redis
from confluent_kafka import Producer

# Variables Globales (Emulación de la API)
NUM_USUARIOS = 100
TICKET_ID = 1
KAFKA_TOPIC = 'compras_pendientes'

# Configuración del Productor de Kafka para el subsistema API
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'ticketmaster-api'
}

# Pre-asignación del pool de red del productor (Persistencia global del demonio de C)
productor_kafka = Producer(KAFKA_CONFIG)

def callback_entrega(err, msg):
    """Callback de supervisión para confirmación del Commit Log distribuido."""
    if err is not None:
        print(f"[ERROR KAFKA] Falla de persistencia en clúster: {err}")
    else:
        print(f"[RECONOCIDO] Evento atómico incorporado al Commit Log particionado: '{msg.topic()}'")

async def endpoint_comprar_boleto(redis_client, usuario_id):
    """
    Representación técnica de un Proxy Inverso/API Endpoint para ingesta unificada.
    Instrumenta rechazo rápido (Fast Fail) basado en cuota de memoria pura L1.
    """
    # 1. Decremento idempotente para filtro de concurrencia in-memory
    stock_restante = await redis_client.decr(f"ticket:{TICKET_ID}:stock")
    
    # 2. Evaluación del resultado de afectación RAM (Culling heurístico)
    if stock_restante < 0:
        return False 
        
    # 3. Formación de estructura del evento orientado a dominio (DDD Event)
    mensaje_evento = {
        "usuario_id": usuario_id,
        "ticket_id": TICKET_ID,
        "timestamp": time.time(),
        "status_pago": "pendiente_de_cobro" # Estado inmutable pre-resolución asíncrona
    }
    
    # Codificación de transporte universal
    payload = json.dumps(mensaje_evento)
    
    # 4. Transmisión delegada (Fire-And-Forget libraria) hacia la cola de mensajería asíncrona
    productor_kafka.produce(
        topic=KAFKA_TOPIC, 
        value=payload.encode('utf-8'),
        callback=callback_entrega
    )
    
    # Invocar subrutina de evacuación de buffer (Liberar callbacks diferidos)
    productor_kafka.poll(0)
    
    # 5. Interrupción temprana de contexto y retorno hacia pool HTTP receptor.
    # Descentralización del procesamiento referencial bancario al estrato backend.
    return True

async def main():
    print("[INIT] Inicializando emulador concurrente de API Gateway...")
    redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    
    # Restablecimiento de entorno L1 a modo experimentación unitaria
    await redis_client.set(f"ticket:{TICKET_ID}:stock", 1)
    print(f"\n[SETUP] Contexto Redis reinicializado (Quota de prueba restructurada a 1 slot disponible).")
    
    print(f"\n[STRESS TEST] Generando aspersión asíncrona de {NUM_USUARIOS} concurrent HTTP mock-quests...")
    
    tareas = []
    for app_user in range(1, NUM_USUARIOS + 1):
        tareas.append(endpoint_comprar_boleto(redis_client, app_user))
    
    inicio = time.perf_counter()
    
    resultados = await asyncio.gather(*tareas)
    
    # Ejecución bloqueante terminal para garantizar flujo de buffers out-of-band antes de matar el proceso del sistema.
    productor_kafka.flush()
    
    fin = time.perf_counter()
    tiempo_total = fin - inicio
    
    exitos = resultados.count(True)
    fallos = resultados.count(False)
    
    print(f"\n=== Reporte de Ingestión Frontal Transaccional ===")
    print(f" Resolución total de estampida ({NUM_USUARIOS} entities): {tiempo_total:.4f} segundos evaluados.")
    print(f" Peticiones enrutadas y derivadas a Event Sourcing asíncrono (Fast Path): {exitos}")
    print(f" Requests mitigados en borde perimetral por Memoria RAM L1 (Fast Fail): {fallos}")
    
    await redis_client.aclose()
    print("\n[TERMINATE] Instancia de API Gateway finalizada limpiamente.")

if __name__ == '__main__':
    asyncio.run(main())
