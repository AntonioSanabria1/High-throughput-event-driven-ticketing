import asyncio
import asyncpg
import json
import time
import random
import redis.asyncio as redis
from confluent_kafka import Consumer, KafkaException

# Configuración BD
DB_CONFIG = {
    'user': 'ticketmaster',
    'password': 'password',
    'database': 'ticketdb',
    'host': '127.0.0.1',
    'port': 5432
}

# Configuración Consumidor Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'grupo-procesamiento-pagos',
    'auto.offset.reset': 'earliest'
}

# Inicialización de consumidor
consumidor_kafka = Consumer(KAFKA_CONFIG)
consumidor_kafka.subscribe(['compras_pendientes'])

async def procesar_pago_bancario(monto):
    """
    Simula validación contra pasarela de pagos.
    Incluye latencia artificial y probabilidad de rechazo.
    """
    print("\n   [Buscando fondos...] (Simulando 2s de latencia)")
    await asyncio.sleep(2)
    
    # Probabilidad de rechazo: 20%
    if random.random() < 0.2:
        return False
    return True

async def compensar_error_redis(redis_client, ticket_id):
    """
    Patrón saga compensatorio.
    Si el pago falla, la cantidad retenida en caché retorna al stock.
    """
    await redis_client.incr(f"ticket:{ticket_id}:stock")
    print(f"   [SAGA] Boleto {ticket_id} devuelto a stock en Redis.")

async def procesar_mensaje(mensaje_json, pool, redis_client):
    """Orquesta flujo transaccional tras consumo de evento de compra."""
    datos = json.loads(mensaje_json)
    usuario_id = datos['usuario_id']
    ticket_id = datos['ticket_id']
    
    print(f"\n[EVENTO] Procesando ticket {ticket_id} para usuario {usuario_id}")
    
    # 1. Validación de fondo bancario
    pago_exitoso = await procesar_pago_bancario(monto=50.00)
    
    if not pago_exitoso:
        print("   [RECHAZADO] Transacción bancaria declinada. Abortando.")
        await compensar_error_redis(redis_client, ticket_id)
        await redis_client.publish(f"notificaciones:{usuario_id}", json.dumps({"status": "rechazado"}))
        return
        
    print("   [APROBADO] Procediendo a consolidar en PostgreSQL...")
        
    # 2. Consolidación persistente
    async with pool.acquire() as conn:
        boleto = await conn.fetchrow('SELECT status, version FROM tickets WHERE id = $1', ticket_id)
        
        # Concurrencia optimista a nivel base de datos
        resultado = await conn.execute('''
            UPDATE tickets 
            SET status = 'sold', version = version + 1 
            WHERE id = $1 AND version = $2
        ''', ticket_id, boleto['version'])
        
        if resultado == 'UPDATE 1':
            print(f"   [CONSOLIDADO] Ticket {ticket_id} asignado a usuario {usuario_id}")
            await redis_client.publish(f"notificaciones:{usuario_id}", json.dumps({"status": "aprobado"}))
        else:
            print(f"   [COLISIÓN] Update abortado por violación de versión optimista.")
            await redis_client.publish(f"notificaciones:{usuario_id}", json.dumps({"status": "error"}))

async def bucle_infinito_del_worker():
    """Bucle principal de consumo"""
    print("Worker ejecutándose en background...")
    
    pool = await asyncpg.create_pool(**DB_CONFIG)
    redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    
    try:
        while True:
            msg = consumidor_kafka.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"Kafka error de lectura: {msg.error()}")
                continue
            
            contenido_json = msg.value().decode('utf-8')
            await procesar_mensaje(contenido_json, pool, redis_client)
            
    except KeyboardInterrupt:
        print("\nCerrando Worker...")
    finally:
        consumidor_kafka.close()
        await pool.close()
        await redis_client.aclose()

if __name__ == '__main__':
    asyncio.run(bucle_infinito_del_worker())
