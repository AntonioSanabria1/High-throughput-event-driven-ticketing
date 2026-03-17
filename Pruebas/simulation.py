import asyncio
import asyncpg
import time
import redis.asyncio as redis

# Configuración de conexión al servidor de la Bóveda (PostgreSQL)
DB_CONFIG = {
    'user': 'ticketmaster',
    'password': 'password',
    'database': 'ticketdb',
    'host': '127.0.0.1', # localhost
    'port': 5432
}

NUM_USUARIOS = 100
TICKET_ID = 1

async def reset_ticket(pool, redis_client=None):
    """Devuelve el ticket a su estado original antes de cada prueba."""
    async with pool.acquire() as conn:
        await conn.execute('''
            UPDATE tickets
            SET status = 'available', version = 0
            WHERE id = $1
        ''', TICKET_ID)
    
    if redis_client:
        await redis_client.set(f"ticket:{TICKET_ID}:stock", 1)
        
    print("\n[SETUP] Contexto de base de datos restaurado a estado transaccional inicial (Versión 0).")

async def test_estrategia(pool, redis_client, nombre_estrategia, corrutina_compra):
    """Función maestra que orquesta la estampida para una estrategia dada."""
    await reset_ticket(pool, redis_client)
    print(f"\nIniciando Prueba: {nombre_estrategia} con {NUM_USUARIOS} usuarios simultáneos")
    
    # Preparamos las peticiones asíncronas
    tareas = []
    for usuario_id in range(1, NUM_USUARIOS + 1):
        tareas.append(corrutina_compra(pool, redis_client, usuario_id))
    
    inicio = time.perf_counter()
    
    # Orquestación concurrente de alta densidad (Thundering Herd Simulation)
    resultados = await asyncio.gather(*tareas)
    
    fin = time.perf_counter()
    tiempo_total = fin - inicio
    
    # Análisis de resultados
    exitos = resultados.count(True)
    fallos = resultados.count(False)
    
    print(f"\nReporte de métricas ({nombre_estrategia}):")
    print(f" Tiempo de resolución de concurrencia: {tiempo_total:.4f} segundos")
    print(f" Transacciones commitadas exitosas: {exitos}")
    print(f" Transacciones abortadas/colisionadas: {fallos}")
    
    # Verificación de aislamiento transaccional
    if exitos > 1:
        print(" [CRÍTICO] Inconsistencia detectada. Violación de las propiedades ACID (Doble Venta).")
    elif exitos == 1:
        print(" [OK] Mutabilidad atómica preservada. Exclusión mutua garantizada.")
    else:
        print(" [WARNING] Todas las transacciones fallaron inesperadamente.")
async def compra_optimista(pool, redis_client, usuario_id):
    """
    Estrategia de Optimistic Concurrency Control (OCC).
    Evita la adquisición de cerrojos exclusivos mediante el patrón Compare-And-Swap (CAS).
    Consolida la actualización si y solo si la versión de la fila matriz no ha mutado durante el procesamiento.
    """
    async with pool.acquire() as conn:
        # 1. Recuperación de estado y versión sin bloqueo de lectura (Non-blocking I/O)
        boleto = await conn.fetchrow('''
            SELECT status, version FROM tickets WHERE id = $1
        ''', TICKET_ID)
        
        # 2. Validación referencial de negocio
        if boleto['status'] == 'available':
            
            version_leida = boleto['version']
            
            # Simulación de latencia de red en procesamiento asíncrono (50ms).
            # En modo OCC, el hilo cede el control al Event Loop permitiendo escalamiento a miles de RPS sin bloqueo.
            await asyncio.sleep(0.05)
            
            # 3. Solicitud atómica de mutación condicionada al delta de versión original
            resultado = await conn.execute('''
                UPDATE tickets 
                SET status = 'sold', version = version + 1 
                WHERE id = $1 AND version = $2
            ''', TICKET_ID, version_leida)
            
            # Evaluación de la confirmación de afectación de registros por parte del RDBMS
            if resultado == 'UPDATE 1':
                return True # Commit confirmado a nivel concurrente
            else:
                return False # Afectación cero; colisión de versión resuelta abortando de manera controlada
        else:
            return False # Registro marcado inmutable previamente

# --- CORRUTINAS DE SIMULACIÓN ARQUITECTÓNICA ---

async def compra_pesimista(pool, redis_client, usuario_id):
    """
    Estrategia Anti-Patrón (Escalado Horizontal limitante): Pessimistic Locking.
    Instruye retención exclusiva de la fila objetivo a nivel de motor de almacenamiento.
    """
    async with pool.acquire() as conn:
        # Declaración de contexto transaccional estricto
        async with conn.transaction():
            # 1. Emisión de cláusula ROW EXCLUSIVE LOCK para prevensión de Read Commited sucia
            boleto = await conn.fetchrow('''
                SELECT status FROM tickets WHERE id = $1 FOR UPDATE
            ''', TICKET_ID)
            
            # 2. Validación de estado retenido
            if boleto['status'] == 'available':
                
                # Simulación de cuello de botella artificial. Al retener el cerrojo exclusivo durante latencias
                # de red ordinarias, el pool de conexiones de la base de datos se saturará proporcionalmente a la carga (Bloqueo en cascada).
                await asyncio.sleep(0.05)
                
                # 3. Flasheo confirmatorio de la transición de estado
                await conn.execute('''
                    UPDATE tickets SET status = 'sold' WHERE id = $1
                ''', TICKET_ID)
                
                return True # Compra exitosa
            else:
                return False # Ya estaba vendido cuando lo leímos

async def compra_hibrida_redis(pool, redis_client, usuario_id):
    """
    Estrategia de Memoria Frontal L1 + DB Persistente L2.
    Interpone evaluación transaccional ultrarápida (Redis) para descartar 
    requests anómalos o fuera-de-cuota sin demandar IOPS al disco subyacente.
    """
    # 1. Delegación de coherencia de cupo al motor clave-valor atómico de un solo hilo
    stock_restante = await redis_client.decr(f"ticket:{TICKET_ID}:stock")
    
    # 2. Reestructuración de tráfico (Culling de excedente concurrente)
    if stock_restante < 0:
        return False 
        
    # 3. Consolidación de peticiones filtradas en el registro persistente (OCC)
    async with pool.acquire() as conn:
        boleto = await conn.fetchrow('''
            SELECT status, version FROM tickets WHERE id = $1
        ''', TICKET_ID)
        
        if boleto['status'] == 'available':
            version_leida = boleto['version']
            
            await asyncio.sleep(0.05) # Simulamos cobro de tarjeta
            
            resultado = await conn.execute('''
                UPDATE tickets 
                SET status = 'sold', version = version + 1 
                WHERE id = $1 AND version = $2
            ''', TICKET_ID, version_leida)
            
            if resultado == 'UPDATE 1':
                return True
            else:
                return False
        else:
            return False

async def main():
    # Creamos un Pool de conexiones a PostgreSQL
    pool = await asyncpg.create_pool(**DB_CONFIG, min_size=NUM_USUARIOS, max_size=NUM_USUARIOS)
    
    # Creamos conexión al "Guardián de la Puerta" Redis
    redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    
    print("[INIT] Framework de simulación de carga inicializado.")
    
    # === EJECUCIÓN SERIALIZADA DE CONTROL DE CREADO ===
    
    print("\n-----------------------------------------------------------")
    await test_estrategia(pool, redis_client, "PostgreSQL: OCC (Non-blocking DB-level Resolution)", compra_optimista)
    
    print("\n-----------------------------------------------------------")
    await test_estrategia(pool, redis_client, "Redis L1 Shield + PostgreSQL L2 OCC", compra_hibrida_redis)
    
    # Cerramos conexiones
    await pool.close()
    await redis_client.aclose()

if __name__ == '__main__':
    # Arrancamos el motor de eventos asíncronos de Python
    asyncio.run(main())
