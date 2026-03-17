from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import asyncio
import time
import json
import redis.asyncio as redis
from confluent_kafka import Producer

# Inicialización de la aplicación FastAPI
app = FastAPI(
    title="Ticketmaster API",
    description="API Gateway protegida por Redis y Kafka",
    version="1.0.0"
)

# Habilitación de CORS para comunicación con el frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de Kafka
KAFKA_TOPIC = 'compras_pendientes'
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'ticketmaster-fastapi'
}

# Productor global de Kafka
productor_kafka = Producer(KAFKA_CONFIG)
redis_client = None

# Modelo de solicitud Pydantic
class SolicitudCompra(BaseModel):
    usuario_id: int
    ticket_id: int

@app.on_event("startup")
async def iniciar_conexiones():
    """Inicialización de conexiones a servicios externos"""
    global redis_client
    print("Conectando FastAPI a Redis...")
    redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    
    # Restablece inventario inicial para desarrollo
    await redis_client.set("ticket:1:stock", 2)
    print("Inventario inicializado en Redis: 2 Boletos (Ticket ID 1)")

@app.on_event("shutdown")
async def cerrar_conexiones():
    """Cierre seguro de clientes externos"""
    print("Cerrando conexiones...")
    productor_kafka.flush()
    await redis_client.aclose()

def callback_kafka(err, msg):
    """Callback de acuse de recibo de Kafka"""
    if err is not None:
        print(f"Error interno de Kafka: {err}")
    else:
        print(f"Kafka persistió evento: {msg.value().decode('utf-8')}")

# ==========================================
# RUTAS DE INTERNET (Endpoints)
# ==========================================

@app.get("/")
async def health_check():
    """Verifica el estado del servicio"""
    return {"status": "online", "message": "Ticketmaster API Operando"}

@app.post("/api/v1/admin/reset")
async def reiniciar_escenario():
    """Restaura el stock a 10000 boletos para pruebas de estrés"""
    await redis_client.set("ticket:1:stock", 10000)
    return {"status": "ok", "message": "Inventario restaurado a 10000 boletos"}

@app.get("/api/v1/notificaciones/{usuario_id}")
async def notificaciones(usuario_id: int):
    """Canal SSE para enviar notificaciones en tiempo real al navegador"""
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(f"notificaciones:{usuario_id}")
        try:
            while True:
                mensaje = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if mensaje is not None:
                    data = mensaje['data']
                    yield f"data: {data}\n\n"
                    break # Finaliza transmisión SSE tras respuesta del worker
                await asyncio.sleep(0.5)
        finally:
            await pubsub.unsubscribe(f"notificaciones:{usuario_id}")
            await pubsub.close()
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/v1/stock")
async def consultar_stock():
    """Consulta de inventario en Redis"""
    stock = await redis_client.get("ticket:1:stock")
    try:
        stock_restante = int(stock) if stock is not None else 0
    except ValueError:
        stock_restante = 0
    return {"ticket_id": 1, "stock_disponible": stock_restante}

@app.post("/api/v1/comprar")
async def comprar_boleto(orden: SolicitudCompra):
    """
    Controla el flujo de compra: decremento de Redis y encolado en Kafka.
    """
    # 1. Validación en caché
    stock_restante = await redis_client.decr(f"ticket:{orden.ticket_id}:stock")
    
    if stock_restante < 0:
        raise HTTPException(
            status_code=400, 
            detail="Stock agotado."
        )
        
    # 2. Emisión a Kafka
    mensaje_evento = {
        "usuario_id": orden.usuario_id,
        "ticket_id": orden.ticket_id,
        "timestamp": time.time(),
        "status_pago": "pendiente_de_cobro"
    }
    
    productor_kafka.produce(
        topic=KAFKA_TOPIC, 
        value=json.dumps(mensaje_evento).encode('utf-8'),
        callback=callback_kafka
    )
    productor_kafka.poll(0)
    
    # 3. Respuesta HTTP inmediata
    return {
        "status": "procesando",
        "ticket_id": orden.ticket_id,
        "mensaje_visual": "Procesando cobro en segundo plano"
    }
