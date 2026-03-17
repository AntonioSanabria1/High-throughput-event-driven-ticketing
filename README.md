# Ticketmaster Simulation Architecture 

Un sistema de arquitectura distribuida a escala empresarial, diseñado para simular el complejo mecanismo transaccional asíncrono que existe detrás de una plataforma masiva de alta demanda, previniendo colapsos de red por estampida comercial (*Thundering Herd Pattern*). 

## Características Técnicas de la Arquitectura
- **API Gateway Asíncrono**: `FastAPI` (Python) encargado de despachar respuestas no bloqueantes a velocidades vertiginosas con SSE (Server-Sent Events) embebidos.
- **Caché en Memoria L1**: `Redis` usado transaccionalmente como barrera mitigadora inicial para rechazar intentos en ~20 milisegundos cuando el stock distribuido alcanza cero. Relevancia dual como difusor Pub/Sub.
- **Event Sourcing / CQRS**: `Apache Kafka` funge como bitácora inmutable asíncrona (Commit Log), segregando las peticiones de cobro de las lecturas. El clúster se pre-configuró con 10 particiones por *Topic* habilitando concurrencia estricta para clústeres elásticos (*Auto-Scaling*).
- **Consolidación Consistente**: `PostgreSQL` y `asyncpg` actuando como Capa Persistente. Aplica `Optimistic Concurrency Control (OCC)` utilizando versionamiento de registros a nivel fila para mitigar anulaciones/sobreventas ante fallas de red asíncrona (Double Spend).

## Requisitos Previos (Dependencias)
Necesitarás instalar un ecosistema local y un entorno distribuido:
- Python 3.10+
- Docker & Docker Compose V2

## Instrucciones de Instalación y Ejecución

Sigue el orden estricto para levantar los microservicios sin colisiones técnicas, ya que el API depende de los descriptores de red del entorno de contenedores subyacentes.

### 1. Iniciar Entorno de Infraestructura
Levanta PostgreSQL, Redis y Kafka embebido (KRaft NodeMode).
```bash
docker compose up -d
```

### 2. Pre-Generación Estática de Particionamiento
Construye el Buzón y fuerza su nivel de concurrencia horizontal (Cajas registradoras) **antes** de habilitar el servicio expuesto.
```bash
docker exec ticketmaster-simulation-kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic compras_pendientes --partitions 10 --bootstrap-server localhost:9092
```

### 3. Habilitar Entorno Virtual y Dependencias
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # O instala individualmente: fastapi, uvicorn, asyncpg, redis, confluent-kafka, locust
```

### 4. Ejecutar Microservicios (Correr en terminales separadas)

**A) Desplegar API Gateway (Frontal de Clientes)**
```bash
uvicorn main:app --reload
```
A partir de aquí, puedes abrir tu navegador consumiendo el archivo HTML en `frontend/index.html` (o usando un plugin simple como LiveServer) y comunicarte eficientemente.

**B) Desplegar Worker Asíncrono de Cobro Back-End**
```bash
python3 worker.py
```
*(Puedes correr `python3 worker.py` simultáneamente en N terminales extra para constatar el balanceo automático que ejerce Kafka entre las 10 Particiones disponibles).*

### 5. Opcional: Pruebas de Estrés
Para emular 5,000 bots y evaluar el límite arquitectónico local:
```bash
locust
```
Accede vía web a `http://localhost:8089` para declarar los parámetros de enjambre (Swarm Params) y verificar métricas en tiempo real.

---
*Desarrollado y guiado como experimento avanzado de diseño de sistemas distribuidos y Chaos Engineering.*
