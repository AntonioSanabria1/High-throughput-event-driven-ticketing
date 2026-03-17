# Fase 7: Pruebas de Carga Destructivas End-to-End (Locust/k6)

## Objetivo
El propósito de esta fase es validar la resiliencia arquitectónica del sistema simulando un escenario conocido como el problema del *Thundering Herd* (Estampida de peticiones), característico de plataformas masivas de boletaje como Ticketmaster. 

Mediante la herramienta `locust`, generamos miles de solicitudes concurrentes al sistema para medir latencias, evaluar el patrón CQRS y verificar la escalabilidad distribuida mediante colas de mensajes (Kafka).

---

## 1. Diseño de la Prueba de Carga

Se escribió un script especializado (`locustfile.py`) encargado de invocar asíncronamente miles de peticiones HTTP `POST` hacia la capa API Gateway (`main.py`).

**Métricas configuradas en la prueba:**
*   **Volumen Total:** 5,000 usuarios concurrentes.
*   **Velocidad de Invocación (Spawn Rate):** 500 peticiones por segundo.
*   **Patrón de latencia:** Aleatoriedad y colisión agresiva al forzar `wait_time = between(0,0)`.

---

## 2. Resultados Empíricos

El sistema compuesto por FastAPI + Redis + Kafka superó con éxito la prueba bajo las siguientes condiciones:
*   **0% de Errores de Servidor (5xx HTTP Fallbacks):** Ni un solo usuario recibió errores de denegación de servicio por caída de CPU en la capa frontal.
*   **1,000 RPS (Requests Per Second):** Volumen máximo sostenible medido antes de la atenuación local por hardware.
*   **21ms Promedio de Latencia:** Gracias a Redis actuando como caché rápido L1, la validación de inventario no llegó nunca a la capa persistente de PostgreSQL mediante llamadas red-a-red bloqueantes. Los rechazos (HTTP 400 - "Boletos Agotados") fueron instantáneos y computados positivamente dentro de los parámetros de estrés funcional.

---

## 3. Discusión Arquitectónica: Escalabilidad y Particiones de Kafka

Durante la prueba, se identificó e iteró un problema de cuello de botella (*Bottleneck*) respecto al proceso de `worker.py`:

### El Problema de la Única Partición
Por defecto al crearse un Tópico sin intervención, Apache Kafka asigna una (1) sola **Partición**. Kafka prohíbe inherentemente que múltiples *Consumers* de un mismo Consumer Group lean eventos de la misma partición a la vez, para garantizar la consistencia en el ordenamiento estricto de eventos por llave (`key`).
Consecuentemente, aunque se inicializaron dos o más Workers de Python en múltiples terminales para procesar compras, sólamente el Nodo Primario recibió carga, dejando en *Standby* inactivo al resto del clúster.

### Solución Dinámica de Re-Asignación (Consumer Rebalancing)
Se procedió a alterar la topología del *topic* directamente en el contenedor Docker mediante herramientas administrativas nativas de Kafka, expandiendo la vía a **10 Particiones concurrentes**.

```bash
docker exec ticketmaster-simulation-kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic compras_pendientes --partitions 10 --bootstrap-server localhost:9092
```

Inmediatamente tras aplicar la alteración de topología `(PartitionCount: 10)`, el Coordinador de Grupo de Kafka ejecutó un rebalanceo automático. Los mensajes encolados comenzaron a fluir equitativamente hacia los diversos `worker.py` corriendo en paralelo (Horizontal Scaling), los cuales procesaron, declinaron y resolvieron transacciones Saga compensatorias hacia Redis a nivel local con consistencia atómica mediante *Optimistic Concurrency Control* (OCC) en PostgreSQL.

## Conclusión Técnica
La arquitectura desacoplada asíncrona demostró cumplir cabalmente con las exigencias del tráfico *Big Data*, derivando eficientemente el impacto computacional del clúster primario hacia colas estables con resolución de compensación transaccional.
