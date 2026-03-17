# Registro de Decisiones Arquitectónicas y Tecnologías

Este documento expone las tecnologías que conforman el stack principal de nuestro simulador y clon conceptual de Ticketmaster. Funciona como una bitácora técnica que detalla el **"Por qué"** se eligió una pieza de software frente a otras alternativas disponibles en la industria.

---

## 1. Base de Datos Relacional (La Bóveda)
**Tecnología Elegida:** PostgreSQL
*   **Rol en el Sistema:** Actúa como la "Fuente Única de Verdad" (Single Source of Truth). Su responsabilidad absoluta es garantizar matemáticamente que un asiento no sufra "Doble Venta" (Sobreventa).
*   **¿Por qué PostgreSQL y no bases NoSQL (MongoDB, Cassandra)?:**
    El caso de uso de Ticketmaster es un problema **puramente Transaccional**. Exige las propiedades ACID (Atomicidad, Consistencia, Aislamiento, Durabilidad) inflexibles. PostgreSQL provee mecanismos nativos a nivel de disco para control de concurrencia avanzado (Bloqueo Pesimista con `FOR UPDATE`, y Bloqueo Optimista con versiones CAS). Una base de datos NoSQL prioriza la disponibilidad y partición (Teorema CAP) sacrificando la consistencia inmediata (Eventual Consistency), lo cual es catastrófico al vender un asiento de estadio físico único.

---

## 2. Caché en Memoria (El Foso del Castillo)
**Tecnología Elegida:** Redis (Modo In-Memory)
*   **Rol en el Sistema:** Actuar como primer filtro o escudo de impacto frente a la Estampida (*Thundering Herd*). Absorbe el golpe de los usuarios concurrentes desde la RAM y rechaza al 99% sin que toquen el disco estructurado.
*   **¿Por qué Redis y no Memcached?:**
    Aunque ambos viven en la memoria RAM, Redis provee Estructuras de Datos Inteligentes y Comandos Atómicos Matemáticos incorporados en su motor escrito en C (ej. el comando `DECR` para restar inventario de un golpe atómico). Su naturaleza estricta de **Un Solo Hilo (Single-Threaded)** procesa las peticiones de red en serie a escala de nanosegundos, lo que repele conceptualmente las condiciones de carrera sin requerir diseño adicional.

---

## 3. Capa de Presentación Web (El API Gateway)
**Tecnología Elegida:** FastAPI (montado sobre Uvicorn ASGI)
*   **Rol en el Sistema:** Actuar como el escudo público de internet. Recibe estampidas de peticiones HTTP de los navegadores, consulta agresivamente a Redis e inyecta a Kafka solo a las personas con inventario real y los despacha retornando una respuesta visual rápida, cortando el puente de conexión HTTP antes del procesamiento duro posterior.
*   **¿Por qué FastAPI en vez de Flask/Django?**:
    Django y el Flask tradicional nacieron bajo la filosofía **WSGI** (Síncrono / Bloqueante). FastAPI abraza el poder moderno asíncrono puro de Python (**ASGI**) apalancado por su motor interno *Starlette* y su servidor residente *Uvicorn*. Además, su integración simbiótica con *Pydantic* valida tipos de datos y auto-genera documentación viva (Swagger UI) instantáneamente, forzando contratos limpios para el equipo de Frontend.

---

## 4. Entorno e Infraestructura 
**Tecnología Elegida:** Docker y Docker Compose
*   **Rol en el Sistema:** Orquestación, aislamiento de procesos, rápida destrucción/creación del laboratorio y portabilidad trans-sistema.
*   **¿Por qué Docker en vez de instalaciones nativas / Máquinas Virtuales (VMs)?:**
    Evita la contaminación del host o conflictos de puertos, logrando una estandarización de Infraestructura como Código (IaC). Permite, además, empaquetar todo el sistema distribuido a modo de red nativa (`host network`) mitigando fallas locales o corrupción de barreras como `iptables` en Linux, garantizando que el diseño completo sea reproducible instáneamente bajando el repositorio desde GitHub.

---

## 5. Orquestación Asíncrona (El Event Sourcing)
**Tecnología Elegida:** Apache Kafka y Patrones de Diseño de Microservicios (Saga / Cache-Aside)
*   **Rol en el Sistema:** Desacoplar físicamente el mundo Web (Celular/API rápida) del mundo operativo lento (Llamadas a terceros/Bancos, Bases de datos en disco). Permite al Frontend responder en milisegundos ignorando la demora del cobro que puede tomar segundos.
*   **¿Por qué Kafka y no RabbitMQ?:**
    RabbitMQ es excepcional despachando y borrando tareas estériles (como instruir que se envíen correos temporales). Kafka, en cambio, ofrece un modelo Event Sourcing (Bitácora inmutable/Logs que no se borran). Ticketmaster prioriza Kafka para preservar eternamente la firma del evento, habilitando en el futuro a sistemas analíticos ("Machine Learning" e Inteligencia Artificial) modelar comportamiento histórico de ventas predictivas o recomponer bases de datos caídas leyendo el historial en el tiempo ("Time-travel").

### El Reto de Sincronía ("Saga Pattern" / "Cache-Aside")
Al separar Redis (la memoria rápida) de PostgreSQL (la base de datos lenta final), surge el problema de **Desincronismo de Datos Disonante**: 
¿Qué pasa si Redis dice "Tienes el asiento", pero luego el banco rechaza la tarjeta del usuario? 

Redis jamás le hablará a PostgreSQL para preguntar "Oye ¿sí se lo cobraste?". 
Para mantener la congruencia, entra en juego el desarrollador y el concepto de **Transacción Compensatoria (Compensating Transaction)**: El Worker funge como el único árbitro consciente de todo el ciclo. Es su estricta obligación, al notar un rechazo bancario de Stripe, abortar su orden a PostgreSQL y **conectarse de regreso y manualmente a la Caché Redis** para regresar el inventario a la vida (comando `INCR`). Los Daemons jamás dialogan entre sí para arreglar discrepancias de fallos, todo recae sobre la arquitectura algorítmica y el código.
