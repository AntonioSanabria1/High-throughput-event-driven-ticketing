# Diseño Base (Fase 1): Arquitectura Transaccional y Prevención de Sobreventa

## Contexto
En sistemas de boletaje con picos masivos de demanda (Ticketmaster, Eventbrite), miles de procesos distintos intentan mutar un mismo recurso finito (ej. "El Asiento 1") en el mismo milisegundo. Esto genera el fenómeno conocido como **Thundering Herd** (Estampida), el cual desemboca en Condiciones de Carrera (*Race Conditions*).

Si las transacciones no se controlan estructuralmente en la base de datos, el sistema incurrirá en una falla catastrófica de negocio: **La Sobrevenda** (Doble Venta del mismo boleto).

El objetivo de esta fase es estructurar el esquema Core en una base de datos relacional (PostgreSQL) para que opere matemáticamente como una "bóveda acorazada", garantizando integridad bajo concurrencia extrema.

---

## Esquema de Tabla
El diseño de la tabla `tickets` requiere abstraerse de los datos del cliente y enfocarse en el control de estado. Para soportar el control de concurrencia avanzado, de diseñó el esquema para ser agnóstico del modelo de bloqueo, pero forzosamente requiere una columna de versión.

```sql
CREATE TABLE tickets (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    
    -- Columna imperativa para la estrategia de Bloqueo Optimista
    version INTEGER NOT NULL DEFAULT 0
);
```
**Justificación de `version`:** Una columna incremental (`version = version + 1`) permite habilitar instrucciones lógicas tipo Compare-And-Swap (CAS) apoyadas directamente en las propiedades ACID del motor de base de datos.

---

## Decisiones de Arquitectura: Pesimista vs Optimista

Para esta primera iteración, la base de datos recibe peticiones desprotegidas directamente desde la lógica de la aplicación. Se evaluaron dos protocolos clásicos de ingeniería de software para prevenir colisiones en la tabla anterior:

### 1. Bloqueo Pesimista (FOR UPDATE)
*   **Mecanismo:** Usa cerrojos explícitos a nivel de fila (`Row-level Locks`). La primera conexión que llega envía un `SELECT ... FOR UPDATE`, paralizando indefinidamente a cualquier otra transacción que intente leer o escribir esa misma fila.
*   **Decisión:** **RECHAZADO para producción masiva.**
*   **Por qué:** Al emular un `Mutex` duro sobre el disco, indujo la saturación del Connection Pool. En nuestras pruebas de regresión, una demora artificial de 50ms para la transacción derivó en casi medio segundo (0.38s) de latencia acumulada con apenas 100 usuarios, probando ser incapaz de escalar y amenazando con monopolizar la RAM de PostgreSQL.

### 2. Bloqueo Optimista (Control de Versiones / CAS)
*   **Mecanismo:** Lectura libre paralela de todos los hilos simultáneamente. Al ejecutar el `UPDATE`, se concatena una condición estricta: se modifica *SÓLO SI* la versión en el disco sigue siendo idéntica a la que el hilo leyó originalmente.
*   **Decisión:** **SELECCIONADO como guardián core.**
*   **Por qué:** Permite procesamiento altamente concurrente asumiendo una baja probabilidad inicial de éxito para la masa. Ejecuta el `UPDATE` en nanosegundos: 1 hilo tiene éxito incrementando la versión y 99 hilos son devueltos instantáneamente a la capa de aplicación web con "0 filas afectadas", ahorrando memoria y bloqueos inactivos en el servidor SQL.

---

## Impacto Futuro de esta Arquitectura

El esquema transaccional **Optimista** ha blindado la integridad de los datos, demostrando que PostgreSQL jamás sobre-venderá un asiento.

Sin embargo, el costo de este diseño en nuestro estado actual de desarrollo es que **el 99% de las peticiones perdidas continúan viajando a través de la red y abriendo conexiones I/O contra PostgreSQL**, únicamente para ser escupidas de regreso por la directiva `version`. 

**Siguiente evolución arquitectónica:** 
Para evitar que la base de datos pague el costo en CPU de rebotar tráfico basura, esta Fase 1 (Base de Datos Relacional) pasará a convertirse en la última capa de defensa de nuestro sistema.

Se planea acoplar en el futuro un sistema **In-Memory Cache (como Redis)** entre los usuarios y PostgreSQL. Redis funcionará como un muro de contención asimilando el tráfico de la estampida y repartiendo los "No, ya se vendió" desde la Memoria RAM, permitiendo que *solamente el hilo ganador* llegue verdaderamente a ejecutar tráfico sobre esta Arquitectura SQL Optimista.
