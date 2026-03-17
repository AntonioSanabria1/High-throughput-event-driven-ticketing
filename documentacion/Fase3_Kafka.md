# Arquitectura de Asincronismo (Fase 3): Apache Kafka y Patrón Saga

## Contexto del Problema
Tras implementar Redis (Fase 2) para atajar la avalancha de peticiones, logramos proteger a PostgreSQL de la sobrecarga. Sin embargo, procesar un cobro bancario real (API de Stripe/Visa) demora varios segundos debido a la latencia de la red y la validación anti-fraudes. 

Si el servidor Web se queda "esperando" a que el banco conteste para cada uno de los clientes ganadores, los hilos de conexión se agotarían rápidamente, causando que la página web de Ticketmaster se quede congelándose para los usuarios.

La solución arquitectónica corporativa es **Desacoplar** (Separar físicamente) la recepción del pedido de la ejecución del cobro.

---

## La Solución: Arquitectura Orientada a Eventos (Productor-Consumidor)

Para lograr este puente temporal, introdujimos **Apache Kafka**, una bitácora distribuida inmutable (Commit Log). La aplicación original monolithica se separó en dos microservicios independientes que jamás se hablan directamente:

### 1. El Servidor Rápido (`api_producer.py`)
Este script simula el Backend que responde a los teléfonos celulares.
Su única misión es:
1. Interrogar a la caché en RAM (Redis).
2. Si el usuario gana el asiento, el servidor **NO** cobra la tarjeta. Simplemente serializa los datos de compra en un documento de texto (JSON).
3. Escribe ese documento JSON en el buzón `compras_pendientes` de Apache Kafka (operación de 1 milisegundo "Dispara y Olvida").
4. Le responde inmediatamente al navegador del celular: *"Procesando pago, te avisaremos"*.

### 2. El Trabajador de Fondo (`worker.py`)
Este script es un "Daemon" (Demonio) interno. Nadie en internet sabe que existe y no tiene un puerto público expuesto.
Su misión es:
1. Leer un bucle infinito ("Polling") el buzón de Kafka buscando mensajes nuevos.
2. Extraer el mensaje JSON, leer la tarjeta, y consumir los 5 segundos de espera contra la API bancaria artificial.
3. Actualizar la base de datos PostgreSQL con el estatus `sold` utilizando el esquema Optimista de la Fase 1.

---

## El Reto de Sincronía ("Saga Pattern")

Al tener Redis y PostgreSQL funcionando en tiempos distintos, nos enfrentamos al problema de la Desincronización de Estado:
¿Qué ocurre si el trabajador de fondo invierte 5 segundos intentando cobrar la tarjeta, pero el banco la declina por **Fondos Insuficientes**, mientras Redis ya le había restado `-1` al inventario horas atrás?

Ese boleto quedaría atrapado en el limbo para siempre (Asiento Fantasma).

### Implementación de la Transacción Compensatoria (Cache-Aside)
En arquitecturas distribuidas asíncronas, el manejo de errores recae en el código mediante el **Patrón Saga**.
En nuestro archivo `worker.py`, si la simulación bancaria retorna un error, el trabajador aborta la escritura a PostgreSQL y dispara una "Transacción de Compensación":

Se conecta de manera forzada y manual a la base de datos de Redis para ejecutar un comando `INCR` (Incrementar) sobre el asiento caído, regresándolo a la vida para que el próximo atacante en la API WEB (`api_producer.py`) pueda adquirirlo, salvando a la empresa de perder dinero por bloqueos falsos.
