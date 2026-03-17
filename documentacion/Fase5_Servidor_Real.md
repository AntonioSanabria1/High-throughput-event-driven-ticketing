# Fase 5: Construcción de un Backend Servidor Real y Event-Driven

Esta fase documenta la evolución final de nuestro ecosistema, convirtiendo scripts aislados en un Servidor Web asíncrono robusto capaz de sostener comunicación bidireccional en tiempo real mediante **Server-Sent Events (SSE)** y **Redis Pub/Sub**.

## 1. El Problema de la Ceguera del Frontend
En la arquitectura HTTP tradicional, el navegador envía una petición y el servidor responde. Una vez enviada la respuesta, la conexión muere y se corta físicamente.
Dado que Ticketmaster usa un Worker asíncrono en el Sótano para autorizar cobros bancarios que tardan varios segundos, el Frontend ("Navegador Web") se quedaba **ciego y desconectado**. 

Históricamente, esto forzaba a los desarrolladores a utilizar malas prácticas como el `Polling` (preguntarle a la base de datos cada segundo *"¿Ya terminaste?"*) o usar funciones engañosas de azar en Javascript que terminaban mostrando mensajes desincronizados al usuario de lo que realmente ocurrió en el sótano.

## 2. La Solución Arquitectónica (SSE + Redis Pub/Sub)
Para lograr que la aplicación reaccione instantáneamente al veredicto de la pasarela de pagos preservando recursos, integramos un patrón de transmisión unidireccional permanente:

1. **El Megáfono Interno (Redis Pub/Sub):** Actúa como un chat de intercomunicación dentro del servidor. Cuando el Worker de Python cobra o rechaza una compra, envía un evento JSON a un "canal de radio" único para ese usuario (Ej. canal `notificaciones:user_555`).
2. **El Tubo de Luz (Server-Sent Events):** FastAPI abre un canal eléctrico ininterrumpido a través de HTTP (`StreamingResponse`). A diferencia de HTTP normal, el backend **no corta la conexión**, sino que se suscribe silenciosamente a la sala de chat de Redis. Tan pronto el Worker lanza su grito de "Aprobado", FastAPI detecta la luz y la "empuja" por el tubo directamente al archivo web `index.html`.

## 3. Beneficios Técnicos al Nivel Producción
* **0% Falsos Positivos:** El cliente web jamás verá un error o una confirmación equivocada de nuevo; su interfaz está encadenada al estado físico y matemático absoluto de PostgreSQL y Redis.
* **Reducción de Latencia y Servidores:** A diferencia del Polling, no hay millones de peticiones HTTP preguntando el estado de los procesos. Hay 1 sola petición asíncrona suspendida, liberando la CPU de Uvicorn para miles de usuarios.
