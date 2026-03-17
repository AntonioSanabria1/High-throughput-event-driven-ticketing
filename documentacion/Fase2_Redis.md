# Diseño Intermedio (Fase 2): Caché en Memoria (Redis) como Guardián

## Contexto: El Cuello de Botella del Disco

Tras estabilizar la **Fase 1** (PostgreSQL con Bloqueo Optimista), logramos aislar la DB de condiciones de carrera y sobreventa. 

Sin embargo, surgió un defecto arquitectónico en el paradigma de Alta Concurrencia: **El Desperdicio de Conexiones a Disco.**
En la simulación anterior, 100 usuarios intentaron comprar el Boleto 1. PostgreSQL detuvo la sobreventa respondiendo `0 filas afectadas` a 99 usuarios, lo cual protegió matemáticamente al sistema. No obstante, esos 99 usuarios aún tuvieron que:
1. Abrir un socket TCP de red contra la Base de Datos.
2. Hacer un *Parse* del SQL.
3. Evaluar el árbol de ejecución local contra el disco duro (donde yacen en frío los datos).

En una escala de millones de usuarios simultáneos, este comportamiento bloquearía los Workers de la API Web, agotando el CPU del servidor relacional, solo para distribuir comandos falimentados.

---

## La Solución: Intercepción en RAM mediante Redis

Por lo expuesto arriba, los sistemas modernos emplean un patrón de **Absorción de Impacto (Buffering)** colocando una Caché `In-Memory` delante del procesador relacional. Seleccionamos **Redis** debido a dos características técnicas que alivian nuestras necesidades:

1. **Memoria Pura (RAM):** Redis no interactúa activamente con un sistema de archivos en disco (I/O Blockers) en las consultas directas. Leer/Escribir en RAM tiene latencias medidas estadísticamente en microsegundos, contra milisegundos de un disco.

2. **Arquitectura Single-Threaded (Un Hilo):** Al igual que NodeJS, Redis usa un procesador concurrente para la I/O de red, repeliendo el concepto de "condición de carrera" al someter comandos que alteran estado (*Mutations*) a una sola fila de comandos bloqueantes.

---

## Decisiones de Arquitectura: Patrón Híbrido

Implementamos una estrategia Híbrida de intercepción en tiempo real a nivel código:
`Aplicación -> Consulta a Redis (Guardián) -> Validado -> Transacción en DB (Bóveda)`

### Estructura de Intercepción
La clave es el comando atómico `DECR` (*Decrement*) natural de Redis. Nuestro algoritmo transcurre así:

1.  **Semilla Inyectada:** El registro se inicializa (`Set`) en Redis como un diccionario simple: `ticket:1:stock = 1`.
2.  **Ataque Simultáneo:** Al sufrir la estampida de los 100 hilos, la aplicación Python ya no redirige el caudal base de SQL a PostgreSQL; somete 100 llamadas `DECR ticket:1:stock` paralelas a Redis.
3.  **Filtrado Atómico:** Redis, corriendo su bucle de un hilo, contesta secuencialmente a velocidad RAM:
      *   Al primero en llegar le resta 1, regresando `0`. (¡GANADOR!)
      *   Al resto de los hilos les regresará números progresivamente negativos (`-1, -2, -3...`). (¡PERDEDORES!)
4.  **Descarte Temprano (*Early Exit*):** La aplicación Python intercepta todo resultado `< 0` y aborta la transacción inmediatamente devolviendo "Error de Sobreventa", ahorrando ciclos I/O de red profunda.
5.  **Finalización Fuerte:** Exclusivamente el ÚNICO hilo que recibió `0`, invoca el comando `UPDATE` hacia PostgreSQL a través del Bloqueo Optimista. 

---

## Resultados del Laboratorio (Caché + DB vs Puro DB)

Sometimos a prueba ambas estructuras (Bóveda Pura vs El Guardián asíncrono + La Bóveda) lanzando 100 subprocesos asíncronos concurrentes de `asyncio` desde un ecosistema Python.

### Métricas Adquiridas
| Arquitectura Testeada | Latencia en el Cliente Web | Queries Totales Recibidos por PostgreSQL | Tráfico Basura Procesado |
| :--- | :--- | :--- | :--- |
| **Bóveda Desnuda (CAS DB)** | `0.1136` segs | `100` peticiones SQL Update | `99` hilos |
| **Arquitectura Híbrida (Redis + CAS DB)** | `0.0933` segs | `1` petición SQL Update | `0` hilos (Todos muertos en la capa de RAM) |

### Conclusión de la Fase 2

Este diseño demostró ser la arquitectura web **Estándar de Producción (Core Tier)** para sistemas de venta hiper-concurrentes. Se ha erradicado en un **99% el tráfico inútil** contra PostgreSQL, protegiéndolo para ser invocado puramente como consolidación de un boleto realmente vendido. 

> *Con la lógica de compra protegida entre Memoria RAM y Disco Estructurado, el sistema ahora soporta ataques extremos de peticiones entrantes. *
> 
> *Sin embargo, el Backend (API) colapsaría si el proceso posterior a la compra (Como la validación de una Tarjeta de Crédito con terceros tipo Stripe) demorara demasiado, agotando sus hilos.  El siguiente eslabón en el roadmap será delegar estos procesos "Lentos" a la asincronía purista, empleando un patrón Broker de **Mensajería (Menssage Queueing) de la Fase 3** para no entorpecer la rapidez de la transacción Frontal.*
