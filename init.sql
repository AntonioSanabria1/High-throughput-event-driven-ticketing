CREATE TABLE tickets (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    version INTEGER NOT NULL DEFAULT 0
);

-- Insertamos un único boleto (Asiento 1) para nuestra prueba de estampida
INSERT INTO tickets (event_id, status, version) VALUES (1, 'available', 0);
