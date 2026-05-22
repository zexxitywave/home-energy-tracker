CREATE TABLE users (
                       id BIGSERIAL PRIMARY KEY,
                       name VARCHAR(100) NOT NULL,
                       surname VARCHAR(100),
                       email VARCHAR(255) NOT NULL UNIQUE,
                       address TEXT,
                       alerting BOOLEAN NOT NULL DEFAULT FALSE,
                       energy_alerting_threshold DOUBLE PRECISION NOT NULL DEFAULT 0
);