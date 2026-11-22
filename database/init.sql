-- ============================================================
-- Table 1 : Mesures nettoyées et validées (Consumer 2)
-- ============================================================
CREATE TABLE IF NOT EXISTS airsense_processed_data (
    id                  BIGSERIAL PRIMARY KEY,
    loc_id         BIGINT,
    loc_name       VARCHAR(100),
    sensor_id           BIGINT,
    value               DOUBLE PRECISION,
    timestamp           TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour accélérer les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_clean_timestamp
    ON airsense_processed_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_clean_sensor
    ON airsense_processed_data (sensor_id);

-- ============================================================
-- Table 2 : Anomalies détectées (Consumer 3)
-- ============================================================
CREATE TABLE IF NOT EXISTS airsense_anomalies (
    id                  BIGSERIAL PRIMARY KEY,
    location_id         BIGINT,
    location_name       VARCHAR(100),
    sensor_id           BIGINT,
    value               DOUBLE PRECISION,
    timestamp           TIMESTAMP,
    status              VARCHAR(20),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp
    ON airsense_anomalies (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_status
    ON airsense_anomalies (status);




CREATE TABLE IF NOT EXISTS sensors_reference (
    sensor_id           BIGINT PRIMARY KEY,
    sensor_name         VARCHAR(100),
    parameter_name      VARCHAR(50),
    parameter_display   VARCHAR(50),
    units               VARCHAR(20),
    valid_min           DOUBLE PRECISION,
    valid_max           DOUBLE PRECISION,
    location_id         BIGINT,
    location_name       VARCHAR(100),
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataops;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataops;
GRANT ALL PRIVILEGES ON SCHEMA public TO dataops;
