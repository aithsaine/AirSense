import os
import json
import requests
import logging
import time

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY", "VOTRE_CLE_API_ICI")
LOCATION_ID = 2178
BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}

# --- KAFKA ---
KAFKA_TOPIC = "air_data"
KAFKA_BROKER = "localhost:9092"
POLL_INTERVAL = 300  # 5 minutes

# --- POSTGRESQL ---
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airsense_db",
    "user": "dataops",
    "password": "dataops_password",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# --- APPELS API ---
def get_location_details(location_id):
    url = f"{BASE_URL}/locations/{location_id}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None


def get_location_sensors(location_id):
    url = f"{BASE_URL}/locations/{location_id}/sensors"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("results", [])


def get_location_latest(location_id):
    url = f"{BASE_URL}/locations/{location_id}/latest"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("results", [])


# --- ÉCRITURE POSTGRESQL ---


def save_sensors_metadata(conn, location_id, location_name, sensors):
    """Insère ou met à jour les métadonnées des capteurs (une seule fois)."""
    rows = []
    for s in sensors:
        sensor_id = s.get("id")
        sensor_name = s.get("name")
        param = s.get("parameter") or {}
        summary = s.get("summary") or {}

        rows.append((
            sensor_id,
            sensor_name,
            param.get("name"),
            param.get("displayName"),
            param.get("units"),
            summary.get("min"),
            summary.get("max"),
            location_id,
            location_name,
        ))

    if not rows:
        logging.warning("Aucun capteur à enregistrer.")
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO sensors_reference (
                sensor_id, sensor_name, parameter_name, parameter_display,
                units, valid_min, valid_max, location_id, location_name, updated_at
            ) VALUES %s
            ON CONFLICT (sensor_id) DO UPDATE SET
                sensor_name = EXCLUDED.sensor_name,
                parameter_name = EXCLUDED.parameter_name,
                parameter_display = EXCLUDED.parameter_display,
                units = EXCLUDED.units,
                valid_min = EXCLUDED.valid_min,
                valid_max = EXCLUDED.valid_max,
                location_id = EXCLUDED.location_id,
                location_name = EXCLUDED.location_name,
                updated_at = NOW();
        """, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())")
    conn.commit()
    logging.info(f"{len(rows)} capteurs enregistrés dans sensors_reference.")


def main():
    # 1. Connexion PostgreSQL
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.autocommit = False
        logging.info("Connexion PostgreSQL établie.")
    except Exception as e:
        logging.error(f"Impossible de se connecter à PostgreSQL : {e}")
        return

    # 2. RÉCUPÉRATION UNIQUE DES MÉTADONNÉES (station + capteurs)
    logging.info("Récupération des métadonnées (une seule fois)...")
    loc = get_location_details(LOCATION_ID)
    if not loc:
        logging.error("Station introuvable. Arrêt.")
        conn.close()
        return

    sensors = get_location_sensors(LOCATION_ID)

    try:
        save_sensors_metadata(conn, LOCATION_ID, loc.get("name"), sensors)
    except Exception as e:
        logging.error(f"Erreur d'écriture des métadonnées : {e}")
        conn.rollback()

    # 3. BOUCLE : uniquement les mesures (latest)
    logging.info("Démarrage de la collecte des mesures...")
    try:
        while True:
            latest = get_location_latest(LOCATION_ID)

            full_data = {
                "location": loc,        # métadonnées déjà connues
                "sensors": sensors,     # métadonnées déjà connues
                "latest": latest,       # ← seules les mesures changent
            }

            if producer and full_data:
                producer.send(KAFKA_TOPIC, full_data)
                producer.flush()
                logging.info(f"Message envoyé au topic Kafka '{KAFKA_TOPIC}'.")
            else:
                logging.warning("Producteur Kafka non initialisé ou données vides.")

            logging.info(f"Attente de {POLL_INTERVAL} secondes avant la prochaine collecte.")
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logging.info("Arrêt du producteur.")
    finally:
        conn.close()
        producer.close()


if __name__ == "__main__":
    main()