import os
import json
import requests
import logging
import time

from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()  # Charge les variables d'environnement depuis le fichier .env
API_KEY = os.getenv("OPENAQ_API_KEY", "VOTRE_CLE_API_ICI")
LOCATION_ID = 2178
BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}

# --- CONFIGURATION kafka ---
KAFKA_TOPIC = "air_data"
KAFKA_BROKER = "localhost:9092"
POLL_INTERVAL = 300  # 5 minutes

logging.basicConfig(level=logging.INFO)


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def get_location_details(location_id):
    """Récupère les métadonnées complètes de la station."""
    url = f"{BASE_URL}/locations/{location_id}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None


def get_location_sensors(location_id):
    """Récupère la liste des capteurs avec leurs paramètres (nom, unité, couverture)."""
    url = f"{BASE_URL}/locations/{location_id}/sensors"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("results", [])


def get_location_latest(location_id):
    """Récupère les dernières mesures de tous les capteurs."""
    url = f"{BASE_URL}/locations/{location_id}/latest"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("results", [])


def main():
    while True:
        # 1. Métadonnées de la station
        loc = get_location_details(LOCATION_ID)
        if not loc:
            logging.error("Station introuvable.")
            time.sleep(POLL_INTERVAL)
            continue

        # 2. Capteurs et paramètres
        sensors = get_location_sensors(LOCATION_ID)

        # 3. Dernières mesures
        latest = get_location_latest(LOCATION_ID)

        # 4. Sortie JSON complète (pour usage programmatique)
        full_data = {
            "location": loc,
            "sensors": sensors,
            "latest": latest,
        }

        if producer and full_data:
            producer.send(KAFKA_TOPIC, full_data)
            producer.flush()
            logging.info(f"Message envoyé au topic Kafka '{KAFKA_TOPIC}'.")
        else:
            logging.warning("Le producteur Kafka n'est pas initialisé ou les données sont vides.")

        # Attendre avant la prochaine itération
        logging.info(f"Attente de {POLL_INTERVAL} secondes avant la prochaine collecte.")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()