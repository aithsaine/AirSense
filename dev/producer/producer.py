import json
import requests
import time
import logging  # <-- Import de la bibliothèque
from dotenv import get_key
from quixstreams import Application

# --- Configuration du logger ---
logging.basicConfig(
    level=logging.INFO, # Niveau minimum à afficher (INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s', # Format : Date - Niveau - Message
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Crée un logger spécifique à ce fichier

key = get_key('/home/ismail/Documents/S3/DataOps & MlOps/projects/AirSense/.env', "API_OPENAQ_KEY")
print(key)

def get_air_data():
    try:
        logger.info("Appel de l'API OpenAQ...")
        response = requests.get(
            "https://api.openaq.org/v3/locations/27/sensors",
            headers={"X-API-Key": key},
            timeout=10
        )
        response.raise_for_status() # Déclenche une erreur si le statut HTTP est 4xx ou 5xx
        logger.info("Données récupérées avec succès.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de l'appel API : {e}")
        return None

def main():
    app = Application(broker_address="localhost:9092", loglevel="ERROR")
    logger.info("Producteur démarré. Envoi des données vers Kafka...")
    
    with app.get_producer() as producer:
        while True:
            air_data = get_air_data()
            
            if air_data:
                try:
                    producer.produce(
                        topic="air_data",
                        key="Morocco".encode('utf-8'),
                        value=json.dumps(air_data).encode('utf-8'),
                    )
                    producer.flush()
                    logger.info("Données envoyées à Kafka avec succès.")
                except Exception as e:
                    logger.error(f"Erreur lors de l'envoi à Kafka : {e}")
            else:
                logger.warning("Aucune donnée reçue de l'API, on passe au cycle suivant.")
            
            # Attendre 5 secondes
            time.sleep(60)

if __name__ == "__main__":
    main()