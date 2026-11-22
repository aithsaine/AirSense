from kafka import KafkaConsumer
import json
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

consumer = KafkaConsumer(
    'processed_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logging.info("Consumer démarré. En attente de messages sur 'anomalies'...")

for message in consumer:
    data = message.value
    logging.info(f"Message reçu : {json.dumps(data, indent=4)}")