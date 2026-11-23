import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_PROCESSED = "processed_data"
TOPIC_ANOMALIES = "anomalies"
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")   # "earliest" pour relire les topics depuis le début

POSTGRES_URL = os.getenv("DB_URL", "jdbc:postgresql://localhost:5432/airsense_db")
POSTGRES_PROPERTIES = {
    "user": os.getenv("DB_USER", "dataops"),
    "password": os.getenv("DB_PASSWORD", "dataops_password"),
    "driver": "org.postgresql.Driver",
}

# Nouveau dossier : l'ancien checkpoint ne connaissait qu'un seul topic (processed_data)
CHECKPOINT_BASE = "/tmp/spark-checkpoints/consumer-storage-v2"

# --- SCHÉMA DES MESSAGES (identique à ce que écrit spark_job.py) ---
# Le topic 'anomalies' n'a pas 'validation_status' : la colonne reste simplement à null.
message_schema = StructType([
    StructField("location_id", LongType(), True),
    StructField("location_name", StringType(), True),
    StructField("sensor_id", LongType(), True),
    StructField("parameter_name", StringType(), True),
    StructField("parameter_display", StringType(), True),
    StructField("units", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("valid_min", DoubleType(), True),
    StructField("valid_max", DoubleType(), True),
    StructField("validation_status", StringType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True),
])

# --- CORRESPONDANCE  colonne PostgreSQL -> champ du message Kafka ---
# Ces colonnes doivent exister dans init.sql. Pour stocker un champ de plus,
# ajoutez-le ici ET dans la table (ALTER TABLE ... ADD COLUMN ...).
PROCESSED_COLUMNS = {
    "loc_id": "location_id",
    "loc_name": "location_name",
    "sensor_id": "sensor_id",
    "value": "value",
    "timestamp": "timestamp",
}
ANOMALY_COLUMNS = {
    "location_id": "location_id",
    "location_name": "location_name",
    "sensor_id": "sensor_id",
    "value": "value",
    "timestamp": "timestamp",
    "status": "status",
}

# topic Kafka -> (table PostgreSQL, correspondance des colonnes)
TABLES = {
    TOPIC_PROCESSED: ("airsense_processed_data", PROCESSED_COLUMNS),
    TOPIC_ANOMALIES: ("airsense_anomalies", ANOMALY_COLUMNS),
}


def parse_messages(raw_df):
    """(topic, json_str) -> colonnes typées. Les messages illisibles sont écartés."""
    return (
        raw_df
        .select(col("topic"), from_json(col("json_str"), message_schema).alias("d"))
        .select("topic", "d.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .where(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
    )


def to_table_rows(parsed_df, topic, columns):
    """Garde les lignes d'un topic et renomme les champs en colonnes de la table."""
    return (
        parsed_df
        .where(col("topic") == topic)
        .select(*[col(src).alias(dst) for dst, src in columns.items()])
    )


def save_to_postgres(df, table):
    df.write \
        .mode("append") \
        .jdbc(POSTGRES_URL, table, properties=POSTGRES_PROPERTIES)


def write_batch(batch_df, batch_id):
    """Route chaque message vers la table de son topic."""
    if batch_df.isEmpty():
        logging.info(f"Batch {batch_id} vide, ignoré.")
        return

    batch_df = batch_df.persist()
    parsed = None
    try:
        received = batch_df.count()
        parsed = parse_messages(batch_df).persist()
        valid = parsed.count()
        if valid < received:
            logging.warning(f"Batch {batch_id} : {received - valid} message(s) illisible(s) ignoré(s).")

        for topic, (table, columns) in TABLES.items():
            rows = to_table_rows(parsed, topic, columns)
            n = rows.count()
            if n == 0:
                logging.info(f"Batch {batch_id} : rien à écrire pour '{topic}'.")
                continue
            save_to_postgres(rows, table)
            logging.info(f"Batch {batch_id} : {n} ligne(s) du topic '{topic}' écrites dans {table}.")
    finally:
        if parsed is not None:
            parsed.unpersist()
        batch_df.unpersist()


def main():
    spark = SparkSession.builder \
        .appName("Consumer-Storage-Processed-Anomalies") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()
    # ansi=false : un timestamp invalide donne NULL (message écarté) au lieu de faire tomber le job

    spark.sparkContext.setLogLevel("WARN")

    # 1. Lecture des DEUX topics dans un seul flux (la colonne 'topic' dit d'où vient chaque message)
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", f"{TOPIC_PROCESSED},{TOPIC_ANOMALIES}") \
        .option("startingOffsets", STARTING_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("topic", "CAST(value AS STRING) AS json_str")

    # 2. Parsing + écriture dans les deux tables via foreachBatch
    query = df_raw.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main") \
        .start()

    logging.info(f"Consumer démarré : '{TOPIC_PROCESSED}' -> airsense_processed_data, "
                 f"'{TOPIC_ANOMALIES}' -> airsense_anomalies.")
    query.awaitTermination()


if __name__ == "__main__":
    main()