import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
TOPIC_PROCESSED = "processed_data"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/airsense_db"
POSTGRES_PROPERTIES = {
    "user": "dataops",
    "password": "dataops_password",
    "driver": "org.postgresql.Driver",
}

CHECKPOINT_BASE = "/tmp/spark-checkpoints/consumer-storage"

# --- SCHÉMA DU MESSAGE processed_data ---
processed_schema = StructType([
    StructField("loc_id", LongType(), True),
    StructField("loc_name", StringType(), True),
    StructField("sensor_id", LongType(), True),
    StructField("value", DoubleType(), True),
    StructField("valid_min", DoubleType(), True),
    StructField("valid_max", DoubleType(), True),
    StructField("validation_status", StringType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True),
])


def write_batch(batch_df, batch_id):
    """Écrit le batch dans les deux tables PostgreSQL."""
    if batch_df.rdd.isEmpty():
        logging.info(f"Batch {batch_id} vide, ignoré.")
        return

    total = batch_df.count()
    logging.info(f"Batch {batch_id} : {total} lignes reçues.")

    # 1. Écriture dans airsense_processed_data (toutes les mesures)
    batch_df.write \
        .mode("append") \
        .jdbc(POSTGRES_URL, "airsense_processed_data", properties=POSTGRES_PROPERTIES)

    logging.info(f"Batch {batch_id} : {total} lignes écrites dans airsense_processed_data.")

    # 2. Écriture dans airsense_anomalies (uniquement ANOMALY_HIGH)
    anomalies_df = batch_df.filter(col("status") == "ANOMALY_HIGH")

    if not anomalies_df.rdd.isEmpty():
        nb_anomalies = anomalies_df.count()
        anomalies_df.write \
            .mode("append") \
            .jdbc(POSTGRES_URL, "airsense_anomalies", properties=POSTGRES_PROPERTIES)
        logging.info(f"Batch {batch_id} : {nb_anomalies} anomalies écrites dans airsense_anomalies.")
    else:
        logging.info(f"Batch {batch_id} : aucune anomalie détectée.")


def main():
    spark = SparkSession.builder \
        .appName("Consumer-Storage-Processed") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Lecture du topic processed_data
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_PROCESSED) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Parsing JSON
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), processed_schema).alias("d")) \
        .select("d.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # 3. Écriture dans les deux tables via foreachBatch
    query = df_parsed.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main") \
        .start()

    logging.info("Consumer démarré : lecture de 'processed_data' et écriture dans "
                 "'airsense_processed_data' + 'airsense_anomalies'.")
    query.awaitTermination()


if __name__ == "__main__":
    main()