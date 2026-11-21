# from kafka import KafkaConsumer
# import json

# consumer = KafkaConsumer(
#     'air_data',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     auto_commit_interval_ms=5000,
#     value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # ← AJOUT
# )

# for message in consumer:
#     print(json.dumps(message.value, indent=4))  # ← AJOUT
    
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, lit, to_json, struct, get_json_object
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, lit, to_json, struct, get_json_object, explode
)

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
TOPIC_RAW = "air_data"
TOPIC_PROCESSED = "processed_data"
TOPIC_ANOMALIES = "anomalies"
CHECKPOINT_BASE = "/tmp/spark-checkpoints/consumer1"

# --- SCHÉMA VOLONTAIREMENT SIMPLE ---
# On ne déclare que ce dont on a besoin. Les champs manquants seront null.
schema = StructType([
    StructField("location", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]), True),
    StructField("latest", StringType(), True),  # on garde le JSON brut de la liste
])


def process_batch(batch_df, batch_id, spark):
    """
    Traite un micro-batch :
    1. Parse le JSON de 'latest' (qui est une liste de mesures)
    2. Explose la liste
    3. Nettoie, valide, détecte les anomalies
    4. Écrit dans les topics processed_data et anomalies
    """
    if batch_df.rdd.isEmpty():
        logging.info(f"Batch {batch_id} vide, ignoré.")
        return

    # 1. Extraction des champs de base
    df_base = batch_df.select(
        col("location.id").alias("location_id"),
        col("location.name").alias("location_name"),
        col("latest").alias("latest_json"),
    )

    # 2. On utilise get_json_object pour extraire chaque mesure de la liste
    #    (approche alternative à explode + from_json)
    #    On suppose que 'latest' est une liste JSON : [{"sensorsId":..., "value":...}, ...]
    #    On va utiliser une UDF Python pour parser la liste, plus simple à déboguer.
    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType

    def parse_latest(json_str):
        import json
        if not json_str:
            return []
        try:
            data = json.loads(json_str)
            if not isinstance(data, list):
                return []
            result = []
            for item in data:
                dt = item.get("datetime", {}) or {}
                result.append((
                    item.get("sensorsId"),
                    item.get("value"),
                    dt.get("utc"),
                ))
            return result
        except Exception as e:
            logging.error(f"Erreur parsing latest: {e}")
            return []

    latest_schema = ArrayType(StructType([
        StructField("sensor_id", LongType(), True),
        StructField("value", DoubleType(), True),
        StructField("datetime_utc", StringType(), True),
    ]))

    parse_udf = udf(parse_latest, latest_schema)

    df_exploded = df_base.select(
        "location_id",
        "location_name",
        explode(parse_udf(col("latest_json"))).alias("m")
    ).select(
        "location_id",
        "location_name",
        col("m.sensor_id").alias("sensor_id"),
        col("m.value").alias("value"),
        col("m.datetime_utc").alias("datetime_utc"),
    )

    # 3. Nettoyage + validation
    df_clean = df_exploded \
        .withColumn("timestamp", to_timestamp(col("datetime_utc"))) \
        .filter(
            col("value").isNotNull() &
            (col("value") >= 0) &
            col("timestamp").isNotNull()
        ) \
        .withColumn(
            "status",
            when(col("value") > 50, lit("ANOMALY_HIGH"))
            .when(col("value") > 35, lit("WARNING"))
            .otherwise(lit("NORMAL"))
        )

    # 4. Écriture dans processed_data (toutes les mesures valides)
    df_processed = df_clean.select(
        to_json(struct(
            "location_id", "location_name", "sensor_id",
            "value", "datetime_utc", "timestamp", "status"
        )).alias("value")
    )

    df_processed.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_PROCESSED) \
        .save()

    logging.info(f"Batch {batch_id} : {df_clean.count()} mesures écrites dans {TOPIC_PROCESSED}")

    # 5. Écriture dans anomalies (uniquement ANOMALY_HIGH)
    df_anomalies = df_clean \
        .filter(col("status") == "ANOMALY_HIGH") \
        .select(
            to_json(struct(
                "location_id", "location_name", "sensor_id",
                "value", "datetime_utc", "timestamp", "status"
            )).alias("value")
        )

    if not df_anomalies.rdd.isEmpty():
        df_anomalies.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", TOPIC_ANOMALIES) \
            .save()
        logging.info(f"Batch {batch_id} : anomalies écrites dans {TOPIC_ANOMALIES}")


def main():
    spark = SparkSession.builder \
        .appName("AirSense-SparkJob-Alt") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Lecture du flux
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parsing initial
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("d")) \
        .select("d.*")

    # Traitement par batch
    query = df_parsed.writeStream \
        .foreachBatch(lambda df, bid: process_batch(df, bid, spark)) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main") \
        .start()

    logging.info("Spark job démarré. En attente de messages sur 'air_data'...")
    query.awaitTermination()


if __name__ == "__main__":
    main()