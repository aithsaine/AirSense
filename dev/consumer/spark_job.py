import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, lit, to_json, struct,
    explode, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
TOPIC_RAW = "air_data"
TOPIC_PROCESSED = "processed_data"
TOPIC_ANOMALIES = "anomalies"
CHECKPOINT_BASE = "/tmp/spark-checkpoints/consumer1"

# --- SCHÉMA ENRICHI : on inclut 'sensors' avec summary.min / summary.max ---
schema = StructType([
    StructField("location", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]), True),
    StructField("sensors", ArrayType(StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("parameter", StructType([
            StructField("name", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("units", StringType(), True),
        ]), True),
        StructField("summary", StructType([
            StructField("min", DoubleType(), True),
            StructField("max", DoubleType(), True),
            StructField("avg", DoubleType(), True),
        ]), True),
    ])), True),
    StructField("latest", StringType(), True),
])


def process_batch(batch_df, batch_id, spark):
    if batch_df.rdd.isEmpty():
        logging.info(f"Batch {batch_id} vide, ignoré.")
        return

    # 1. Extraction des champs de base
    df_base = batch_df.select(
        col("location.id").alias("location_id"),
        col("location.name").alias("location_name"),
        col("sensors").alias("sensors_list"),
        col("latest").alias("latest_json"),
    )

    # 2. Construction du mapping sensor_id -> (parameter, units, min, max)
    df_sensor_map = df_base.select(
        explode(col("sensors_list")).alias("s")
    ).select(
        col("s.id").alias("sensor_id"),
        col("s.parameter.name").alias("parameter_name"),
        col("s.parameter.displayName").alias("parameter_display"),
        col("s.parameter.units").alias("units"),
        col("s.summary.min").alias("valid_min"),
        col("s.summary.max").alias("valid_max"),
    ).dropDuplicates(["sensor_id"])

    # 3. Parsing de 'latest' via UDF
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

    # 4. Jointure avec le mapping (inclut valid_min / valid_max)
    df_enriched = df_exploded.join(df_sensor_map, on="sensor_id", how="left")

    # 5. VALIDATION selon min/max du summary
    #    - value hors [valid_min, valid_max] => INVALID_RANGE
    #    - value dans la plage => NORMAL / WARNING / ANOMALY_HIGH
    df_validated = df_enriched \
        .withColumn("timestamp", to_timestamp(col("datetime_utc"))) \
        .filter(
            col("value").isNotNull() &
            col("timestamp").isNotNull() &
            (col("value") >= 0)   # on garde >= 0, le reste est filtré plus bas
        ) \
        .withColumn(
            "validation_status",
            when(
                col("valid_min").isNull() | col("valid_max").isNull(),
                lit("NO_BOUNDS")          # pas de bornes disponibles
            ).when(
                (col("value") < col("valid_min")) | (col("value") > col("valid_max")),
                lit("INVALID_RANGE")      # hors des bornes historiques
            ).otherwise(lit("VALID"))
        ) \
        .withColumn(
            "status",
            when(col("validation_status") == "INVALID_RANGE", lit("INVALID_RANGE"))
            .when(col("value") > 50, lit("ANOMALY_HIGH"))
            .when(col("value") > 35, lit("WARNING"))
            .otherwise(lit("NORMAL"))
        )

    # 6. Écriture dans processed_data (toutes les mesures, avec bornes et validation)
    df_processed = df_validated.select(
        to_json(struct(
            "location_id", "location_name", "sensor_id",
            "parameter_name", "parameter_display", "units",
            "value", "valid_min", "valid_max",
            "validation_status",
            "datetime_utc", "timestamp", "status"
        )).alias("value")
    )

    df_processed.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_PROCESSED) \
        .save()

    logging.info(f"Batch {batch_id} : {df_validated.count()} mesures écrites dans {TOPIC_PROCESSED}")

    # 7. Écriture dans anomalies (uniquement ANOMALY_HIGH, hors INVALID_RANGE)
    df_anomalies = df_validated \
        .filter(col("status") == "ANOMALY_HIGH") \
        .select(
            to_json(struct(
                "location_id", "location_name", "sensor_id",
                "parameter_name", "parameter_display", "units",
                "value", "valid_min", "valid_max",
                "datetime_utc", "timestamp", "status"
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
        .appName("AirSense-SparkJob-MinMax") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("d")) \
        .select("d.*")

    query = df_parsed.writeStream \
        .foreachBatch(lambda df, bid: process_batch(df, bid, spark)) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main") \
        .start()

    logging.info("Spark job démarré (validation min/max activée).")
    query.awaitTermination()


if __name__ == "__main__":
    main()