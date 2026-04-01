import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, struct, to_json, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = "34.175.118.163:9092" #"51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC_IN = "imat3b-AVAX"
TOPIC_OUT = "imat3b-AVAX-VWAP"
CHECKPOINT_S3 = "s3://juanpan/checkpoints_vwap/"

jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{USERNAME}" password="{PASSWORD}";'

print("iniciando motor de spark streaming en aws glue...")

spark = SparkSession.builder \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema_in = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", StringType(), True), 
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_IN) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema_in).alias("data")) \
    .select(
        col("data.symbol").alias("symbol"),
        col("data.`@timestamp`").alias("ts_col"),
        col("data.close").alias("close"),
        col("data.volume").alias("volume")
    ) \
    .withColumn("ts_col", col("ts_col").cast("timestamp"))

df_watermarked = df_parsed.withWatermark("ts_col", "1 minute")

df_grouped = df_watermarked.groupBy(
    window(col("ts_col"), "5 minutes"),
    col("symbol")
)

df_vwap = df_grouped.agg(
    (_sum(col("close") * col("volume")) / _sum(col("volume"))).alias("vwap")
)

df_formatted = df_vwap.select(
    col("symbol").alias("key"),
    to_json(struct(
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias("window_end"),
        col("symbol"),
        col("vwap")
    )).alias("value")
)

query = df_formatted.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_OUT) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("checkpointLocation", CHECKPOINT_S3) \
    .outputMode("append") \
    .start()

query.awaitTermination()