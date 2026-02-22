import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

# configuración
BUCKET_NAME = "juanpan"
BRONZE_PATH = f"s3://{BUCKET_NAME}/bronze/"
SILVER_PATH = f"s3://{BUCKET_NAME}/silver/"

# Inicialización
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("job_bronze_to_silver_imat3b09", {})

print(f"inicio de bronce para plata -")

# 1. lectura 
print(f"Leyendo desde: {BRONZE_PATH}")
# recursiveFileLookup=true permite leer dentro de subcarpetas (2021, 2022...)
df_bronze = spark.read.option("header", "true") \
                      .option("recursiveFileLookup", "true") \
                      .csv(BRONZE_PATH)

# transformacion
print("Aplicando limpieza y tipado de datos...")
df_silver = df_bronze.withColumn("datetime", to_date(col("datetime"))) \
                     .withColumn("open", col("open").cast("double")) \
                     .withColumn("high", col("high").cast("double")) \
                     .withColumn("low", col("low").cast("double")) \
                     .withColumn("close", col("close").cast("double")) \
                     .withColumn("volume", col("volume").cast("double"))

# limpieza
df_silver = df_silver.na.drop(subset=["close", "datetime"])

# 3. carga (LOAD)
print(f"Escribiendo en formato Parquet en: {SILVER_PATH}")
# modo sobreescritura 
df_silver.write.mode("overwrite").parquet(SILVER_PATH)

print("capa silver completada")
job.commit()