from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower, col
import time

inicio = time.time()

spark = SparkSession.builder \
    .appName("PreprocesamientoAmazonReviews") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# RUTA PARA LOCAL (CAMBIAR PARA CLUSTER UIS)
ruta_hdfs = "hdfs://namenode:9000/user/spark/data/Music.txt"


df = spark.read.text(ruta_hdfs)

# Limpieza
df_limpio = df.withColumn(
    "value",
    lower(regexp_replace(col("value"), "[^a-zA-Z\\s]", ""))
)

# RUTA LOCAL PARA GUARDAR EL DATASET LIMPIO (CAMBIAR PARA CLUSTER UIS)
df_limpio.write.mode("overwrite").text("hdfs://namenode:9000/user/spark/data/cleaned/")


print("✅ Preprocesamiento completado y guardado en /user/spark/data/cleaned/")

spark.stop()

fin = time.time()
print("⏱ Tiempo total: {:.2f} segundos".format(fin - inicio))
