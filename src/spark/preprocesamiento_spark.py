from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower, col

spark = SparkSession.builder \
    .appName("PreprocesamientoAmazonReviews") \
    .getOrCreate()

# RUTA DEL DATASET EN EL HDFS
ruta_hdfs = "hdfs://namenode:9000/user/andres/datasets/Music.txt"
df = spark.read.text(ruta_hdfs)

# LIMPIEZA DEL DATASET
df_limpio = df.withColumn(
    "value",
    lower(regexp_replace(col("value"), "[^a-zA-Z\\s]", ""))
)

# GUARDAMOS EL NUEVO DATASET LIMPIO EN HDFS
df_limpio.write.mode("overwrite").text("hdfs://namenode:9000/user/andres/cleaned/")

print("Preprocesamiento completado y guardado en /user/andres/cleaned/")
spark.stop()
