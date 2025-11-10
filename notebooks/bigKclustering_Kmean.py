# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
import time

inicio = time.time()

spark = SparkSession.builder.appName("AgrupacionKMeans").getOrCreate()

# RECORDAR CAMBIAR RUTA PARA CLUSTER UIS
ruta_tfidf = "hdfs://namenode:9000/user/spark/data/vectorized_tfidf/"
df = spark.read.parquet(ruta_tfidf)

# K=2 SE REFIERE A 2 AGRUPACIONES (RESEÑAS BUENAS O RESEÑAS MALAS)
kmeans = KMeans(k=2, seed=42, featuresCol="features", predictionCol="grupo")
modelo = kmeans.fit(df)
resultado = modelo.transform(df)

# GUARDAMOS EN HDFS LOS RESULTADOS (CAMBIAR RUTA EN CLUSTER UIS)
salida = "hdfs://namenode:9000/user/spark/data/resenias_clusterizadas/"
resultado.select("features", "grupo").write.mode("overwrite").csv(salida)

fin = time.time()
print(f"Clustering completado. Guardado en {salida}")
print(f"Tiempo total: {fin - inicio:.2f} segundos")

spark.stop()
