# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
import time
inicio = time.time()

spark = SparkSession.builder \
    .appName("VectorizacionTFIDF") \
    .getOrCreate()

# RUTA DEL DATASET LIMPIO EN EL HDFS (CAMBIAR RUTA EN CLUSTER UIS)
ruta_limpio = "hdfs://namenode:9000/user/spark/data/cleaned/"

df = spark.read.text(ruta_limpio).withColumnRenamed("value", "texto")

# SEPARAMOS LAS PALABRAS
tokenizer = Tokenizer(inputCol="texto", outputCol="tokens")
df_tokens = tokenizer.transform(df)

# EELIMINAMOS PALABRAS SIN SENTIDO (STOPWORDS)
remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_filtrados")
df_filtrado = remover.transform(df_tokens)

# AQUI VEMOS LAS PALABRAS QUE SE REPITEN Y LAS CONTAMOS (LA FRECUENCIA DE CADA PALABRA)
hashingTF = HashingTF(inputCol="tokens_filtrados", outputCol="rawFeatures", numFeatures=10000)
featurizedData = hashingTF.transform(df_filtrado)

# SE CALCULA EL IDF Y SE APLICA AL DATASET
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# GUARDAMOS LAS COLUMNAS QUE NOS IMPORTAN EN EL HDFS (CAMBIAR RUTA EN CLUSTER UIS)
rescaledData.select("features").write.mode("overwrite").parquet("hdfs://namenode:9000/user/spark/data/vectorized_tfidf/")

print("Vectorización TF-IDF completada y guardada en /user/spark/data/vectorized_tfidf/")
spark.stop()

fin = time.time()
print("Tiempo total de vectorización: {:.2f} segundos".format(fin - inicio))
