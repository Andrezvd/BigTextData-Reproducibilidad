# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.functions import col
import time
inicio = time.time()

spark = SparkSession.builder \
    .appName("VectorizacionTFIDF") \
    .getOrCreate()

ruta_limpio = "hdfs://namenode:9000/user/andres/cleaned/"

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

# GUARDAMOS LAS COLUMNAS QUE NOS IMPORTAN EN EL HDFS
rescaledData.select("features").write.mode("overwrite").parquet("hdfs://namenode:9000/user/andres/vectorized_tfidf/")

print("Vectorización TF-IDF completada y guardada en /user/andres/vectorized_tfidf/")
spark.stop()

fin = time.time()
print("Tiempo total de vectorización: {:.2f} segundos".format(fin - inicio))
