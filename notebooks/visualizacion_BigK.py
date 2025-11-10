from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ResumenClusters").getOrCreate()

# CAMBIAR RUTA EN CLUSTER UIS

df_clusters = spark.read.csv("hdfs://namenode:9000/user/spark/data/reseñas_clusterizadas/", header=False)
df_clusters.groupBy("_c1").count().show()

spark.stop()
