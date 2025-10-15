
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ex1") \
    .getOrCreate()

df = spark.read.parquet("./data/logs/")
df.groupBy("region").count().show()
