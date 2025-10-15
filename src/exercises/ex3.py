
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ex3") \
    .getOrCreate()

df = spark \
        .read \
        .option("header", True) \
        .csv("./data/transactions/") 
df.count()
