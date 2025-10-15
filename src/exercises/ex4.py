
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ex4") \
    .getOrCreate()

df = spark.read.parquet("./data/people/")
df.cache()
df.filter("col1 = 'foo'").count()
df.filter("col2 = 'bar'").count()
