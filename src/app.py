from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Docker PySpark Example") \
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

df.write.mode("overwrite").parquet("hdfs://hdfs-single-node:9000/test_parquet/")

spark.read.parquet("hdfs://hdfs-single-node:9000/test_parquet/").show()

