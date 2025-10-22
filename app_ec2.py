from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Docker PySpark Example") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

df.write.option("header", True).csv("s3a://test-3il-2025-blahouel/test1")
