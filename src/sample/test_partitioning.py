
# save this as test_partitioning.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionTest").getOrCreate()

df = spark.range(0, 1000000)
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

df = df.repartition(6)  # increase partitions to test distribution
print(f"After repartitioning: {df.rdd.getNumPartitions()}")

df.groupBy((df["id"] % 5).alias("group")).count().show()

spark.stop()


