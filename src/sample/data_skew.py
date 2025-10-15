from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
import random

# Create SparkSession
spark = SparkSession.builder \
    .appName("SaltingExample") \
    .getOrCreate()

# Generate large skewed dataset
large_df = spark.range(500_000_000) \
    .withColumn("user_id",
        when(rand() < 0.9, concat(lit("user_1")))
        .otherwise(concat(lit("user_"), (floor(rand() * 8) + 2).cast("int")))
    ) \
    .withColumn("transaction_amount", rand() * 100)

# Generate small dataset (user profile)
small_data = [(f"user_{i}", f"User Name {i}", f"user_{i}@example.com") for i in range(1, 10)]
small_df = spark.createDataFrame(small_data, ["user_id", "name", "email"])

# Sample skew because of join
joined_df = large_df.join(small_df, on="user_id", how="inner")
joined_df.groupBy("user_id").agg(count("*").alias("txn_count")).orderBy(desc("txn_count")).show(10)


# salting
NUM_SALTS = 10
large_salted = large_df.withColumn("salt", floor(rand(seed=42) * NUM_SALTS))
salt_values = spark.createDataFrame([Row(salt=i) for i in range(NUM_SALTS)])
small_salted = small_df.withColumn("salt", floor(rand(seed=42) * NUM_SALTS))
large_salted.join(
    small_salted,
    on=[ "user_id", "salt" ],
    how="inner"
).drop("salt").show()



