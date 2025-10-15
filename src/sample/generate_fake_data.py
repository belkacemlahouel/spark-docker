# spark-submit --master spark://spark-master:7077 /app/sample/generate_fake_data.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when
import random

print("generating data for tp3...")
spark = SparkSession.builder \
    .appName("TP3 Spark Data generation") \
    .getOrCreate()

base_path = "/data/fake_data/"

# -----------------------------
# 1. skewed data
# -----------------------------
def generate_skewed_logs():
    num_rows = 10_000_000

    skewed_regions = ["US"] * int(0.7 * num_rows) + \
                    ["FR"] * int(0.1 * num_rows) + \
                    ["DE"] * int(0.1 * num_rows) + \
                    ["JP"] * int(0.1 * num_rows)
    
    print("generating data for ex1...")
    rdd = spark.sparkContext.parallelize(range(num_rows)) \
        .map(lambda i: (i, random.choice(skewed_regions)))
    
    df = rdd.toDF(["log_id", "region"])

    df.write.mode("overwrite").parquet(f"{base_path}/ex1_data")
    print("ex1 data generated. ***")

# ------------------------------------
# 2. transactions & countries
# ------------------------------------
def generate_transactions_and_countries():
    # small dataset
    print("generating data for ex2...")
    countries = [("FR", "France"), ("DE", "Germany"), ("JP", "Japan"), ("US", "United States")]
    country_df = spark.createDataFrame(countries, ["country_code", "country_name"])
    country_df.write.mode("overwrite").parquet(f"{base_path}/ex2_data_countries")

    num_rows = 5_000_000
    def get_random_transaction(i):
        return (i,
                    f"user_{random.randint(1, 500_000)}", random.choice(["FR", "DE", "JP", "US"]), \
                    round(random.uniform(10, 500), 2)
                )
    
    rdd = spark.sparkContext.parallelize(range(num_rows)).map(get_random_transaction)
    df = rdd.toDF(["transaction_id", "user_id", "country_code", "amount"])
    df.write.mode("overwrite").parquet(f"{base_path}/ex2_data_transations")
    print("ex2 data generated. ***")

# ----------------------------
# 3. small files (10 lines each)
# ----------------------------
def generate_small_files():
    import os
    import shutil

    print("generating data for ex3...")
    output_dir = f"{base_path}/ex3_data"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    os.makedirs(output_dir, exist_ok=True)

    # 1000 files - 10 lines each
    for i in range(1000):
        df = spark.range(10).withColumn("file_id", lit(i))
        df.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/part_{i}", header=True)

    print("ex3 data generated. ***")

# ----------------------------
# 4. big data
# ----------------------------
def generate_big_dataset():
    num_rows = 500_000_000
    print("generating data for ex4...")
    rdd = spark.sparkContext.parallelize(range(num_rows)).map(
        lambda i: (i, random.choice(["A", "B", "C", "D"]), round(random.uniform(0, 1000), 2))
    )
    df = rdd.toDF(["id", "category", "value"])
    df.write.mode("overwrite").parquet(f"{base_path}/ex4_data")
    print("ex4 data generated. ***")


# --------------------------

generate_skewed_logs()
generate_transactions_and_countries()
generate_small_files()
generate_big_dataset()
