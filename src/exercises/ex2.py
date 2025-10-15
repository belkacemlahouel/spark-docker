
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ex2") \
    .getOrCreate()

large_df = spark.read.parquet("./data/transactions/")
small_df = spark.read.parquet("./data/countries/")

joined = large_df.join(small_df, "country")
joined.groupBy("country_name").count().show()
