from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = (
    SparkSession.builder
    .appName("simulate_users_batch2")
    .getOrCreate()
)

SOURCE_PATH = "/warehouse/raw/users"
OUTPUT_PATH = "/warehouse/simulate/users_batch2"

TARGET_USER_ID = 825

df = spark.read.parquet(SOURCE_PATH)

df_batch2 = (
    df.withColumn(
        "yearly_income",
        when(col("id") == TARGET_USER_ID, "$999999").otherwise(col("yearly_income"))
    )
    .withColumn(
        "total_debt",
        when(col("id") == TARGET_USER_ID, "$12345").otherwise(col("total_debt"))
    )
    .withColumn(
        "address",
        when(col("id") == TARGET_USER_ID, "462 Rose Lane UPDATED").otherwise(col("address"))
    )
)

df_batch2.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Created: {OUTPUT_PATH}")
df_batch2.filter(col("id") == TARGET_USER_ID).show(truncate=False)

spark.stop()
