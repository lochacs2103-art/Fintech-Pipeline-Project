from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = (
    SparkSession.builder
    .appName("simulate_cards_batch2")
    .getOrCreate()
)

SOURCE_PATH = "/warehouse/raw/cards"
OUTPUT_PATH = "/warehouse/simulate/cards_batch2"

TARGET_CARD_ID = 1

df = spark.read.parquet(SOURCE_PATH)

df_batch2 = (
    df.withColumn(
        "credit_limit",
        when(col("id") == TARGET_CARD_ID, lit(99999.0)).otherwise(col("credit_limit"))
    )
    .withColumn(
        "has_chip",
        when(col("id") == TARGET_CARD_ID, lit("NO")).otherwise(col("has_chip"))
    )
    .withColumn(
        "card_type",
        when(col("id") == TARGET_CARD_ID, lit("UPDATED_TYPE")).otherwise(col("card_type"))
    )
)

df_batch2.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Created: {OUTPUT_PATH}")
df_batch2.filter(col("id") == TARGET_CARD_ID).show(truncate=False)

spark.stop()
