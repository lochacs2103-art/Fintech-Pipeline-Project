from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("build_transaction_index")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Đọc bảng raw transactions đã ingest vào HDFS
transactions_path = "hdfs:///warehouse/raw/transactions"
output_path = "hdfs:///warehouse/raw/transaction_index"

df = spark.read.parquet(transactions_path)

# Chọn các cột cần cho index table
transaction_index = (
    df.select(
        col("id").alias("transaction_id"),
        col("client_id"),
        col("card_id"),
        col("table_source"),
        col("ingest_time"),
        col("ingest_year"),
        col("ingest_month"),
        col("ingest_day")
    )
    .dropDuplicates(["transaction_id"])
)

# Ghi ra HDFS dưới dạng parquet, partition theo ingest date
(
    transaction_index.write
    .mode("overwrite")
    .partitionBy("ingest_year", "ingest_month", "ingest_day")
    .parquet(output_path)
)

print("Transaction index table created successfully.")
print(f"Output path: {output_path}")

spark.stop()
