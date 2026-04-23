import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if len(sys.argv) != 2:
    print("Usage: spark-submit query_transaction_index.py <transaction_id>")
    sys.exit(1)

transaction_id = int(sys.argv[1])

spark = (
    SparkSession.builder
    .appName("query_transaction_index")
    .getOrCreate()
)

INDEX_PATH = "/warehouse/raw/transaction_index"
TRANSACTIONS_PATH = "/warehouse/raw/transactions"

df_index = spark.read.parquet(INDEX_PATH)
df_txn = spark.read.parquet(TRANSACTIONS_PATH)

print("=== INDEX SCHEMA ===")
df_index.printSchema()

df_lookup = df_index.filter(col("transaction_id") == transaction_id)

if df_lookup.count() == 0:
    print(f"Transaction ID {transaction_id} NOT FOUND")
    spark.stop()
    sys.exit(0)

print(f"=== INDEX RESULT: transaction_id = {transaction_id} ===")
df_lookup.show(truncate=False)

df_result = (
    df_lookup.alias("idx")
    .join(
        df_txn.alias("txn"),
        col("idx.transaction_id") == col("txn.id"),
        "inner"
    )
    .select(
        col("idx.transaction_id"),
        col("idx.client_id"),
        col("idx.card_id"),
        col("idx.table_source"),
        col("idx.ingest_time"),
        col("idx.ingest_year"),
        col("idx.ingest_month"),
        col("idx.ingest_day"),

        col("txn.date"),
        col("txn.amount"),
        col("txn.merchant_id"),
        col("txn.merchant_city"),
        col("txn.merchant_state"),
        col("txn.errors")
    )
)

print("=== FULL TRANSACTION DATA ===")
df_result.show(truncate=False)

spark.stop()
