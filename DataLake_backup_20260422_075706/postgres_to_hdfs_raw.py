from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, year, month, dayofmonth, lit, col

spark = (
    SparkSession.builder
    .appName("postgres_to_hdfs_raw")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://172.31.64.1:5432/fintech_db"

jdbc_properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}

tables = [
    {
        "name": "users",
        "dbtable": "raw_users",
        "output_path": "hdfs:///warehouse/raw/users",
        "table_source": "users_data.csv"
    },
    {
        "name": "cards",
        "dbtable": "raw_cards",
        "output_path": "hdfs:///warehouse/raw/cards",
        "table_source": "cards_data.csv"
    },
    {
        "name": "transactions",
        "dbtable": "raw_transactions",
        "output_path": "hdfs:///warehouse/raw/transactions",
        "table_source": "transactions_data.csv"
    }
]

for t in tables:
    print(f"START READING: {t['dbtable']}")

    if t["name"] == "transactions":
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", t["dbtable"])
            .option("user", jdbc_properties["user"])
            .option("password", jdbc_properties["password"])
            .option("driver", jdbc_properties["driver"])
            .option("fetchsize", 10000)
            .load()
        )
    else:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=t["dbtable"],
            properties=jdbc_properties
        )

    df_out = (
        df.withColumn("table_source", lit(t["table_source"]))
          .withColumn("ingest_time", current_timestamp())
          .withColumn("ingest_year", year(col("ingest_time")))
          .withColumn("ingest_month", month(col("ingest_time")))
          .withColumn("ingest_day", dayofmonth(col("ingest_time")))
    )

    if t["name"] == "transactions":
        df_out = df_out.repartition(8)

    print(f"START WRITING: {t['output_path']}")

    (
        df_out.write
        .mode("overwrite")
        .partitionBy("ingest_year", "ingest_month", "ingest_day")
        .parquet(t["output_path"])
    )

    print(f"WRITE OK: {t['dbtable']} -> {t['output_path']}")

spark.stop()
