import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("cards_scd2")


CURRENT_PATH = "/warehouse/current/cards"
INCOMING_PATH = "/warehouse/simulate/cards_batch2"

SCD2_PATH = "/warehouse/history/cards_scd2"
SCD2_TMP_PATH = "/warehouse/history/cards_scd2_tmp"

BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

BUSINESS_COLUMNS = [
    "card_id",
    "client_id",
    "card_brand",
    "card_type",
    "card_number",
    "expires",
    "cvv",
    "has_chip",
    "num_cards_issued",
    "credit_limit",
    "acct_open_date",
    "year_pin_last_changed",
    "card_on_dark_web",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("cards_scd2")
        .getOrCreate()
    )


def path_exists(spark: SparkSession, path: str) -> bool:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path
    return fs.exists(Path(path))


def swap_hdfs_path(spark: SparkSession, src_path: str, dst_path: str) -> None:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path

    src = Path(src_path)
    dst = Path(dst_path)

    if fs.exists(dst):
        fs.delete(dst, True)

    ok = fs.rename(src, dst)
    if not ok:
        raise RuntimeError(f"Failed to rename {src_path} -> {dst_path}")


def normalize_cards(df: DataFrame) -> DataFrame:
    columns = set(df.columns)

    if "id" in columns:
        id_col = F.col("id").cast("long").alias("card_id")
    elif "card_id" in columns:
        id_col = F.col("card_id").cast("long").alias("card_id")
    else:
        raise ValueError(
            f"Cannot find card identifier column. Available columns: {df.columns}"
        )

    if "client_id" not in columns:
        raise ValueError(
            f"Cannot find client_id column. Available columns: {df.columns}"
        )

    required_columns = [
        "card_brand",
        "card_type",
        "card_number",
        "expires",
        "cvv",
        "has_chip",
        "num_cards_issued",
        "credit_limit",
        "acct_open_date",
        "year_pin_last_changed",
        "card_on_dark_web",
    ]

    missing_columns = [c for c in required_columns if c not in columns]
    if missing_columns:
        raise ValueError(
            f"Missing required card columns: {missing_columns}. Available columns: {df.columns}"
        )

    return (
        df.select(
            id_col,
            F.col("client_id").cast("long").alias("client_id"),
            F.col("card_brand"),
            F.col("card_type"),
            F.col("card_number"),
            F.col("expires"),
            F.col("cvv"),
            F.col("has_chip"),
            F.col("num_cards_issued"),
            F.col("credit_limit"),
            F.col("acct_open_date"),
            F.col("year_pin_last_changed"),
            F.col("card_on_dark_web"),
        )
    )


def build_change_condition() -> F.Column:
    compare_cols = [
        "client_id",
        "card_brand",
        "card_type",
        "card_number",
        "expires",
        "cvv",
        "has_chip",
        "num_cards_issued",
        "credit_limit",
        "acct_open_date",
        "year_pin_last_changed",
        "card_on_dark_web",
    ]

    condition = None
    for c in compare_cols:
        expr = ~F.col(f"cur.{c}").eqNullSafe(F.col(f"inc.{c}"))
        condition = expr if condition is None else (condition | expr)

    return condition


def create_initial_scd2(incoming_cards: DataFrame) -> DataFrame:
    return (
        incoming_cards
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(BATCH_ID))
    )


def main() -> None:
    spark = build_spark()

    raw_current_df = spark.read.parquet(CURRENT_PATH)
    raw_incoming_df = spark.read.parquet(INCOMING_PATH)

    logger.info("Current cards columns: %s", raw_current_df.columns)
    logger.info("Incoming cards columns: %s", raw_incoming_df.columns)

    current_cards = normalize_cards(raw_current_df)
    incoming_cards = normalize_cards(raw_incoming_df)

    # First run: initialize full SCD2 from incoming batch
    if not path_exists(spark, SCD2_PATH):
        logger.info("cards_scd2 does not exist yet. Initializing first version.")
        initial_scd2 = create_initial_scd2(incoming_cards)

        logger.info("Writing initial cards_scd2 to temp path: %s", SCD2_TMP_PATH)
        initial_scd2.coalesce(1).write.mode("overwrite").parquet(SCD2_TMP_PATH)

        logger.info("Promoting temp path to final SCD2 path: %s", SCD2_PATH)
        swap_hdfs_path(spark, SCD2_TMP_PATH, SCD2_PATH)

        logger.info("Initialized cards_scd2 successfully")
        spark.stop()
        return

    logger.info("Reading existing SCD2 table from %s", SCD2_PATH)
    existing_scd2 = spark.read.parquet(SCD2_PATH)

    current_scd2 = existing_scd2.filter(F.col("is_current") == True)

    joined = (
        current_scd2.alias("cur")
        .join(
            incoming_cards.alias("inc"),
            F.col("cur.card_id") == F.col("inc.card_id"),
            "fullouter"
        )
    )

    # INSERT: card exists in incoming but not in current SCD2
    insert_condition = (
        F.col("cur.card_id").isNull() &
        F.col("inc.card_id").isNotNull()
    )

    inserted_rows = (
        joined.filter(insert_condition)
        .select(*[F.col(f"inc.{c}").alias(c) for c in BUSINESS_COLUMNS])
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(BATCH_ID))
    )

    # UPDATE: card exists in both but one or more attributes changed
    update_condition = (
        F.col("cur.card_id").isNotNull() &
        F.col("inc.card_id").isNotNull() &
        build_change_condition()
    )

    updated_card_ids = (
        joined.filter(update_condition)
        .select(F.col("cur.card_id").alias("card_id"))
        .distinct()
    )

    # Close old current versions
    closed_versions = (
        existing_scd2.alias("scd")
        .join(
            updated_card_ids.alias("u"),
            (F.col("scd.card_id") == F.col("u.card_id")) & (F.col("scd.is_current") == True),
            "inner"
        )
        .select("scd.*")
        .withColumn("effective_to", F.current_timestamp())
        .withColumn("is_current", F.lit(False))
    )

    # Keep all existing rows except the current rows being replaced
    unchanged_rows = (
        existing_scd2.alias("scd")
        .join(
            updated_card_ids.alias("u"),
            (F.col("scd.card_id") == F.col("u.card_id")) & (F.col("scd.is_current") == True),
            "left_anti"
        )
    )

    # Insert new current versions for updated cards
    new_versions = (
        joined.filter(update_condition)
        .select(*[F.col(f"inc.{c}").alias(c) for c in BUSINESS_COLUMNS])
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("batch_id", F.lit(BATCH_ID))
    )

    final_scd2 = (
        unchanged_rows
        .unionByName(closed_versions)
        .unionByName(new_versions)
        .unionByName(inserted_rows)
    )

    logger.info("Writing updated cards_scd2 to temp path: %s", SCD2_TMP_PATH)
    final_scd2.coalesce(1).write.mode("overwrite").parquet(SCD2_TMP_PATH)

    logger.info("Promoting temp path to final SCD2 path: %s", SCD2_PATH)
    swap_hdfs_path(spark, SCD2_TMP_PATH, SCD2_PATH)

    logger.info("cards_scd2 updated successfully")
    spark.stop()


if __name__ == "__main__":
    main()
