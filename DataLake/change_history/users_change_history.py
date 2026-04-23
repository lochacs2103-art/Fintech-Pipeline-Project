import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("users_change_history")


CURRENT_PATH = "/warehouse/current/users"
INCOMING_PATH = "/warehouse/simulate/users_batch2"

HISTORY_PATH = "/warehouse/history/users_history"
HISTORY_TMP_PATH = "/warehouse/history/users_history_tmp"

CURRENT_TMP_PATH = "/warehouse/current/users_tmp"

BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

BUSINESS_COLUMNS = [
    "user_id",
    "current_age",
    "retirement_age",
    "birth_year",
    "birth_month",
    "gender",
    "address",
    "latitude",
    "longitude",
    "per_capita_income",
    "yearly_income",
    "total_debt",
    "credit_score",
    "num_credit_cards",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("users_change_history")
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


def normalize_users(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").cast("long").alias("user_id"),
            F.col("current_age"),
            F.col("retirement_age"),
            F.col("birth_year"),
            F.col("birth_month"),
            F.col("gender"),
            F.col("address"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("per_capita_income"),
            F.col("yearly_income"),
            F.col("total_debt"),
            F.col("credit_score"),
            F.col("num_credit_cards"),
        )
    )


def build_change_condition() -> F.Column:
    compare_cols = [
        "current_age",
        "retirement_age",
        "birth_year",
        "birth_month",
        "gender",
        "address",
        "latitude",
        "longitude",
        "per_capita_income",
        "yearly_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
    ]

    condition = None
    for c in compare_cols:
        expr = ~F.col(f"cur.{c}").eqNullSafe(F.col(f"inc.{c}"))
        condition = expr if condition is None else (condition | expr)

    return condition


def build_history_delta(current_df: DataFrame, incoming_df: DataFrame) -> DataFrame:
    joined = (
        current_df.alias("cur")
        .join(
            incoming_df.alias("inc"),
            F.col("cur.user_id") == F.col("inc.user_id"),
            "fullouter"
        )
    )

    update_condition = (
        F.col("cur.user_id").isNotNull() &
        F.col("inc.user_id").isNotNull() &
        build_change_condition()
    )

    delete_condition = (
        F.col("cur.user_id").isNotNull() &
        F.col("inc.user_id").isNull()
    )

    updated_before_image = (
        joined.filter(update_condition)
        .select(*[F.col(f"cur.{c}").alias(c) for c in BUSINESS_COLUMNS])
        .withColumn("change_type", F.lit("UPDATE"))
        .withColumn("change_detected_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(BATCH_ID))
    )

    deleted_before_image = (
        joined.filter(delete_condition)
        .select(*[F.col(f"cur.{c}").alias(c) for c in BUSINESS_COLUMNS])
        .withColumn("change_type", F.lit("DELETE"))
        .withColumn("change_detected_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(BATCH_ID))
    )

    return updated_before_image.unionByName(deleted_before_image)


def main() -> None:
    spark = build_spark()

    logger.info("Reading current users from %s", CURRENT_PATH)
    current_users = normalize_users(spark.read.parquet(CURRENT_PATH))

    logger.info("Reading incoming users from %s", INCOMING_PATH)
    incoming_users = normalize_users(spark.read.parquet(INCOMING_PATH))

    logger.info("Detecting user changes")
    history_delta = build_history_delta(current_users, incoming_users)

    history_delta_count = history_delta.count()
    logger.info("Detected %s changed/deleted user rows", history_delta_count)

    if history_delta_count > 0:
        if path_exists(spark, HISTORY_PATH):
            logger.info("Reading existing users history from %s", HISTORY_PATH)
            existing_history = spark.read.parquet(HISTORY_PATH)
            final_history = existing_history.unionByName(history_delta)
        else:
            logger.info("History path does not exist yet, creating new history dataset")
            final_history = history_delta

        logger.info("Writing users history to temp path %s", HISTORY_TMP_PATH)
        final_history.coalesce(1).write.mode("overwrite").parquet(HISTORY_TMP_PATH)

        logger.info("Swapping temp history path into final history path")
        swap_hdfs_path(spark, HISTORY_TMP_PATH, HISTORY_PATH)
    else:
        logger.info("No changes detected. Users history remains unchanged.")

    logger.info("Refreshing current users snapshot")
    incoming_users.coalesce(1).write.mode("overwrite").parquet(CURRENT_TMP_PATH)
    swap_hdfs_path(spark, CURRENT_TMP_PATH, CURRENT_PATH)

    logger.info("Users change history job completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()
