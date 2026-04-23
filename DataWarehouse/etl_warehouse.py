import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
LOGGER = logging.getLogger("warehouse_etl")


# ============================================================
# GLOBALS
# ============================================================
CURRENT_YEAR = datetime.now().year


# ============================================================
# CONFIG
# ============================================================
@dataclass(frozen=True)
class WarehouseConfig:
    hive_database: str = "fintech_dw"

    users_source_path: str = "/warehouse/raw/users"
    cards_scd2_source_path: str = "/warehouse/history/cards_scd2"
    transactions_source_path: str = "/warehouse/raw/transactions"

    users_refined_output_path: str = "/warehouse/datawarehouse/users_refined"
    cards_refined_scd2_output_path: str = "/warehouse/datawarehouse/cards_refined_scd2"
    transactions_refined_output_path: str = "/warehouse/datawarehouse/transactions_refined"


# ============================================================
# SPARK FACTORY

# ============================================================
class SparkFactory:
    @staticmethod
    def create(app_name: str) -> SparkSession:
        return (
            SparkSession.builder
            .master("local[2]")
            .appName(app_name)
            .enableHiveSupport()
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "2")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.speculation", "false")
            .config("spark.sql.files.maxPartitionBytes", "134217728")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate()
        )
# ============================================================
# HIVE ADMIN
# ============================================================
class WarehouseAdmin:
    def __init__(self, spark: SparkSession, config: WarehouseConfig):
        self.spark = spark
        self.config = config

    def create_database(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.hive_database}")
        self.spark.sql(f"USE {self.config.hive_database}")
        LOGGER.info("Hive database ensured: %s", self.config.hive_database)


# ============================================================
# DATA EXTRACTOR
# ============================================================
class DataExtractor:
    def __init__(self, spark: SparkSession, config: WarehouseConfig):
        self.spark = spark
        self.config = config

    def read_users(self) -> DataFrame:
        LOGGER.info("Reading users from %s", self.config.users_source_path)
        return self.spark.read.parquet(self.config.users_source_path)

    def read_cards_scd2(self) -> DataFrame:
        LOGGER.info("Reading cards_scd2 from %s", self.config.cards_scd2_source_path)
        return self.spark.read.parquet(self.config.cards_scd2_source_path)

    def read_transactions(self) -> DataFrame:
        LOGGER.info("Reading transactions from %s", self.config.transactions_source_path)

        df = self.spark.read.parquet(self.config.transactions_source_path)

        required_cols = {"ingest_year", "ingest_month", "ingest_day"}
        if required_cols.issubset(set(df.columns)):
            latest_ingest = (
                df.select("ingest_year", "ingest_month", "ingest_day")
                  .distinct()
                  .orderBy(
                      F.col("ingest_year").desc(),
                      F.col("ingest_month").desc(),
                      F.col("ingest_day").desc()
                  )
                  .first()
            )

            latest_year = latest_ingest["ingest_year"]
            latest_month = latest_ingest["ingest_month"]
            latest_day = latest_ingest["ingest_day"]

            LOGGER.info(
                "Reading latest transaction batch: %s-%s-%s",
                latest_year,
                latest_month,
                latest_day
            )

            return df.filter(
                (F.col("ingest_year") == latest_year)
                & (F.col("ingest_month") == latest_month)
                & (F.col("ingest_day") == latest_day)
            )

        LOGGER.warning(
            "Transactions source has no ingest partition columns. Falling back to full read."
        )
        return df

# ============================================================
# QUALITY HELPERS
# ============================================================
class DataQualityUtils:
    @staticmethod
    def has_columns(df: DataFrame, required_cols: Set[str]) -> bool:
        return required_cols.issubset(set(df.columns))

    @staticmethod
    def null_expr(column_name: str) -> F.Column:
        return F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == "")

    @staticmethod
    def valid_integer_expr(column_name: str) -> F.Column:
        return F.col(column_name).cast("string").rlike(r"^[0-9]+$")

    @staticmethod
    def valid_decimal_expr(column_name: str) -> F.Column:
        return F.col(column_name).cast("string").rlike(r"^-?[0-9]+(\.[0-9]+)?$")

    @staticmethod
    def valid_currency_expr(column_name: str) -> F.Column:
        return F.col(column_name).cast("string").rlike(r"^\$?[0-9,]+(\.[0-9]+)?$")

    @staticmethod
    def currency_amount_expr(column_name: str) -> F.Column:
        return F.regexp_replace(F.col(column_name).cast("string"), r"[\$,]", "")

    @staticmethod
    def currency_code_expr(column_name: str) -> F.Column:
        return (
            F.when(DataQualityUtils.null_expr(column_name), F.lit(None).cast("string"))
             .when(F.col(column_name).cast("string").startswith("$"), F.lit("USD"))
             .otherwise(F.lit("UNKNOWN"))
        )

    @staticmethod
    def add_integer_quality_columns(
        df: DataFrame,
        source_column: str,
        output_column: str,
        output_type: str,
        raw_column: Optional[str] = None,
    ) -> DataFrame:
        raw_column = raw_column or f"{output_column}_raw"

        return (
            df.withColumn(raw_column, F.col(source_column).cast("string"))
              .withColumn(f"{output_column}_null_flag", DataQualityUtils.null_expr(source_column))
              .withColumn(
                  f"{output_column}_unknown_flag",
                  (~DataQualityUtils.null_expr(source_column))
                  & (~DataQualityUtils.valid_integer_expr(source_column))
              )
              .withColumn(
                  output_column,
                  F.when(
                      DataQualityUtils.valid_integer_expr(source_column),
                      F.col(source_column).cast(output_type)
                  ).otherwise(F.lit(None).cast(output_type))
              )
        )

    @staticmethod
    def add_decimal_quality_columns(
        df: DataFrame,
        source_column: str,
        output_column: str,
        raw_column: Optional[str] = None,
    ) -> DataFrame:
        raw_column = raw_column or f"{output_column}_raw"

        return (
            df.withColumn(raw_column, F.col(source_column).cast("string"))
              .withColumn(f"{output_column}_null_flag", DataQualityUtils.null_expr(source_column))
              .withColumn(
                  f"{output_column}_unknown_flag",
                  (~DataQualityUtils.null_expr(source_column))
                  & (~DataQualityUtils.valid_decimal_expr(source_column))
              )
              .withColumn(
                  output_column,
                  F.when(
                      DataQualityUtils.valid_decimal_expr(source_column),
                      F.col(source_column).cast("double")
                  ).otherwise(F.lit(None).cast("double"))
              )
        )

    @staticmethod
    def add_currency_quality_columns(
        df: DataFrame,
        source_column: str,
        raw_output_column: str,
        amount_output_column: str,
        currency_output_column: str,
    ) -> DataFrame:
        return (
            df.withColumn(raw_output_column, F.col(source_column).cast("string"))
              .withColumn(f"{amount_output_column}_null_flag", DataQualityUtils.null_expr(source_column))
              .withColumn(
                  f"{amount_output_column}_unknown_flag",
                  (~DataQualityUtils.null_expr(source_column))
                  & (~DataQualityUtils.valid_currency_expr(source_column))
              )
              .withColumn(
                  amount_output_column,
                  F.when(
                      DataQualityUtils.valid_currency_expr(source_column),
                      DataQualityUtils.currency_amount_expr(source_column).cast("double")
                  ).otherwise(F.lit(None).cast("double"))
              )
              .withColumn(
                  currency_output_column,
                  F.when(
                      DataQualityUtils.valid_currency_expr(source_column),
                      DataQualityUtils.currency_code_expr(source_column)
                  ).otherwise(F.lit("UNKNOWN"))
              )
        )


# ============================================================
# USERS REFINED BUILDER
# ============================================================
class UsersRefinedBuilder:
    def __init__(self, current_year: int):
        self.current_year = current_year

    def build(self, source_df: DataFrame) -> DataFrame:
        df = source_df

        user_id_source = "id" if "id" in df.columns else "user_id"

        df = df.withColumn("user_id_raw", F.col(user_id_source).cast("string"))
        df = df.withColumn("user_id_null_flag", DataQualityUtils.null_expr(user_id_source))
        df = df.withColumn(
            "user_id_unknown_flag",
            (~DataQualityUtils.null_expr(user_id_source))
            & (~DataQualityUtils.valid_integer_expr(user_id_source))
        )
        df = df.withColumn(
            "user_id",
            F.when(
                DataQualityUtils.valid_integer_expr(user_id_source),
                F.col(user_id_source).cast("bigint")
            ).otherwise(F.lit(None).cast("bigint"))
        )

        integer_specs = [
            ("current_age", "current_age", "int"),
            ("retirement_age", "retirement_age", "int"),
            ("birth_year", "birth_year", "int"),
            ("birth_month", "birth_month", "int"),
            ("credit_score", "credit_score", "int"),
            ("num_credit_cards", "num_credit_cards", "int"),
        ]

        for source_col, output_col, output_type in integer_specs:
            df = DataQualityUtils.add_integer_quality_columns(
                df=df,
                source_column=source_col,
                output_column=output_col,
                output_type=output_type,
                raw_column=f"{output_col}_raw"
            )

        df = DataQualityUtils.add_decimal_quality_columns(
            df=df,
            source_column="latitude",
            output_column="latitude",
            raw_column="latitude_raw"
        )

        df = DataQualityUtils.add_decimal_quality_columns(
            df=df,
            source_column="longitude",
            output_column="longitude",
            raw_column="longitude_raw"
        )

        df = DataQualityUtils.add_currency_quality_columns(
            df=df,
            source_column="per_capita_income",
            raw_output_column="per_capita_income_raw",
            amount_output_column="per_capita_income_amount",
            currency_output_column="per_capita_income_currency"
        )

        df = DataQualityUtils.add_currency_quality_columns(
            df=df,
            source_column="yearly_income",
            raw_output_column="yearly_income_raw",
            amount_output_column="yearly_income_amount",
            currency_output_column="yearly_income_currency"
        )

        df = DataQualityUtils.add_currency_quality_columns(
            df=df,
            source_column="total_debt",
            raw_output_column="total_debt_raw",
            amount_output_column="total_debt_amount",
            currency_output_column="total_debt_currency"
        )

        df = df.withColumn(
            "birth_year_outlier_flag",
            F.col("birth_year").isNotNull() & (F.col("birth_year") > F.lit(self.current_year))
        )
        df = df.withColumn(
            "current_age_outlier_flag",
            F.col("current_age").isNotNull() & (F.col("current_age") < 0)
        )
        df = df.withColumn(
            "retirement_age_outlier_flag",
            F.col("retirement_age").isNotNull() & (F.col("retirement_age") > 100)
        )

        df = df.withColumn(
            "age_group",
            F.when(F.col("current_age").isNull(), F.lit(None).cast("string"))
             .when((F.col("current_age") >= 0) & (F.col("current_age") < 18), F.lit("under_18"))
             .when((F.col("current_age") >= 18) & (F.col("current_age") <= 25), F.lit("18_25"))
             .when((F.col("current_age") >= 26) & (F.col("current_age") <= 40), F.lit("26_40"))
             .when((F.col("current_age") >= 41) & (F.col("current_age") <= 60), F.lit("41_60"))
             .otherwise(F.lit("over_60"))
        )

        df = df.withColumn(
            "income_group",
            F.when(F.col("yearly_income_amount").isNull(), F.lit(None).cast("string"))
             .when(F.col("yearly_income_amount") < 30000, F.lit("low_income"))
             .when(
                 (F.col("yearly_income_amount") >= 30000)
                 & (F.col("yearly_income_amount") < 70000),
                 F.lit("middle_income")
             )
             .otherwise(F.lit("high_income"))
        )

        if DataQualityUtils.has_columns(df, {"ingest_year", "ingest_month", "ingest_day"}):
            df = df.withColumn(
                "load_day",
                F.to_date(
                    F.concat_ws(
                        "-",
                        F.col("ingest_year").cast("string"),
                        F.lpad(F.col("ingest_month").cast("string"), 2, "0"),
                        F.lpad(F.col("ingest_day").cast("string"), 2, "0")
                    )
                )
            )
        else:
            df = df.withColumn("load_day", F.current_date())

        return df.select(
            "user_id",
            "user_id_raw",
            "user_id_null_flag",
            "user_id_unknown_flag",

            "current_age",
            "current_age_raw",
            "current_age_null_flag",
            "current_age_unknown_flag",
            "current_age_outlier_flag",

            "retirement_age",
            "retirement_age_raw",
            "retirement_age_null_flag",
            "retirement_age_unknown_flag",
            "retirement_age_outlier_flag",

            "birth_year",
            "birth_year_raw",
            "birth_year_null_flag",
            "birth_year_unknown_flag",
            "birth_year_outlier_flag",

            "birth_month",
            "birth_month_raw",
            "birth_month_null_flag",
            "birth_month_unknown_flag",

            "gender",
            "address",

            "latitude",
            "latitude_raw",
            "latitude_null_flag",
            "latitude_unknown_flag",

            "longitude",
            "longitude_raw",
            "longitude_null_flag",
            "longitude_unknown_flag",

            "per_capita_income_raw",
            "per_capita_income_amount",
            "per_capita_income_amount_null_flag",
            "per_capita_income_amount_unknown_flag",
            "per_capita_income_currency",

            "yearly_income_raw",
            "yearly_income_amount",
            "yearly_income_amount_null_flag",
            "yearly_income_amount_unknown_flag",
            "yearly_income_currency",

            "total_debt_raw",
            "total_debt_amount",
            "total_debt_amount_null_flag",
            "total_debt_amount_unknown_flag",
            "total_debt_currency",

            "credit_score",
            "credit_score_raw",
            "credit_score_null_flag",
            "credit_score_unknown_flag",

            "num_credit_cards",
            "num_credit_cards_raw",
            "num_credit_cards_null_flag",
            "num_credit_cards_unknown_flag",

            "age_group",
            "income_group",
            "load_day"
        )


# ============================================================
# CARDS SCD2 REFINED BUILDER
# ============================================================
class CardsRefinedSCD2Builder:
    def __init__(self, current_year: int):
        self.current_year = current_year

    def build(self, source_df: DataFrame) -> DataFrame:
        df = source_df

        card_id_source = "card_id" if "card_id" in df.columns else "id"

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column=card_id_source,
            output_column="card_id",
            output_type="bigint",
            raw_column="card_id_raw"
        )

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column="client_id",
            output_column="client_id",
            output_type="bigint",
            raw_column="client_id_raw"
        )

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column="num_cards_issued",
            output_column="num_cards_issued",
            output_type="int",
            raw_column="num_cards_issued_raw"
        )

        # hỗ trợ cả trường hợp source có credit_limit hoặc credit_limit_raw
        credit_limit_source = "credit_limit" if "credit_limit" in df.columns else "credit_limit_raw"

        df = DataQualityUtils.add_currency_quality_columns(
            df=df,
            source_column=credit_limit_source,
            raw_output_column="credit_limit_raw",
            amount_output_column="credit_limit_amount",
            currency_output_column="credit_limit_currency"
        )

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column="year_pin_last_changed",
            output_column="year_pin_last_changed",
            output_type="int",
            raw_column="year_pin_last_changed_raw"
        )

        df = df.withColumn(
            "year_pin_last_changed_outlier_flag",
            F.col("year_pin_last_changed").isNotNull()
            & (F.col("year_pin_last_changed") > F.lit(self.current_year))
        )

        if "acct_open_date" in df.columns:
            df = df.withColumn("acct_open_date_raw", F.col("acct_open_date").cast("string"))
            df = df.withColumn("acct_open_date_null_flag", DataQualityUtils.null_expr("acct_open_date"))
            df = df.withColumn(
                "acct_open_date",
                F.to_date(F.col("acct_open_date_raw"), "MM/yyyy")
            )
            df = df.withColumn(
                "acct_open_date_unknown_flag",
                (~DataQualityUtils.null_expr("acct_open_date_raw")) & F.col("acct_open_date").isNull()
            )
        else:
            df = df.withColumn("acct_open_date", F.lit(None).cast("date"))
            df = df.withColumn("acct_open_date_raw", F.lit(None).cast("string"))
            df = df.withColumn("acct_open_date_null_flag", F.lit(True))
            df = df.withColumn("acct_open_date_unknown_flag", F.lit(False))

        df = df.withColumn("effective_year", F.year("effective_from").cast("int"))
        df = df.withColumn("effective_month", F.month("effective_from").cast("int"))

        return df.select(
            "card_id",
            "card_id_raw",
            "card_id_null_flag",
            "card_id_unknown_flag",

            "client_id",
            "client_id_raw",
            "client_id_null_flag",
            "client_id_unknown_flag",

            "card_brand",
            "card_type",
            "card_number",
            "expires",
            "cvv",
            "has_chip",

            "num_cards_issued",
            "num_cards_issued_raw",
            "num_cards_issued_null_flag",
            "num_cards_issued_unknown_flag",

            "credit_limit_raw",
            "credit_limit_amount",
            "credit_limit_amount_null_flag",
            "credit_limit_amount_unknown_flag",
            "credit_limit_currency",

            "acct_open_date",
            "acct_open_date_raw",
            "acct_open_date_null_flag",
            "acct_open_date_unknown_flag",

            "year_pin_last_changed",
            "year_pin_last_changed_raw",
            "year_pin_last_changed_null_flag",
            "year_pin_last_changed_unknown_flag",
            "year_pin_last_changed_outlier_flag",

            "card_on_dark_web",
            "effective_from",
            "effective_to",
            "is_current",
            "batch_id",

            "effective_year",
            "effective_month"
        )


# ============================================================
# TRANSACTIONS REFINED BUILDER
# ============================================================
class TransactionsRefinedBuilder:
    def build(self, source_df: DataFrame) -> DataFrame:
        df = source_df

        transaction_id_source = "id" if "id" in df.columns else "transaction_id"

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column=transaction_id_source,
            output_column="transaction_id",
            output_type="bigint",
            raw_column="transaction_id_raw"
        )

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column="client_id",
            output_column="client_id",
            output_type="bigint",
            raw_column="client_id_raw"
        )

        df = DataQualityUtils.add_integer_quality_columns(
            df=df,
            source_column="card_id",
            output_column="card_id",
            output_type="bigint",
            raw_column="card_id_raw"
        )

        amount_source = "amount" if "amount" in df.columns else "amount_raw"

        df = DataQualityUtils.add_currency_quality_columns(
            df=df,
            source_column=amount_source,
            raw_output_column="amount_raw",
            amount_output_column="amount",
            currency_output_column="amount_currency"
        )

        date_source = "date" if "date" in df.columns else "txn_timestamp"
        df = df.withColumn("txn_timestamp", F.to_timestamp(F.col(date_source)))
        df = df.withColumn("txn_year", F.year("txn_timestamp").cast("int"))
        df = df.withColumn("txn_month", F.month("txn_timestamp").cast("int"))
        df = df.withColumn("txn_day", F.dayofmonth("txn_timestamp").cast("int"))

        return df.select(
            "transaction_id",
            "transaction_id_raw",
            "transaction_id_null_flag",
            "transaction_id_unknown_flag",

            "txn_timestamp",

            "client_id",
            "client_id_raw",
            "client_id_null_flag",
            "client_id_unknown_flag",

            "card_id",
            "card_id_raw",
            "card_id_null_flag",
            "card_id_unknown_flag",

            "amount_raw",
            "amount",
            "amount_null_flag",
            "amount_unknown_flag",
            "amount_currency",

            "use_chip",
            "merchant_id",
            "merchant_city",
            "merchant_state",
            "zip",
            "mcc",
            "errors",

            "txn_year",
            "txn_month",
            "txn_day"
        )


# ============================================================
# WRITER
# ============================================================
class RefinedWriter:
    def __init__(self, config: WarehouseConfig):
        self.config = config

    def write_users_refined(self, df: DataFrame) -> None:
        LOGGER.info("Start writing users_refined")

        (
            df.write
              .mode("overwrite")
              .partitionBy("load_day")
              .parquet(self.config.users_refined_output_path)
        )

        LOGGER.info(
            "users_refined written to %s",
            self.config.users_refined_output_path
        )

    def write_cards_refined_scd2(self, df: DataFrame) -> None:
        LOGGER.info("Start writing cards_refined_scd2")

        (
            df.write
              .mode("overwrite")
              .partitionBy("effective_year", "effective_month")
              .parquet(self.config.cards_refined_scd2_output_path)
        )

        LOGGER.info(
            "cards_refined_scd2 written to %s",
            self.config.cards_refined_scd2_output_path
        )

    def write_transactions_refined(self, df: DataFrame) -> None:
        LOGGER.info("Start preparing transactions_refined incremental batch")

        tx_df = (
            df
            .filter(
                F.col("txn_year").isNotNull()
                & F.col("txn_month").isNotNull()
                & F.col("txn_day").isNotNull()
            )
            .select(
                "transaction_id",
                "transaction_id_raw",
                "transaction_id_null_flag",
                "transaction_id_unknown_flag",
                "txn_timestamp",
                "client_id",
                "client_id_raw",
                "client_id_null_flag",
                "client_id_unknown_flag",
                "card_id",
                "card_id_raw",
                "card_id_null_flag",
                "card_id_unknown_flag",
                "amount_raw",
                "amount",
                "amount_null_flag",
                "amount_unknown_flag",
                "amount_currency",
                "use_chip",
                "merchant_id",
                "merchant_city",
                "merchant_state",
                "zip",
                "mcc",
                "errors",
                "txn_year",
                "txn_month",
                "txn_day",
            )
        )

        LOGGER.info("Collecting year-month chunks for transactions_refined")

        year_months = (
            tx_df.select("txn_year", "txn_month")
                 .distinct()
                 .orderBy("txn_year", "txn_month")
                 .collect()
        )

        LOGGER.info("Found %s transaction year-month chunks", len(year_months))

        for row in year_months:
            y = row["txn_year"]
            m = row["txn_month"]

            LOGGER.info(
                "Writing transactions_refined chunk year=%s month=%s",
                y,
                m
            )

            chunk_df = (
                tx_df
                .filter(
                    (F.col("txn_year") == y)
                    & (F.col("txn_month") == m)
                )
                .repartition(2, "txn_day")
            )

            (
                chunk_df.write
                    .mode("append")
                    .option("maxRecordsPerFile", 200000)
                    .partitionBy("txn_year", "txn_month", "txn_day")
                    .parquet(self.config.transactions_refined_output_path)
            )

            LOGGER.info(
                "Done transactions_refined chunk year=%s month=%s",
                y,
                m
            )

        LOGGER.info(
            "transactions_refined incremental append completed to %s",
            self.config.transactions_refined_output_path
        )
# ============================================================
# PIPELINE
class WarehousePipeline:
    def __init__(self, spark: SparkSession, config: WarehouseConfig):
        self.spark = spark
        self.config = config

        self.admin = WarehouseAdmin(spark, config)
        self.extractor = DataExtractor(spark, config)
        self.writer = RefinedWriter(config)

        self.users_builder = UsersRefinedBuilder(current_year=CURRENT_YEAR)
        self.cards_builder = CardsRefinedSCD2Builder(current_year=CURRENT_YEAR)
        self.transactions_builder = TransactionsRefinedBuilder()

    def run(self) -> None:
        LOGGER.info("Starting warehouse ETL pipeline")

        self.admin.create_database()

        LOGGER.info("Reading users source")
        users_source_df = self.extractor.read_users()

        LOGGER.info("Reading cards_scd2 source")
        cards_scd2_source_df = self.extractor.read_cards_scd2()

        LOGGER.info("Reading transactions source")
        transactions_source_df = self.extractor.read_transactions()

        LOGGER.info("Building users_refined")
        users_refined_df = self.users_builder.build(users_source_df)

        LOGGER.info("Building cards_refined_scd2")
        cards_refined_scd2_df = self.cards_builder.build(cards_scd2_source_df)

        LOGGER.info("Building transactions_refined")
        transactions_refined_df = self.transactions_builder.build(transactions_source_df)

        LOGGER.info("Writing users_refined")
        self.writer.write_users_refined(users_refined_df)

        LOGGER.info("Writing cards_refined_scd2")
        self.writer.write_cards_refined_scd2(cards_refined_scd2_df)

        LOGGER.info("Writing transactions_refined")
        self.writer.write_transactions_refined(transactions_refined_df)

        LOGGER.info("Warehouse ETL pipeline completed successfully")
# ============================================================
# ENTRYPOINT
# ============================================================

def main() -> None:
    config = WarehouseConfig()
    spark = SparkFactory.create(app_name="etl_warehouse")

    spark.sparkContext.setLogLevel("WARN")

    try:
        pipeline = WarehousePipeline(spark=spark, config=config)
        pipeline.run()
    finally:
        spark.stop()
        LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    main()
