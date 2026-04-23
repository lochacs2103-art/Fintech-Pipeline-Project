import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
LOGGER = logging.getLogger("warehouse_etl")


# =========================================================
# CONSTANTS
# =========================================================
CURRENT_YEAR = datetime.now().year


# =========================================================
# CONFIG
# =========================================================
@dataclass(frozen=True)
class WarehouseConfig:
    hive_database: str = "fintech_dw"

    raw_users_path: str = "/warehouse/raw/users"
    raw_cards_path: str = "/warehouse/raw/cards"
    raw_transactions_path: str = "/warehouse/raw/transactions"

    cards_scd2_path: str = "/warehouse/history/cards_scd2"

    users_refined_path: str = "/warehouse/datawarehouse/users_refined"
    cards_refined_scd2_path: str = "/warehouse/datawarehouse/cards_refined_scd2"
    transactions_refined_path: str = "/warehouse/datawarehouse/transactions_refined"


# =========================================================
# SPARK FACTORY
# =========================================================
class SparkFactory:
    @staticmethod
    def create(app_name: str) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark


# =========================================================
# HDFS / HIVE ADMIN
# =========================================================
class WarehouseAdmin:
    def __init__(self, spark: SparkSession, config: WarehouseConfig):
        self.spark = spark
        self.config = config

    def create_database(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.hive_database}")
        self.spark.sql(f"USE {self.config.hive_database}")
        LOGGER.info("Hive database ensured: %s", self.config.hive_database)


# =========================================================
# VALIDATION / CLEANSING HELPERS
# =========================================================
class DataQualityUtils:
    @staticmethod
    def is_null_expr(column_name: str) -> F.Column:
        return F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == "")

    @staticmethod
    def is_valid_integer_expr(column_name: str) -> F.Column:
        return F.col(column_name).cast("string").rlike(r"^[0-9]+$")

    @staticmethod
    def is_valid_signed_decimal_expr(column_name: str) -> F.Column:
        return F.col(column_name).cast("string").rlike(r"^-?[0-9]+(\.[0-9]+)?$")

    @staticmethod
    def is_valid_currency_expr(column_name: str) -> F.Column:
        return F.col(column_name).cast("string").rlike(r"^\$?[0-9,]+(\.[0-9]+)?$")

    @staticmethod
    def currency_code_expr(column_name: str) -> F.Column:
        return (
            F.when(F.col(column_name).cast("string").startswith("$"), F.lit("USD"))
             .when(DataQualityUtils.is_null_expr(column_name), F.lit(None).cast("string"))
             .otherwise(F.lit("UNKNOWN"))
        )

    @staticmethod
    def currency_amount_expr(column_name: str) -> F.Column:
        return F.regexp_replace(F.col(column_name).cast("string"), r"[\$,]", "").cast("double")

    @staticmethod
    def with_integer_quality_columns(
        df: DataFrame,
        source_column: str,
        output_column: str,
        output_type: str = "bigint",
        raw_column: Optional[str] = None,
    ) -> DataFrame:
        raw_column = raw_column or f"{output_column}_raw"

        return (
            df.withColumn(raw_column, F.col(source_column).cast("string"))
              .withColumn(f"{output_column}_null_flag", DataQualityUtils.is_null_expr(source_column))
              .withColumn(
                  f"{output_column}_unknown_flag",
                  (~DataQualityUtils.is_null_expr(source_column))
                  & (~DataQualityUtils.is_valid_integer_expr(source_column))
              )
              .withColumn(
                  output_column,
                  F.when(
                      DataQualityUtils.is_valid_integer_expr(source_column),
                      F.col(source_column).cast(output_type)
                  ).otherwise(F.lit(None).cast(output_type))
              )
        )

    @staticmethod
    def with_decimal_quality_columns(
        df: DataFrame,
        source_column: str,
        output_column: str,
        raw_column: Optional[str] = None,
    ) -> DataFrame:
        raw_column = raw_column or f"{output_column}_raw"

        return (
            df.withColumn(raw_column, F.col(source_column).cast("string"))
              .withColumn(f"{output_column}_null_flag", DataQualityUtils.is_null_expr(source_column))
              .withColumn(
                  f"{output_column}_unknown_flag",
                  (~DataQualityUtils.is_null_expr(source_column))
                  & (~DataQualityUtils.is_valid_signed_decimal_expr(source_column))
              )
              .withColumn(
                  output_column,
                  F.when(
                      DataQualityUtils.is_valid_signed_decimal_expr(source_column),
                      F.col(source_column).cast("double")
                  ).otherwise(F.lit(None).cast("double"))
              )
        )

    @staticmethod
    def with_currency_quality_columns(
        df: DataFrame,
        source_column: str,
        raw_output_column: str,
        amount_output_column: str,
        currency_output_column: str,
    ) -> DataFrame:
        return (
            df.withColumn(raw_output_column, F.col(source_column).cast("string"))
              .withColumn(f"{amount_output_column}_null_flag", DataQualityUtils.is_null_expr(source_column))
              .withColumn(
                  f"{amount_output_column}_unknown_flag",
                  (~DataQualityUtils.is_null_expr(source_column))
                  & (~DataQualityUtils.is_valid_currency_expr(source_column))
              )
              .withColumn(
                  amount_output_column,
                  F.when(
                      DataQualityUtils.is_valid_currency_expr(source_column),
                      DataQualityUtils.currency_amount_expr(source_column)
                  ).otherwise(F.lit(None).cast("double"))
              )
              .withColumn(
                  currency_output_column,
                  F.when(
                      DataQualityUtils.is_valid_currency_expr(source_column),
                      DataQualityUtils.currency_code_expr(source_column)
                  ).otherwise(F.lit("UNKNOWN"))
              )
        )


# =========================================================
# EXTRACTORS
# =========================================================
class DataExtractor:
    def __init__(self, spark: SparkSession, config: WarehouseConfig):
        self.spark = spark
        self.config = config

    def read_users(self) -> DataFrame:
        LOGGER.info("Reading users from %s", self.config.raw_users_path)
        return self.spark.read.parquet(self.config.raw_users_path)

    def read_cards_scd2(self) -> DataFrame:
        LOGGER.info("Reading cards_scd2 from %s", self.config.cards_scd2_path)
        return self.spark.read.parquet(self.config.cards_scd2_path)

    def read_transactions(self) -> DataFrame:
        LOGGER.info("Reading transactions from %s", self.config.raw_transactions_path)
        return self.spark.read.parquet(self.config.raw_transactions_path)


# =========================================================
# USERS BUILDER
# =========================================================
class UsersRefinedBuilder:
    def __init__(self, current_year: int):
        self.current_year = current_year

    def build(self, source_df: DataFrame) -> DataFrame:
        df = source_df

        # ---------------------------------------------
        # ID
        # ---------------------------------------------
        df = df.withColumn("user_id_raw", F.col("user_id").cast("string"))
        df = df.withColumn("user_id_null_flag", DataQualityUtils.is_null_expr("user_id"))
        df = df.withColumn(
            "user_id_unknown_flag",
            (~DataQualityUtils.is_null_expr("user_id"))
            & (~DataQualityUtils.is_valid_integer_expr("user_id"))
        )
        df = df.withColumn(
            "user_id",
            F.when(
                DataQualityUtils.is_valid_integer_expr("user_id"),
                F.col("user_id").cast("bigint")
            ).otherwise(F.lit(None).cast("bigint"))
        )

        # ---------------------------------------------
        # INTEGER COLUMNS
        # ---------------------------------------------
        integer_columns = [
            ("current_age", "current_age", "int"),
            ("retirement_age", "retirement_age", "int"),
            ("birth_year", "birth_year", "int"),
            ("birth_month", "birth_month", "int"),
            ("credit_score", "credit_score", "int"),
            ("num_credit_cards", "num_credit_cards", "int"),
        ]

        for source_column, output_column, output_type in integer_columns:
            df = DataQualityUtils.with_integer_quality_columns(
                df=df,
                source_column=source_column,
                output_column=output_column,
                output_type=output_type,
                raw_column=None
            )

        # ---------------------------------------------
        # DECIMAL COLUMNS
        # ---------------------------------------------
        df = DataQualityUtils.with_decimal_quality_columns(df, "latitude", "latitude", "latitude_raw")
        df = DataQualityUtils.with_decimal_quality_columns(df, "longitude", "longitude", "longitude_raw")

        # ---------------------------------------------
        # CURRENCY COLUMNS
        # ---------------------------------------------
        df = DataQualityUtils.with_currency_quality_columns(
            df,
            source_column="per_capita_income",
            raw_output_column="per_capita_income_raw",
            amount_output_column="per_capita_income_amount",
            currency_output_column="per_capita_income_currency"
        )

        df = DataQualityUtils.with_currency_quality_columns(
            df,
            source_column="yearly_income",
            raw_output_column="yearly_income_raw",
            amount_output_column="yearly_income_amount",
            currency_output_column="yearly_income_currency"
        )

        df = DataQualityUtils.with_currency_quality_columns(
            df,
            source_column="total_debt",
            raw_output_column="total_debt_raw",
            amount_output_column="total_debt_amount",
            currency_output_column="total_debt_currency"
        )

        # ---------------------------------------------
        # OUTLIER FLAGS
        # ---------------------------------------------
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

        # ---------------------------------------------
        # BUSINESS GROUPS
        # ---------------------------------------------
        df = df.withColumn(
            "age_group",
            F.when(F.col("current_age").isNull(), F.lit(None).cast("string"))
             .when(F.col("current_age") < 18, F.lit("under_18"))
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
                 & (F.col("yearly_income_amount") <= 70000),
                 F.lit("middle_income")
             )
             .otherwise(F.lit("high_income"))
        )

        # ---------------------------------------------
        # LOAD DAY
        # ---------------------------------------------
        df = df.withColumn("load_day", F.to_date("ingest_time"))

        # ---------------------------------------------
        # FINAL PROJECTION
        # ---------------------------------------------
        final_df = df.select(
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

        return final_df


# =========================================================
# CARDS BUILDER
# =========================================================
class CardsRefinedSCD2Builder:
    def __init__(self, current_year: int):
        self.current_year = current_year

    def build(self, source_df: DataFrame) -> DataFrame:
        df = source_df

        card_id_source = "card_id" if "card_id" in df.columns else "id"

        df = DataQualityUtils.with_integer_quality_columns(
            df=df,
            source_column=card_id_source,
            output_column="card_id",
            output_type="bigint",
            raw_column="card_id_raw"
        )

        df = DataQualityUtils.with_integer_quality_columns(
            df=df,
            source_column="client_id",
            output_column="client_id",
            output_type="bigint",
            raw_column="client_id_raw"
        )

        df = DataQualityUtils.with_integer_quality_columns(
            df=df,
            source_column="num_cards_issued",
            output_column="num_cards_issued",
            output_type="int",
            raw_column="num_cards_issued_raw"
        )

        df = DataQualityUtils.with_currency_quality_columns(
            df,
            source_column="credit_limit",
            raw_output_column="credit_limit_raw",
            amount_output_column="credit_limit_amount",
            currency_output_column="credit_limit_currency"
        )

        df = DataQualityUtils.with_integer_quality_columns(
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

        df = df.withColumn("acct_open_date_raw", F.col("acct_open_date").cast("string"))
        df = df.withColumn("acct_open_date", F.to_date("acct_open_date"))
        df = df.withColumn(
            "acct_open_date_null_flag",
            DataQualityUtils.is_null_expr("acct_open_date_raw")
        )
        df = df.withColumn(
            "acct_open_date_unknown_flag",
            (~DataQualityUtils.is_null_expr("acct_open_date_raw"))
            & F.col("acct_open_date").isNull()
        )

        df = df.withColumn("effective_year", F.year("effective_from").cast("int"))
        df = df.withColumn("effective_month", F.month("effective_from").cast("int"))

        final_df = df.select(
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

        return final_df


# =========================================================
# TRANSACTIONS BUILDER
# =========================================================
class TransactionsRefinedBuilder:
    def build(self, source_df: DataFrame) -> DataFrame:
        df = source_df

        df = DataQualityUtils.with_integer_quality_columns(
            df=df,
            source_column="id",
            output_column="transaction_id",
            output_type="bigint",
            raw_column="transaction_id_raw"
        )

        df = DataQualityUtils.with_integer_quality_columns(
            df=df,
            source_column="client_id",
            output_column="client_id",
            output_type="bigint",
            raw_column="client_id_raw"
        )

        df = DataQualityUtils.with_integer_quality_columns(
            df=df,
            source_column="card_id",
            output_column="card_id",
            output_type="bigint",
            raw_column="card_id_raw"
        )

        df = DataQualityUtils.with_currency_quality_columns(
            df,
            source_column="amount",
            raw_output_column="amount_raw",
            amount_output_column="amount",
            currency_output_column="amount_currency"
        )

        df = df.withColumn("txn_timestamp", F.to_timestamp("date"))
        df = df.withColumn("txn_year", F.year("txn_timestamp").cast("int"))
        df = df.withColumn("txn_month", F.month("txn_timestamp").cast("int"))
        df = df.withColumn("txn_day", F.dayofmonth("txn_timestamp").cast("int"))

        final_df = df.select(
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

        return final_df


# =========================================================
# WRITERS
# =========================================================
class RefinedWriter:
    def __init__(self, config: WarehouseConfig):
        self.config = config

    def write_users_refined(self, df: DataFrame) -> None:
        (
            df.write
            .mode("overwrite")
            .partitionBy("load_day")
            .parquet(self.config.users_refined_path)
        )
        LOGGER.info("users_refined written to %s", self.config.users_refined_path)

    def write_cards_refined_scd2(self, df: DataFrame) -> None:
        (
            df.write
            .mode("overwrite")
            .partitionBy("effective_year", "effective_month")
            .parquet(self.config.cards_refined_scd2_path)
        )
        LOGGER.info("cards_refined_scd2 written to %s", self.config.cards_refined_scd2_path)

    def write_transactions_refined(self, df: DataFrame) -> None:
        (
            df.write
            .mode("overwrite")
            .partitionBy("txn_year", "txn_month", "txn_day")
            .parquet(self.config.transactions_refined_path)
        )
        LOGGER.info("transactions_refined written to %s", self.config.transactions_refined_path)


# =========================================================
# ORCHESTRATOR
# =========================================================
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

        users_raw_df = self.extractor.read_users()
        cards_scd2_df = self.extractor.read_cards_scd2()
        transactions_raw_df = self.extractor.read_transactions()

        users_refined_df = self.users_builder.build(users_raw_df)
        cards_refined_scd2_df = self.cards_builder.build(cards_scd2_df)
        transactions_refined_df = self.transactions_builder.build(transactions_raw_df)

        self.writer.write_users_refined(users_refined_df)
        self.writer.write_cards_refined_scd2(cards_refined_scd2_df)
        self.writer.write_transactions_refined(transactions_refined_df)

        LOGGER.info("Warehouse ETL pipeline completed successfully")


# =========================================================
# ENTRYPOINT
# =========================================================
def main() -> None:
    config = WarehouseConfig()
    spark = SparkFactory.create(app_name="etl_warehouse")

    try:
        pipeline = WarehousePipeline(spark=spark, config=config)
        pipeline.run()
    finally:
        spark.stop()
        LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    main()
