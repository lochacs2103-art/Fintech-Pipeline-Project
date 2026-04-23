from dataclasses import dataclass


@dataclass(frozen=True)
class WarehousePaths:
    users_refined: str = "/warehouse/datawarehouse/users_refined"
    cards_refined_scd2: str = "/warehouse/datawarehouse/cards_refined_scd2"
    transactions_refined: str = "/warehouse/datawarehouse/transactions_refined"


PATHS = WarehousePaths()


def users_refined() -> str:
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS users_refined (
        user_id BIGINT,
        user_id_raw STRING,
        user_id_null_flag BOOLEAN,
        user_id_unknown_flag BOOLEAN,

        current_age INT,
        current_age_raw STRING,
        current_age_null_flag BOOLEAN,
        current_age_unknown_flag BOOLEAN,
        current_age_outlier_flag BOOLEAN,

        retirement_age INT,
        retirement_age_raw STRING,
        retirement_age_null_flag BOOLEAN,
        retirement_age_unknown_flag BOOLEAN,
        retirement_age_outlier_flag BOOLEAN,

        birth_year INT,
        birth_year_raw STRING,
        birth_year_null_flag BOOLEAN,
        birth_year_unknown_flag BOOLEAN,
        birth_year_outlier_flag BOOLEAN,

        birth_month INT,
        birth_month_raw STRING,
        birth_month_null_flag BOOLEAN,
        birth_month_unknown_flag BOOLEAN,

        gender STRING,
        address STRING,

        latitude DOUBLE,
        latitude_raw STRING,
        latitude_null_flag BOOLEAN,
        latitude_unknown_flag BOOLEAN,

        longitude DOUBLE,
        longitude_raw STRING,
        longitude_null_flag BOOLEAN,
        longitude_unknown_flag BOOLEAN,

        per_capita_income_raw STRING,
        per_capita_income_amount DOUBLE,
        per_capita_income_amount_null_flag BOOLEAN,
        per_capita_income_amount_unknown_flag BOOLEAN,
        per_capita_income_currency STRING,

        yearly_income_raw STRING,
        yearly_income_amount DOUBLE,
        yearly_income_amount_null_flag BOOLEAN,
        yearly_income_amount_unknown_flag BOOLEAN,
        yearly_income_currency STRING,

        total_debt_raw STRING,
        total_debt_amount DOUBLE,
        total_debt_amount_null_flag BOOLEAN,
        total_debt_amount_unknown_flag BOOLEAN,
        total_debt_currency STRING,

        credit_score INT,
        credit_score_raw STRING,
        credit_score_null_flag BOOLEAN,
        credit_score_unknown_flag BOOLEAN,

        num_credit_cards INT,
        num_credit_cards_raw STRING,
        num_credit_cards_null_flag BOOLEAN,
        num_credit_cards_unknown_flag BOOLEAN,

        age_group STRING,
        income_group STRING
    )
    PARTITIONED BY (
        load_day DATE
    )
    STORED AS PARQUET
    LOCATION '{PATHS.users_refined}'
    """


def cards_refined_scd2() -> str:
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS cards_refined_scd2 (
        card_id BIGINT,
        card_id_raw STRING,
        card_id_null_flag BOOLEAN,
        card_id_unknown_flag BOOLEAN,

        client_id BIGINT,
        client_id_raw STRING,
        client_id_null_flag BOOLEAN,
        client_id_unknown_flag BOOLEAN,

        card_brand STRING,
        card_type STRING,
        card_number STRING,
        expires STRING,
        cvv STRING,
        has_chip STRING,

        num_cards_issued INT,
        num_cards_issued_raw STRING,
        num_cards_issued_null_flag BOOLEAN,
        num_cards_issued_unknown_flag BOOLEAN,

        credit_limit_raw STRING,
        credit_limit_amount DOUBLE,
        credit_limit_amount_null_flag BOOLEAN,
        credit_limit_amount_unknown_flag BOOLEAN,
        credit_limit_currency STRING,

        acct_open_date DATE,
        acct_open_date_raw STRING,
        acct_open_date_null_flag BOOLEAN,
        acct_open_date_unknown_flag BOOLEAN,

        year_pin_last_changed INT,
        year_pin_last_changed_raw STRING,
        year_pin_last_changed_null_flag BOOLEAN,
        year_pin_last_changed_unknown_flag BOOLEAN,
        year_pin_last_changed_outlier_flag BOOLEAN,

        card_on_dark_web STRING,

        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN,
        batch_id STRING
    )
    PARTITIONED BY (
        effective_year INT,
        effective_month INT
    )
    STORED AS PARQUET
    LOCATION '{PATHS.cards_refined_scd2}'
    """


def transactions_refined() -> str:
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS transactions_refined (
        transaction_id BIGINT,
        transaction_id_raw STRING,
        transaction_id_null_flag BOOLEAN,
        transaction_id_unknown_flag BOOLEAN,

        txn_timestamp TIMESTAMP,

        client_id BIGINT,
        client_id_raw STRING,
        client_id_null_flag BOOLEAN,
        client_id_unknown_flag BOOLEAN,

        card_id BIGINT,
        card_id_raw STRING,
        card_id_null_flag BOOLEAN,
        card_id_unknown_flag BOOLEAN,

        amount_raw STRING,
        amount DOUBLE,
        amount_null_flag BOOLEAN,
        amount_unknown_flag BOOLEAN,
        amount_currency STRING,

        use_chip STRING,
        merchant_id STRING,
        merchant_city STRING,
        merchant_state STRING,
        zip STRING,
        mcc STRING,
        errors STRING
    )
    PARTITIONED BY (
        txn_year INT,
        txn_month INT,
        txn_day INT
    )
    STORED AS PARQUET
    LOCATION '{PATHS.transactions_refined}'
    """


def all_tables() -> list[str]:
    return [
        users_refined(),
        cards_refined_scd2(),
        transactions_refined(),
    ]
