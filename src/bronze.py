# ============================================================
# bronze.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Capa Bronze
# Un solo Lakeflow Pipeline escribiendo en fintech_finpay.bronze
# ============================================================

from pyspark import pipelines as dp

from utils import (
    LANDING_PATH,
    BRONZE_TRANSACTIONS,
    BRONZE_MERCHANTS,
    BRONZE_USERS,
    TRANSACTIONS_BRONZE_SCHEMA,
    MERCHANTS_BRONZE_SCHEMA,
    USERS_BRONZE_SCHEMA,
    add_audit_columns,
)


# ============================================================
# BRONZE - TRANSACTIONS
# ============================================================

@dp.table(
    name=BRONZE_TRANSACTIONS,
    comment="Bronze raw streaming table para transacciones FinPay.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def bronze_transactions_raw():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")
        .schema(TRANSACTIONS_BRONZE_SCHEMA)
        .load(f"{LANDING_PATH}/transactions")
    )

    return add_audit_columns(df, "transactions")


# ============================================================
# BRONZE - MERCHANTS
# ============================================================

@dp.table(
    name=BRONZE_MERCHANTS,
    comment="Bronze raw streaming table para comercios FinPay.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def bronze_merchants_raw():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .schema(MERCHANTS_BRONZE_SCHEMA)
        .load(f"{LANDING_PATH}/merchants")
    )

    return add_audit_columns(df, "merchants")


# ============================================================
# BRONZE - USERS
# ============================================================

@dp.table(
    name=BRONZE_USERS,
    comment="Bronze raw streaming table para usuarios FinPay.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def bronze_users_raw():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("delimiter", "|")
        .option("mode", "PERMISSIVE")
        .schema(USERS_BRONZE_SCHEMA)
        .load(f"{LANDING_PATH}/users")
    )

    return add_audit_columns(df, "users")