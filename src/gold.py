# ============================================================
# gold.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Capa Gold
# Un solo Lakeflow Pipeline escribiendo en fintech_finpay.gold
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import (
    SILVER_TRANSACTIONS,
    SILVER_MERCHANTS,
    SILVER_USERS,
    GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL,
    GOLD_DAILY_REVERSAL_RATE,
    GOLD_MERCHANT_ANOMALY_CANDIDATES,
    GOLD_USER_CHANNEL_SUMMARY,
)


# ============================================================
# GOLD - RISK KPIS BY MERCHANT CHANNEL
# ============================================================

@dp.table(
    name=GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL,
    comment="KPIs de riesgo por comercio, canal, fecha y moneda.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def risk_kpis_by_merchant_channel():
    transactions_df = spark.read.table(SILVER_TRANSACTIONS)
    merchants_df = spark.read.table(SILVER_MERCHANTS)

    base_df = (
        transactions_df
        .join(
            merchants_df.select(
                "merchant_id",
                "merchant_name",
                "category",
                "country",
                "risk_level",
            ),
            on="merchant_id",
            how="left",
        )
    )

    result_df = (
        base_df
        .groupBy(
            "transaction_date",
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "channel",
            "currency",
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum(F.when(F.col("transaction_type") == "pago", 1).otherwise(0)).alias("payment_count"),
            F.sum(F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)).alias("reversal_count"),
            F.sum(F.when(F.col("transaction_type") == "retiro", 1).otherwise(0)).alias("withdrawal_count"),
            F.sum(F.when(F.col("status") == "aprobado", 1).otherwise(0)).alias("approved_count"),
            F.sum(F.when(F.col("status") == "rechazado", 1).otherwise(0)).alias("rejected_count"),
            F.sum(F.when(F.col("status") == "pendiente", 1).otherwise(0)).alias("pending_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .withColumn(
            "reversal_rate",
            F.round(
                F.col("reversal_count")
                / F.when(F.col("transaction_count") == 0, None).otherwise(F.col("transaction_count")),
                4,
            ),
        )
        .withColumn(
            "rejected_rate",
            F.round(
                F.col("rejected_count")
                / F.when(F.col("transaction_count") == 0, None).otherwise(F.col("transaction_count")),
                4,
            ),
        )
        .withColumn(
            "amount_risk_factor",
            F.when(F.col("avg_amount") >= 1000, F.lit(1.0))
            .when(F.col("avg_amount") >= 500, F.lit(0.5))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "risk_score",
            F.round(
                (F.coalesce(F.col("reversal_rate"), F.lit(0.0)) * F.lit(0.50))
                + (F.coalesce(F.col("rejected_rate"), F.lit(0.0)) * F.lit(0.30))
                + (F.col("amount_risk_factor") * F.lit(0.20)),
                4,
            ),
        )
        .withColumn(
            "calculated_risk_level",
            F.when(F.col("risk_score") >= 0.70, F.lit("alto"))
            .when(F.col("risk_score") >= 0.40, F.lit("medio"))
            .otherwise(F.lit("bajo")),
        )
        .withColumn("gold_processing_timestamp", F.current_timestamp())
    )

    return result_df


# ============================================================
# GOLD - DAILY REVERSAL RATE
# ============================================================

@dp.table(
    name=GOLD_DAILY_REVERSAL_RATE,
    comment="Tasa diaria de reversas por pais, categoria, canal y moneda.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def daily_reversal_rate():
    transactions_df = spark.read.table(SILVER_TRANSACTIONS)
    merchants_df = spark.read.table(SILVER_MERCHANTS)

    base_df = (
        transactions_df
        .join(
            merchants_df.select(
                "merchant_id",
                "country",
                "category",
                "risk_level",
            ),
            on="merchant_id",
            how="left",
        )
    )

    result_df = (
        base_df
        .groupBy(
            "transaction_date",
            "country",
            "category",
            "channel",
            "currency",
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum(F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)).alias("reversal_count"),
            F.sum(F.when(F.col("status") == "rechazado", 1).otherwise(0)).alias("rejected_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .withColumn(
            "reversal_rate",
            F.round(
                F.col("reversal_count")
                / F.when(F.col("transaction_count") == 0, None).otherwise(F.col("transaction_count")),
                4,
            ),
        )
        .withColumn(
            "rejected_rate",
            F.round(
                F.col("rejected_count")
                / F.when(F.col("transaction_count") == 0, None).otherwise(F.col("transaction_count")),
                4,
            ),
        )
        .withColumn("gold_processing_timestamp", F.current_timestamp())
    )

    return result_df


# ============================================================
# GOLD - MERCHANT ANOMALY CANDIDATES
# ============================================================

@dp.table(
    name=GOLD_MERCHANT_ANOMALY_CANDIDATES,
    comment="Comercios candidatos a anomalia por tasa de reversa, rechazo o score de riesgo.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def merchant_anomaly_candidates():
    kpis_df = spark.read.table(GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL)

    result_df = (
        kpis_df
        .withColumn(
            "is_anomaly_candidate",
            F.when(
                (F.col("reversal_rate") >= 0.30)
                | (F.col("risk_score") >= 0.70)
                | (
                    (F.col("avg_amount") >= 1000)
                    & (F.col("rejected_rate") >= 0.20)
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "anomaly_reason",
            F.when(F.col("reversal_rate") >= 0.30, F.lit("high_reversal_rate"))
            .when(F.col("risk_score") >= 0.70, F.lit("high_risk_score"))
            .when(
                (F.col("avg_amount") >= 1000)
                & (F.col("rejected_rate") >= 0.20),
                F.lit("high_amount_and_rejected_rate"),
            )
            .otherwise(F.lit("normal")),
        )
        .filter(F.col("is_anomaly_candidate") == True)
        .select(
            "transaction_date",
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "channel",
            "currency",
            "transaction_count",
            "payment_count",
            "reversal_count",
            "withdrawal_count",
            "approved_count",
            "rejected_count",
            "pending_count",
            "total_amount",
            "avg_amount",
            "reversal_rate",
            "rejected_rate",
            "risk_score",
            "calculated_risk_level",
            "is_anomaly_candidate",
            "anomaly_reason",
            "gold_processing_timestamp",
        )
    )

    return result_df


# ============================================================
# GOLD - USER CHANNEL SUMMARY
# ============================================================

@dp.table(
    name=GOLD_USER_CHANNEL_SUMMARY,
    comment="Resumen de comportamiento transaccional por usuario y canal.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def user_channel_summary():
    transactions_df = spark.read.table(SILVER_TRANSACTIONS)
    users_df = spark.read.table(SILVER_USERS)

    base_df = (
        transactions_df
        .join(
            users_df.select(
                "user_id",
                "country",
                "segment",
                "registration_date",
            ),
            on="user_id",
            how="left",
        )
    )

    result_df = (
        base_df
        .groupBy(
            "user_id",
            "country",
            "segment",
            "channel",
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum(F.when(F.col("transaction_type") == "pago", 1).otherwise(0)).alias("payment_count"),
            F.sum(F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)).alias("reversal_count"),
            F.sum(F.when(F.col("transaction_type") == "retiro", 1).otherwise(0)).alias("withdrawal_count"),
            F.sum(F.when(F.col("status") == "aprobado", 1).otherwise(0)).alias("approved_count"),
            F.sum(F.when(F.col("status") == "rechazado", 1).otherwise(0)).alias("rejected_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.min("transaction_date").alias("first_transaction_date"),
            F.max("transaction_date").alias("last_transaction_date"),
        )
        .withColumn(
            "reversal_rate",
            F.round(
                F.col("reversal_count")
                / F.when(F.col("transaction_count") == 0, None).otherwise(F.col("transaction_count")),
                4,
            ),
        )
        .withColumn(
            "rejected_rate",
            F.round(
                F.col("rejected_count")
                / F.when(F.col("transaction_count") == 0, None).otherwise(F.col("transaction_count")),
                4,
            ),
        )
        .withColumn(
            "user_risk_score",
            F.round(
                (F.coalesce(F.col("reversal_rate"), F.lit(0.0)) * F.lit(0.60))
                + (F.coalesce(F.col("rejected_rate"), F.lit(0.0)) * F.lit(0.40)),
                4,
            ),
        )
        .withColumn(
            "calculated_user_risk_level",
            F.when(F.col("user_risk_score") >= 0.70, F.lit("alto"))
            .when(F.col("user_risk_score") >= 0.40, F.lit("medio"))
            .otherwise(F.lit("bajo")),
        )
        .withColumn("gold_processing_timestamp", F.current_timestamp())
    )

    return result_df