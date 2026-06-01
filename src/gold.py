# ============================================================
# gold.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Capa Gold: KPIs de riesgo, tasas de reversa y anomalías
# ============================================================

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    when,
    lit,
    dayofmonth,
    month,
    quarter,
    year
)

from src.utils import (
    SILVER_TRANSACTIONS,
    SILVER_MERCHANTS,
    SILVER_USERS,
    GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL,
    GOLD_DAILY_RISK_SUMMARY,
    GOLD_MERCHANT_ANOMALY_CANDIDATES,
    GOLD_USER_RISK_SUMMARY,
    GOLD_COUNTRY_CATEGORY_RISK_SUMMARY,
    GOLD_DATE_SUMMARY
)


# ============================================================
# GOLD: KPIs DE RIESGO POR COMERCIO, CANAL Y FECHA
# ============================================================

@dp.materialized_view(
    name=GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL,
    comment="KPIs Gold de riesgo por comercio, canal y fecha"
)
def gold_risk_kpis_by_merchant_channel():
    transactions = spark.read.table(SILVER_TRANSACTIONS)
    merchants = spark.read.table(SILVER_MERCHANTS)

    base = (
        transactions.alias("t")
        .join(
            merchants.alias("m"),
            col("t.merchant_id") == col("m.merchant_id"),
            "left"
        )
    )

    return (
        base.groupBy(
            col("t.transaction_date").alias("transaction_date"),
            col("t.merchant_id").alias("merchant_id"),
            col("m.merchant_name").alias("merchant_name"),
            col("m.category").alias("merchant_category"),
            col("m.country").alias("merchant_country"),
            col("m.risk_level").alias("merchant_risk_level"),
            col("t.channel").alias("channel")
        )
        .agg(
            count("*").alias("transaction_count"),
            countDistinct("t.user_id").alias("unique_users"),
            spark_sum("t.amount_value").alias("total_amount"),
            avg("t.amount_value").alias("avg_amount"),
            spark_max("t.amount_value").alias("max_amount"),
            spark_min("t.amount_value").alias("min_amount"),
            spark_sum(
                when(col("t.transaction_type") == "pago", lit(1)).otherwise(lit(0))
            ).alias("payment_count"),
            spark_sum(
                when(col("t.transaction_type") == "reversa", lit(1)).otherwise(lit(0))
            ).alias("reversal_count"),
            spark_sum(
                when(col("t.transaction_type") == "retiro", lit(1)).otherwise(lit(0))
            ).alias("withdrawal_count"),
            spark_sum(
                when(col("t.status") == "rechazado", lit(1)).otherwise(lit(0))
            ).alias("rejected_count"),
            spark_sum(
                when(col("t.status") == "pendiente", lit(1)).otherwise(lit(0))
            ).alias("pending_count"),
            spark_sum(
                when(col("t.status") == "aprobado", lit(1)).otherwise(lit(0))
            ).alias("approved_count")
        )
        .withColumn(
            "reversal_rate",
            col("reversal_count") / col("transaction_count")
        )
        .withColumn(
            "rejection_rate",
            col("rejected_count") / col("transaction_count")
        )
        .withColumn(
            "pending_rate",
            col("pending_count") / col("transaction_count")
        )
        .withColumn(
            "risk_score",
            (
                col("reversal_rate") * lit(0.50)
                + col("rejection_rate") * lit(0.30)
                + col("pending_rate") * lit(0.10)
                + when(col("merchant_risk_level") == "alto", lit(0.10))
                  .when(col("merchant_risk_level") == "medio", lit(0.05))
                  .otherwise(lit(0.00))
            )
        )
        .withColumn(
            "risk_band",
            when(col("risk_score") >= lit(0.50), lit("alto"))
            .when(col("risk_score") >= lit(0.25), lit("medio"))
            .otherwise(lit("bajo"))
        )
    )


# ============================================================
# GOLD: RESUMEN DIARIO DE RIESGO
# ============================================================

@dp.materialized_view(
    name=GOLD_DAILY_RISK_SUMMARY,
    comment="Resumen diario Gold de riesgo por canal, tipo de transaccion y moneda"
)
def gold_daily_risk_summary():
    transactions = spark.read.table(SILVER_TRANSACTIONS)

    return (
        transactions
        .groupBy(
            col("transaction_date"),
            col("channel"),
            col("transaction_type"),
            col("currency")
        )
        .agg(
            count("*").alias("transaction_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("merchant_id").alias("unique_merchants"),
            spark_sum("amount_value").alias("total_amount"),
            avg("amount_value").alias("avg_amount"),
            spark_sum(
                when(col("status") == "aprobado", lit(1)).otherwise(lit(0))
            ).alias("approved_count"),
            spark_sum(
                when(col("status") == "rechazado", lit(1)).otherwise(lit(0))
            ).alias("rejected_count"),
            spark_sum(
                when(col("status") == "pendiente", lit(1)).otherwise(lit(0))
            ).alias("pending_count"),
            spark_sum(
                when(col("transaction_type") == "reversa", lit(1)).otherwise(lit(0))
            ).alias("reversal_count")
        )
        .withColumn(
            "rejection_rate",
            col("rejected_count") / col("transaction_count")
        )
        .withColumn(
            "reversal_rate",
            col("reversal_count") / col("transaction_count")
        )
        .withColumn(
            "pending_rate",
            col("pending_count") / col("transaction_count")
        )
    )


# ============================================================
# GOLD: CANDIDATOS DE ANOMALIA POR COMERCIO Y CANAL
# ============================================================

@dp.materialized_view(
    name=GOLD_MERCHANT_ANOMALY_CANDIDATES,
    comment="Comercios candidatos a anomalia por tasas altas de reversa, rechazo o score de riesgo"
)
def gold_merchant_anomaly_candidates():
    kpis = spark.read.table(GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL)

    return (
        kpis
        .where(
            (col("reversal_rate") >= lit(0.20))
            | (col("rejection_rate") >= lit(0.20))
            | (col("risk_score") >= lit(0.25))
            | (col("transaction_count") >= lit(50))
            | (col("total_amount") >= lit(10000))
        )
        .select(
            col("transaction_date"),
            col("merchant_id"),
            col("merchant_name"),
            col("merchant_category"),
            col("merchant_country"),
            col("merchant_risk_level"),
            col("channel"),
            col("transaction_count"),
            col("unique_users"),
            col("total_amount"),
            col("avg_amount"),
            col("max_amount"),
            col("payment_count"),
            col("reversal_count"),
            col("withdrawal_count"),
            col("rejected_count"),
            col("pending_count"),
            col("approved_count"),
            col("reversal_rate"),
            col("rejection_rate"),
            col("pending_rate"),
            col("risk_score"),
            col("risk_band")
        )
    )


# ============================================================
# GOLD: RIESGO POR USUARIO
# ============================================================

@dp.materialized_view(
    name=GOLD_USER_RISK_SUMMARY,
    comment="Resumen Gold de comportamiento transaccional y riesgo por usuario"
)
def gold_user_risk_summary():
    transactions = spark.read.table(SILVER_TRANSACTIONS)
    users = spark.read.table(SILVER_USERS)

    base = (
        transactions.alias("t")
        .join(
            users.alias("u"),
            col("t.user_id") == col("u.user_id"),
            "left"
        )
    )

    return (
        base.groupBy(
            col("t.user_id").alias("user_id"),
            col("u.country").alias("user_country"),
            col("u.segment").alias("user_segment")
        )
        .agg(
            count("*").alias("transaction_count"),
            countDistinct("t.merchant_id").alias("unique_merchants"),
            countDistinct("t.channel").alias("unique_channels"),
            spark_sum("t.amount_value").alias("total_amount"),
            avg("t.amount_value").alias("avg_amount"),
            spark_max("t.amount_value").alias("max_amount"),
            spark_sum(
                when(col("t.transaction_type") == "pago", lit(1)).otherwise(lit(0))
            ).alias("payment_count"),
            spark_sum(
                when(col("t.transaction_type") == "reversa", lit(1)).otherwise(lit(0))
            ).alias("reversal_count"),
            spark_sum(
                when(col("t.transaction_type") == "retiro", lit(1)).otherwise(lit(0))
            ).alias("withdrawal_count"),
            spark_sum(
                when(col("t.status") == "rechazado", lit(1)).otherwise(lit(0))
            ).alias("rejected_count")
        )
        .withColumn(
            "reversal_rate",
            col("reversal_count") / col("transaction_count")
        )
        .withColumn(
            "rejection_rate",
            col("rejected_count") / col("transaction_count")
        )
        .withColumn(
            "user_risk_score",
            (
                col("reversal_rate") * lit(0.50)
                + col("rejection_rate") * lit(0.30)
                + when(col("max_amount") >= lit(5000), lit(0.20))
                  .otherwise(lit(0.00))
            )
        )
        .withColumn(
            "user_risk_band",
            when(col("user_risk_score") >= lit(0.50), lit("alto"))
            .when(col("user_risk_score") >= lit(0.25), lit("medio"))
            .otherwise(lit("bajo"))
        )
    )


# ============================================================
# GOLD: RESUMEN POR PAIS Y CATEGORIA DE COMERCIO
# ============================================================

@dp.materialized_view(
    name=GOLD_COUNTRY_CATEGORY_RISK_SUMMARY,
    comment="Resumen Gold de riesgo por pais y categoria de comercio"
)
def gold_country_category_risk_summary():
    kpis = spark.read.table(GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL)

    return (
        kpis
        .groupBy(
            col("merchant_country"),
            col("merchant_category"),
            col("channel"),
            col("transaction_date")
        )
        .agg(
            spark_sum("transaction_count").alias("transaction_count"),
            spark_sum("unique_users").alias("unique_users_approx"),
            spark_sum("total_amount").alias("total_amount"),
            avg("avg_amount").alias("avg_amount"),
            spark_sum("reversal_count").alias("reversal_count"),
            spark_sum("rejected_count").alias("rejected_count"),
            avg("risk_score").alias("avg_risk_score")
        )
        .withColumn(
            "reversal_rate",
            col("reversal_count") / col("transaction_count")
        )
        .withColumn(
            "rejection_rate",
            col("rejected_count") / col("transaction_count")
        )
        .withColumn(
            "risk_band",
            when(col("avg_risk_score") >= lit(0.50), lit("alto"))
            .when(col("avg_risk_score") >= lit(0.25), lit("medio"))
            .otherwise(lit("bajo"))
        )
    )


# ============================================================
# GOLD: RESUMEN POR FECHA
# ============================================================

@dp.materialized_view(
    name=GOLD_DATE_SUMMARY,
    comment="Resumen Gold por fecha con atributos calendario basicos"
)
def gold_date_summary():
    transactions = spark.read.table(SILVER_TRANSACTIONS)

    return (
        transactions
        .groupBy(
            col("transaction_date")
        )
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount_value").alias("total_amount"),
            spark_sum(
                when(col("transaction_type") == "reversa", lit(1)).otherwise(lit(0))
            ).alias("reversal_count"),
            spark_sum(
                when(col("status") == "rechazado", lit(1)).otherwise(lit(0))
            ).alias("rejected_count")
        )
        .withColumn("day", dayofmonth(col("transaction_date")))
        .withColumn("month", month(col("transaction_date")))
        .withColumn("quarter", quarter(col("transaction_date")))
        .withColumn("year", year(col("transaction_date")))
        .withColumn(
            "reversal_rate",
            col("reversal_count") / col("transaction_count")
        )
        .withColumn(
            "rejection_rate",
            col("rejected_count") / col("transaction_count")
        )
    )