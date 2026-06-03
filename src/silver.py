# ============================================================
# silver.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Capa Silver
# Un solo Lakeflow Pipeline escribiendo en fintech_finpay.silver
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import (
    BRONZE_TRANSACTIONS,
    BRONZE_MERCHANTS,
    BRONZE_USERS,
    SILVER_TRANSACTIONS,
    SILVER_MERCHANTS,
    SILVER_USERS,
    SILVER_QUARANTINE,
    SILVER_QUARANTINE_TRANSACTIONS,
    SILVER_QUARANTINE_MERCHANTS,
    SILVER_QUARANTINE_USERS,
    clean_string,
    normalize_code,
    normalize_lower,
    parse_amount,
    parse_date_multi_format,
    VALID_CHANNELS,
    VALID_TRANSACTION_TYPES,
    VALID_TRANSACTION_STATUS,
    VALID_CURRENCIES,
    VALID_COUNTRIES,
    VALID_MERCHANT_STATUS,
    VALID_RISK_LEVELS,
    VALID_USER_SEGMENTS,
    VALID_MERCHANT_CATEGORIES,
)


# ============================================================
# FUNCIONES INTERNAS
# ============================================================

def build_transactions_clean_df():
    df = spark.readStream.table(BRONZE_TRANSACTIONS)

    return (
        df
        .withColumn("transaction_id", clean_string("transaction_id"))
        .withColumn("user_id", clean_string("user_id"))
        .withColumn("merchant_id", clean_string("merchant_id"))
        .withColumn("channel", normalize_lower("channel"))
        .withColumn("transaction_type", normalize_lower("transaction_type"))
        .withColumn("amount", parse_amount("amount"))
        .withColumn("currency", normalize_code("currency"))
        .withColumn("transaction_date", parse_date_multi_format("transaction_date"))
        .withColumn("status", normalize_lower("status"))
        .withColumn("reference_id", clean_string("reference_id"))
    )


def build_merchants_clean_df():
    df = spark.readStream.table(BRONZE_MERCHANTS)

    return (
        df
        .withColumn("merchant_id", clean_string("merchant_id"))
        .withColumn("merchant_name", clean_string("merchant_name"))
        .withColumn("category", normalize_lower("category"))
        .withColumn("country", normalize_code("country"))
        .withColumn("affiliation_date", parse_date_multi_format("affiliation_date"))
        .withColumn("status", normalize_lower("status"))
        .withColumn("risk_level", normalize_lower("risk_level"))
    )


def build_users_clean_df():
    df = spark.readStream.table(BRONZE_USERS)

    return (
        df
        .withColumn("user_id", clean_string("user_id"))
        .withColumn("full_name", clean_string("full_name"))
        .withColumn("document_id", clean_string("document_id"))
        .withColumn("email", normalize_lower("email"))
        .withColumn("phone", clean_string("phone"))
        .withColumn("country", normalize_code("country"))
        .withColumn("segment", normalize_lower("segment"))
        .withColumn("registration_date", parse_date_multi_format("registration_date"))
    )


def build_quarantine_record(df, source_name: str):
    raw_columns = [
        column_name
        for column_name in df.columns
        if column_name not in [
            "source_name",
            "rejection_reason",
            "processing_timestamp",
            "raw_record",
        ]
    ]

    return (
        df
        .where(F.col("rejection_reason").isNotNull())
        .withColumn("source_name", F.lit(source_name))
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn(
            "raw_record",
            F.to_json(F.struct(*[F.col(c).alias(c) for c in raw_columns]))
        )
        .select(
            "source_name",
            "rejection_reason",
            "processing_timestamp",
            "raw_record",
        )
    )


# ============================================================
# SILVER - TRANSACTIONS
# ============================================================

@dp.table(
    name=SILVER_TRANSACTIONS,
    comment="Transacciones limpias, casteadas, validadas y deduplicadas.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop(
    "valid_transaction_id",
    "transaction_id RLIKE '^TXN-[0-9]{8}-[0-9]{5}$'",
)
@dp.expect_or_drop(
    "valid_user_id",
    "user_id RLIKE '^USR-[0-9]{6}$'",
)
@dp.expect_or_drop(
    "valid_merchant_id",
    "merchant_id RLIKE '^MCH-[0-9]{5}$'",
)
@dp.expect_or_drop(
    "valid_channel",
    "channel IN ('web', 'app', 'pos')",
)
@dp.expect_or_drop(
    "valid_transaction_type",
    "transaction_type IN ('pago', 'reversa', 'retiro')",
)
@dp.expect_or_drop(
    "valid_amount",
    "amount IS NOT NULL AND amount > 0",
)
@dp.expect_or_drop(
    "valid_currency",
    "currency IN ('PEN', 'USD', 'COP', 'MXN', 'CLP', 'ARS')",
)
@dp.expect_or_drop(
    "valid_transaction_date",
    "transaction_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_status",
    "status IN ('aprobado', 'rechazado', 'pendiente')",
)
@dp.expect_or_drop(
    "valid_reversal_reference",
    "CASE WHEN transaction_type = 'reversa' THEN reference_id IS NOT NULL ELSE TRUE END",
)
def transactions():
    df = build_transactions_clean_df()

    return (
        df
        .dropDuplicates(["transaction_id"])
        .select(
            "transaction_id",
            "user_id",
            "merchant_id",
            "channel",
            "transaction_type",
            "amount",
            "currency",
            "transaction_date",
            "status",
            "reference_id",
            "_source_name",
            "_source_file",
            "_ingestion_timestamp",
            "_record_hash",
        )
    )


# ============================================================
# SILVER - MERCHANTS
# ============================================================

@dp.table(
    name=SILVER_MERCHANTS,
    comment="Comercios limpios, estandarizados y validados.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop(
    "valid_merchant_id",
    "merchant_id RLIKE '^MCH-[0-9]{5}$'",
)
@dp.expect_or_drop(
    "valid_merchant_name",
    "merchant_name IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_category",
    """
    category IN (
        'retail',
        'restaurante',
        'farmacia',
        'supermercado',
        'tecnologia',
        'transporte',
        'educacion',
        'salud',
        'entretenimiento',
        'moda'
    )
    """,
)
@dp.expect_or_drop(
    "valid_country",
    "country IN ('PE', 'CO', 'MX', 'CL', 'AR')",
)
@dp.expect_or_drop(
    "valid_affiliation_date",
    "affiliation_date IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_merchant_status",
    "status IN ('activo', 'inactivo', 'suspendido')",
)
@dp.expect_or_drop(
    "valid_risk_level",
    "risk_level IS NULL OR risk_level IN ('bajo', 'medio', 'alto')",
)
def merchants():
    df = build_merchants_clean_df()

    return (
        df
        .dropDuplicates(["merchant_id"])
        .select(
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "affiliation_date",
            "status",
            "risk_level",
            "_source_name",
            "_source_file",
            "_ingestion_timestamp",
            "_record_hash",
        )
    )


# ============================================================
# SILVER - USERS
# ============================================================

@dp.table(
    name=SILVER_USERS,
    comment="Usuarios limpios, estandarizados y validados. Contiene PII.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop(
    "valid_user_id",
    "user_id RLIKE '^USR-[0-9]{6}$'",
)
@dp.expect_or_drop(
    "valid_full_name",
    "full_name IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_document_id",
    "document_id IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_email",
    "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'",
)
@dp.expect_or_drop(
    "valid_phone",
    "phone RLIKE '^\\\\+[0-9]{8,15}$'",
)
@dp.expect_or_drop(
    "valid_country",
    "country IN ('PE', 'CO', 'MX', 'CL', 'AR')",
)
@dp.expect_or_drop(
    "valid_segment",
    "segment IS NULL OR segment IN ('premium', 'estandar', 'nuevo')",
)
@dp.expect_or_drop(
    "valid_registration_date",
    "registration_date IS NOT NULL",
)
def users():
    df = build_users_clean_df()

    return (
        df
        .dropDuplicates(["user_id"])
        .select(
            "user_id",
            "full_name",
            "document_id",
            "email",
            "phone",
            "country",
            "segment",
            "registration_date",
            "_source_name",
            "_source_file",
            "_ingestion_timestamp",
            "_record_hash",
        )
    )


# ============================================================
# SILVER - QUARANTINE TRANSACTIONS
# ============================================================

@dp.table(
    name=SILVER_QUARANTINE_TRANSACTIONS,
    comment="Registros rechazados de transacciones.",
    table_properties={
        "quality": "silver",
        "purpose": "quarantine",
    },
)
def quarantine_transactions():
    df = build_transactions_clean_df()

    rejected_df = (
        df
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("transaction_id").isNull(),
                F.lit("transaction_id_null"),
            )
            .when(
                ~F.col("transaction_id").rlike(r"^TXN-[0-9]{8}-[0-9]{5}$"),
                F.lit("invalid_transaction_id"),
            )
            .when(
                F.col("user_id").isNull(),
                F.lit("user_id_null"),
            )
            .when(
                ~F.col("user_id").rlike(r"^USR-[0-9]{6}$"),
                F.lit("invalid_user_id"),
            )
            .when(
                F.col("merchant_id").isNull(),
                F.lit("merchant_id_null"),
            )
            .when(
                ~F.col("merchant_id").rlike(r"^MCH-[0-9]{5}$"),
                F.lit("invalid_merchant_id"),
            )
            .when(
                F.col("channel").isNull(),
                F.lit("channel_null"),
            )
            .when(
                ~F.col("channel").isin(VALID_CHANNELS),
                F.lit("invalid_channel"),
            )
            .when(
                F.col("transaction_type").isNull(),
                F.lit("transaction_type_null"),
            )
            .when(
                ~F.col("transaction_type").isin(VALID_TRANSACTION_TYPES),
                F.lit("invalid_transaction_type"),
            )
            .when(
                F.col("amount").isNull(),
                F.lit("amount_null_or_invalid"),
            )
            .when(
                F.col("amount") <= 0,
                F.lit("amount_not_positive"),
            )
            .when(
                F.col("currency").isNull(),
                F.lit("currency_null"),
            )
            .when(
                ~F.col("currency").isin(VALID_CURRENCIES),
                F.lit("invalid_currency"),
            )
            .when(
                F.col("transaction_date").isNull(),
                F.lit("invalid_transaction_date"),
            )
            .when(
                F.col("status").isNull(),
                F.lit("status_null"),
            )
            .when(
                ~F.col("status").isin(VALID_TRANSACTION_STATUS),
                F.lit("invalid_status"),
            )
            .when(
                (F.col("transaction_type") == "reversa")
                & (F.col("reference_id").isNull()),
                F.lit("reversal_without_reference_id"),
            )
            .otherwise(F.lit(None))
        )
    )

    return build_quarantine_record(rejected_df, "transactions")


# ============================================================
# SILVER - QUARANTINE MERCHANTS
# ============================================================

@dp.table(
    name=SILVER_QUARANTINE_MERCHANTS,
    comment="Registros rechazados de comercios.",
    table_properties={
        "quality": "silver",
        "purpose": "quarantine",
    },
)
def quarantine_merchants():
    df = build_merchants_clean_df()

    rejected_df = (
        df
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("merchant_id").isNull(),
                F.lit("merchant_id_null"),
            )
            .when(
                ~F.col("merchant_id").rlike(r"^MCH-[0-9]{5}$"),
                F.lit("invalid_merchant_id"),
            )
            .when(
                F.col("merchant_name").isNull(),
                F.lit("merchant_name_null"),
            )
            .when(
                F.col("category").isNull(),
                F.lit("category_null"),
            )
            .when(
                ~F.col("category").isin(VALID_MERCHANT_CATEGORIES),
                F.lit("invalid_category"),
            )
            .when(
                F.col("country").isNull(),
                F.lit("country_null"),
            )
            .when(
                ~F.col("country").isin(VALID_COUNTRIES),
                F.lit("invalid_country"),
            )
            .when(
                F.col("affiliation_date").isNull(),
                F.lit("invalid_affiliation_date"),
            )
            .when(
                F.col("status").isNull(),
                F.lit("status_null"),
            )
            .when(
                ~F.col("status").isin(VALID_MERCHANT_STATUS),
                F.lit("invalid_status"),
            )
            .when(
                F.col("risk_level").isNotNull()
                & (~F.col("risk_level").isin(VALID_RISK_LEVELS)),
                F.lit("invalid_risk_level"),
            )
            .otherwise(F.lit(None))
        )
    )

    return build_quarantine_record(rejected_df, "merchants")


# ============================================================
# SILVER - QUARANTINE USERS
# ============================================================

@dp.table(
    name=SILVER_QUARANTINE_USERS,
    comment="Registros rechazados de usuarios.",
    table_properties={
        "quality": "silver",
        "purpose": "quarantine",
    },
)
def quarantine_users():
    df = build_users_clean_df()

    rejected_df = (
        df
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("user_id").isNull(),
                F.lit("user_id_null"),
            )
            .when(
                ~F.col("user_id").rlike(r"^USR-[0-9]{6}$"),
                F.lit("invalid_user_id"),
            )
            .when(
                F.col("full_name").isNull(),
                F.lit("full_name_null"),
            )
            .when(
                F.col("document_id").isNull(),
                F.lit("document_id_null"),
            )
            .when(
                F.col("email").isNull(),
                F.lit("email_null"),
            )
            .when(
                ~F.col("email").rlike(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                ),
                F.lit("invalid_email"),
            )
            .when(
                F.col("phone").isNull(),
                F.lit("phone_null"),
            )
            .when(
                ~F.col("phone").rlike(r"^\+[0-9]{8,15}$"),
                F.lit("invalid_phone"),
            )
            .when(
                F.col("country").isNull(),
                F.lit("country_null"),
            )
            .when(
                ~F.col("country").isin(VALID_COUNTRIES),
                F.lit("invalid_country"),
            )
            .when(
                F.col("segment").isNotNull()
                & (~F.col("segment").isin(VALID_USER_SEGMENTS)),
                F.lit("invalid_segment"),
            )
            .when(
                F.col("registration_date").isNull(),
                F.lit("invalid_registration_date"),
            )
            .otherwise(F.lit(None))
        )
    )

    return build_quarantine_record(rejected_df, "users")


# ============================================================
# SILVER - QUARANTINE CONSOLIDADA
# ============================================================

@dp.table(
    name=SILVER_QUARANTINE,
    comment="Tabla consolidada de cuarentena para auditoria, trazabilidad y reproceso.",
    table_properties={
        "quality": "silver",
        "purpose": "quarantine",
    },
)
def quarantine():
    q_transactions = spark.readStream.table(SILVER_QUARANTINE_TRANSACTIONS)
    q_merchants = spark.readStream.table(SILVER_QUARANTINE_MERCHANTS)
    q_users = spark.readStream.table(SILVER_QUARANTINE_USERS)

    return (
        q_transactions
        .unionByName(q_merchants)
        .unionByName(q_users)
    )