# ============================================================
# silver.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Capa Silver: limpieza, estandarizacion, calidad y cuarentena
# ============================================================

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp,
    to_json,
    struct
)

from src.utils import (
    BRONZE_TRANSACTIONS,
    BRONZE_MERCHANTS,
    BRONZE_USERS,
    SILVER_TRANSACTIONS,
    SILVER_MERCHANTS,
    SILVER_USERS,
    SILVER_QUARANTINE,
    clean_string,
    normalize_code,
    normalize_lower,
    parse_amount,
    parse_date_multi_format
)


# ============================================================
# SILVER: TRANSACTIONS VALIDAS
# ============================================================

@dp.table(
    name=SILVER_TRANSACTIONS,
    comment="Silver transactions limpias, estandarizadas, casteadas y deduplicadas"
)
@dp.expect_or_drop(
    "valid_transaction_id",
    "transaction_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_user_id",
    "user_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_merchant_id",
    "merchant_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_channel",
    "channel IN ('web', 'app', 'pos')"
)
@dp.expect_or_drop(
    "valid_transaction_type",
    "transaction_type IN ('pago', 'reversa', 'retiro')"
)
@dp.expect_or_drop(
    "valid_amount",
    "amount_value IS NOT NULL AND amount_value > 0"
)
@dp.expect_or_drop(
    "valid_currency",
    "currency IN ('PEN', 'USD', 'COP', 'MXN', 'CLP', 'ARS')"
)
@dp.expect_or_drop(
    "valid_transaction_date",
    "transaction_date IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_status",
    "status IN ('aprobado', 'rechazado', 'pendiente')"
)
def silver_transactions():
    df = spark.readStream.table(BRONZE_TRANSACTIONS)

    return (
        df.select(
            clean_string("transaction_id").alias("transaction_id"),
            clean_string("user_id").alias("user_id"),
            clean_string("merchant_id").alias("merchant_id"),
            normalize_lower("channel").alias("channel"),
            normalize_lower("transaction_type").alias("transaction_type"),
            parse_amount("amount").alias("amount_value"),
            normalize_code("currency").alias("currency"),
            parse_date_multi_format("transaction_date").alias("transaction_date"),
            normalize_lower("status").alias("status"),
            clean_string("reference_id").alias("reference_id"),
            col("_source_name"),
            col("_source_file"),
            col("_ingestion_timestamp"),
            col("_record_hash")
        )
        .dropDuplicates(["transaction_id"])
    )


# ============================================================
# SILVER: MERCHANTS VALIDOS
# ============================================================

@dp.table(
    name=SILVER_MERCHANTS,
    comment="Silver merchants limpios, estandarizados y deduplicados"
)
@dp.expect_or_drop(
    "valid_merchant_id",
    "merchant_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_merchant_name",
    "merchant_name IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_category",
    "category IN ('retail', 'restaurante', 'farmacia', 'supermercado', 'tecnologia', 'transporte', 'educacion', 'salud', 'entretenimiento', 'moda')"
)
@dp.expect_or_drop(
    "valid_country",
    "country IN ('PE', 'CO', 'MX', 'CL', 'AR')"
)
@dp.expect_or_drop(
    "valid_affiliation_date",
    "affiliation_date IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_merchant_status",
    "status IN ('activo', 'inactivo', 'suspendido')"
)
def silver_merchants():
    df = spark.readStream.table(BRONZE_MERCHANTS)

    return (
        df.select(
            clean_string("merchant_id").alias("merchant_id"),
            clean_string("merchant_name").alias("merchant_name"),
            normalize_lower("category").alias("category"),
            normalize_code("country").alias("country"),
            parse_date_multi_format("affiliation_date").alias("affiliation_date"),
            normalize_lower("status").alias("status"),
            normalize_lower("risk_level").alias("risk_level"),
            col("_source_name"),
            col("_source_file"),
            col("_ingestion_timestamp"),
            col("_record_hash")
        )
        .dropDuplicates(["merchant_id"])
    )


# ============================================================
# SILVER: USERS VALIDOS
# ============================================================

@dp.table(
    name=SILVER_USERS,
    comment="Silver users limpios, estandarizados y deduplicados. Contiene PII para masking posterior en Unity Catalog."
)
@dp.expect_or_drop(
    "valid_user_id",
    "user_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_full_name",
    "full_name IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_email",
    "email IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_country",
    "country IN ('PE', 'CO', 'MX', 'CL', 'AR')"
)
@dp.expect_or_drop(
    "valid_registration_date",
    "registration_date IS NOT NULL"
)
def silver_users():
    df = spark.readStream.table(BRONZE_USERS)

    return (
        df.select(
            clean_string("user_id").alias("user_id"),
            clean_string("full_name").alias("full_name"),
            clean_string("document_id").alias("document_id"),
            normalize_lower("email").alias("email"),
            clean_string("phone").alias("phone"),
            normalize_code("country").alias("country"),
            normalize_lower("segment").alias("segment"),
            parse_date_multi_format("registration_date").alias("registration_date"),
            col("_source_name"),
            col("_source_file"),
            col("_ingestion_timestamp"),
            col("_record_hash")
        )
        .dropDuplicates(["user_id"])
    )


# ============================================================
# SILVER: TABLA DE CUARENTENA
# ============================================================

@dp.table(
    name=SILVER_QUARANTINE,
    comment="Registros rechazados en Silver para auditoria, trazabilidad y reproceso"
)
def quarantine():

    # --------------------------------------------------------
    # Rechazos de transactions
    # --------------------------------------------------------
    tx = spark.readStream.table(BRONZE_TRANSACTIONS)
    tx_raw_columns = tx.columns

    rejected_transactions = (
        tx.withColumn("transaction_id_clean", clean_string("transaction_id"))
          .withColumn("user_id_clean", clean_string("user_id"))
          .withColumn("merchant_id_clean", clean_string("merchant_id"))
          .withColumn("channel_clean", normalize_lower("channel"))
          .withColumn("transaction_type_clean", normalize_lower("transaction_type"))
          .withColumn("amount_value", parse_amount("amount"))
          .withColumn("currency_clean", normalize_code("currency"))
          .withColumn("transaction_date_parsed", parse_date_multi_format("transaction_date"))
          .withColumn("status_clean", normalize_lower("status"))
          .withColumn("reference_id_clean", clean_string("reference_id"))
          .withColumn(
              "rejection_reason",
              when(col("transaction_id_clean").isNull(), lit("transaction_id_null"))
              .when(col("user_id_clean").isNull(), lit("user_id_null"))
              .when(col("merchant_id_clean").isNull(), lit("merchant_id_null"))
              .when(~col("channel_clean").isin("web", "app", "pos"), lit("invalid_channel"))
              .when(~col("transaction_type_clean").isin("pago", "reversa", "retiro"), lit("invalid_transaction_type"))
              .when(col("amount_value").isNull(), lit("invalid_amount"))
              .when(col("amount_value") <= 0, lit("amount_not_positive"))
              .when(~col("currency_clean").isin("PEN", "USD", "COP", "MXN", "CLP", "ARS"), lit("invalid_currency"))
              .when(col("transaction_date_parsed").isNull(), lit("invalid_transaction_date"))
              .when(~col("status_clean").isin("aprobado", "rechazado", "pendiente"), lit("invalid_status"))
              .when(
                  (col("transaction_type_clean") == "reversa") &
                  (col("reference_id_clean").isNull()),
                  lit("reversa_without_reference_id")
              )
              .otherwise(lit(None))
          )
          .where(col("rejection_reason").isNotNull())
          .select(
              lit("transactions").alias("source_name"),
              col("rejection_reason"),
              current_timestamp().alias("processing_timestamp"),
              col("_source_file").alias("source_file"),
              to_json(
                  struct(*[col(c).alias(c) for c in tx_raw_columns])
              ).alias("raw_record")
          )
    )

    # --------------------------------------------------------
    # Rechazos de merchants
    # --------------------------------------------------------
    merchants = spark.readStream.table(BRONZE_MERCHANTS)
    merchants_raw_columns = merchants.columns

    rejected_merchants = (
        merchants.withColumn("merchant_id_clean", clean_string("merchant_id"))
                 .withColumn("merchant_name_clean", clean_string("merchant_name"))
                 .withColumn("category_clean", normalize_lower("category"))
                 .withColumn("country_clean", normalize_code("country"))
                 .withColumn("affiliation_date_parsed", parse_date_multi_format("affiliation_date"))
                 .withColumn("status_clean", normalize_lower("status"))
                 .withColumn("risk_level_clean", normalize_lower("risk_level"))
                 .withColumn(
                     "rejection_reason",
                     when(col("merchant_id_clean").isNull(), lit("merchant_id_null"))
                     .when(col("merchant_name_clean").isNull(), lit("merchant_name_null"))
                     .when(
                         ~col("category_clean").isin(
                             "retail",
                             "restaurante",
                             "farmacia",
                             "supermercado",
                             "tecnologia",
                             "transporte",
                             "educacion",
                             "salud",
                             "entretenimiento",
                             "moda"
                         ),
                         lit("invalid_category")
                     )
                     .when(~col("country_clean").isin("PE", "CO", "MX", "CL", "AR"), lit("invalid_country"))
                     .when(col("affiliation_date_parsed").isNull(), lit("invalid_affiliation_date"))
                     .when(~col("status_clean").isin("activo", "inactivo", "suspendido"), lit("invalid_status"))
                     .when(
                         col("risk_level_clean").isNotNull() &
                         ~col("risk_level_clean").isin("bajo", "medio", "alto"),
                         lit("invalid_risk_level")
                     )
                     .otherwise(lit(None))
                 )
                 .where(col("rejection_reason").isNotNull())
                 .select(
                     lit("merchants").alias("source_name"),
                     col("rejection_reason"),
                     current_timestamp().alias("processing_timestamp"),
                     col("_source_file").alias("source_file"),
                     to_json(
                         struct(*[col(c).alias(c) for c in merchants_raw_columns])
                     ).alias("raw_record")
                 )
    )

    # --------------------------------------------------------
    # Rechazos de users
    # --------------------------------------------------------
    users = spark.readStream.table(BRONZE_USERS)
    users_raw_columns = users.columns

    rejected_users = (
        users.withColumn("user_id_clean", clean_string("user_id"))
             .withColumn("full_name_clean", clean_string("full_name"))
             .withColumn("document_id_clean", clean_string("document_id"))
             .withColumn("email_clean", normalize_lower("email"))
             .withColumn("phone_clean", clean_string("phone"))
             .withColumn("country_clean", normalize_code("country"))
             .withColumn("segment_clean", normalize_lower("segment"))
             .withColumn("registration_date_parsed", parse_date_multi_format("registration_date"))
             .withColumn(
                 "rejection_reason",
                 when(col("user_id_clean").isNull(), lit("user_id_null"))
                 .when(col("full_name_clean").isNull(), lit("full_name_null"))
                 .when(col("document_id_clean").isNull(), lit("document_id_null"))
                 .when(col("email_clean").isNull(), lit("email_null"))
                 .when(~col("email_clean").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), lit("invalid_email"))
                 .when(col("phone_clean").isNull(), lit("phone_null"))
                 .when(~col("phone_clean").rlike(r"^\+[0-9]{8,15}$"), lit("invalid_phone"))
                 .when(~col("country_clean").isin("PE", "CO", "MX", "CL", "AR"), lit("invalid_country"))
                 .when(
                     col("segment_clean").isNotNull() &
                     ~col("segment_clean").isin("premium", "estandar", "nuevo"),
                     lit("invalid_segment")
                 )
                 .when(col("registration_date_parsed").isNull(), lit("invalid_registration_date"))
                 .otherwise(lit(None))
             )
             .where(col("rejection_reason").isNotNull())
             .select(
                 lit("users").alias("source_name"),
                 col("rejection_reason"),
                 current_timestamp().alias("processing_timestamp"),
                 col("_source_file").alias("source_file"),
                 to_json(
                     struct(*[col(c).alias(c) for c in users_raw_columns])
                 ).alias("raw_record")
             )
    )

    return (
        rejected_transactions
        .unionByName(rejected_merchants)
        .unionByName(rejected_users)
    )