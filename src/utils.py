# ============================================================
# utils.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Funciones y constantes compartidas para Bronze, Silver y Gold
# ============================================================

import json

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    regexp_replace,
    to_date,
    when,
    current_timestamp,
    sha2,
    concat_ws,
    lit
)


# ============================================================
# CONFIGURACION GENERAL DEL PROYECTO
# ============================================================

CATALOG = "fintech_finpay"

DEFAULT_SCHEMA = "default"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OBSERVABILITY_SCHEMA = "observability"

LANDING_PATH = f"/Volumes/{CATALOG}/{DEFAULT_SCHEMA}/vol_landing"
ARCHETYPE_PATH = f"{LANDING_PATH}/metadata/ingestion_archetypes.json"


# ============================================================
# NOMBRES FULLY-QUALIFIED DE TABLAS BRONZE
# ============================================================

BRONZE_TRANSACTIONS = f"{CATALOG}.{BRONZE_SCHEMA}.transactions"
BRONZE_MERCHANTS = f"{CATALOG}.{BRONZE_SCHEMA}.merchants"
BRONZE_USERS = f"{CATALOG}.{BRONZE_SCHEMA}.users"


# ============================================================
# NOMBRES FULLY-QUALIFIED DE TABLAS SILVER
# ============================================================

SILVER_TRANSACTIONS = f"{CATALOG}.{SILVER_SCHEMA}.silver_transactions"
SILVER_MERCHANTS = f"{CATALOG}.{SILVER_SCHEMA}.silver_merchants"
SILVER_USERS = f"{CATALOG}.{SILVER_SCHEMA}.silver_users"
SILVER_QUARANTINE = f"{CATALOG}.{SILVER_SCHEMA}.quarantine"


# ============================================================
# NOMBRES FULLY-QUALIFIED DE TABLAS GOLD
# ============================================================

GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_risk_kpis_by_merchant_channel"
)

GOLD_DAILY_RISK_SUMMARY = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_daily_risk_summary"
)

GOLD_MERCHANT_ANOMALY_CANDIDATES = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_merchant_anomaly_candidates"
)

GOLD_USER_RISK_SUMMARY = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_user_risk_summary"
)

GOLD_COUNTRY_CATEGORY_RISK_SUMMARY = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_country_category_risk_summary"
)

GOLD_DATE_SUMMARY = (
    f"{CATALOG}.{GOLD_SCHEMA}.gold_date_summary"
)


# ============================================================
# OBSERVABILIDAD
# ============================================================

OBSERVABILITY_EVENT_LOG = (
    f"{CATALOG}.{OBSERVABILITY_SCHEMA}.pipeline_event_log"
)


# ============================================================
# SCHEMAS RAW BRONZE
# En Bronze todos los campos llegan como STRING.
# El casteo real se hace en Silver.
# ============================================================

TRANSACTIONS_BRONZE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("reference_id", StringType(), True)
])


MERCHANTS_BRONZE_SCHEMA = StructType([
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("country", StringType(), True),
    StructField("affiliation_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("risk_level", StringType(), True)
])


USERS_BRONZE_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("document_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("registration_date", StringType(), True)
])


# ============================================================
# FUNCIONES PARA INGESTA METADATA-DRIVEN
# ============================================================

def get_schema_by_source(source_name: str):
    """
    Retorna el schema Bronze correspondiente a cada fuente.
    """
    source_name = str(source_name).strip().lower()

    if source_name == "transactions":
        return TRANSACTIONS_BRONZE_SCHEMA

    if source_name == "merchants":
        return MERCHANTS_BRONZE_SCHEMA

    if source_name == "users":
        return USERS_BRONZE_SCHEMA

    raise ValueError(f"Fuente no soportada en get_schema_by_source: {source_name}")


def get_bronze_table_name_by_source(source_name: str):
    """
    Retorna el nombre fully-qualified de la tabla Bronze
    para cada fuente.
    """
    source_name = str(source_name).strip().lower()

    if source_name == "transactions":
        return BRONZE_TRANSACTIONS

    if source_name == "merchants":
        return BRONZE_MERCHANTS

    if source_name == "users":
        return BRONZE_USERS

    raise ValueError(f"Fuente no soportada en get_bronze_table_name_by_source: {source_name}")


def read_ingestion_archetypes():
    """
    Lee el archivo metadata-driven ingestion_archetypes.json
    desde la landing zone.
    """
    with open(ARCHETYPE_PATH, "r") as file:
        archetypes = json.load(file)

    return [
        archetype
        for archetype in archetypes
        if archetype.get("active") is True
    ]


# ============================================================
# COLUMNAS TECNICAS DE AUDITORIA
# ============================================================

def add_audit_columns(df, source_name: str):
    """
    Agrega columnas tecnicas de auditoria en Bronze.

    Nota:
    En Unity Catalog no se debe usar input_file_name().
    Se usa _metadata.file_path.
    """
    data_columns = [
        c for c in df.columns
        if c != "_metadata"
    ]

    return (
        df.withColumn("_source_name", lit(source_name))
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn(
              "_record_hash",
              sha2(
                  concat_ws(
                      "||",
                      *[col(c).cast("string") for c in data_columns]
                  ),
                  256
              )
          )
    )


# ============================================================
# FUNCIONES DE LIMPIEZA Y NORMALIZACION PARA SILVER
# ============================================================

def clean_string(column_name: str):
    """
    Limpia espacios en blanco al inicio y final.
    """
    return trim(col(column_name))


def normalize_code(column_name: str):
    """
    Normaliza codigos a mayusculas.
    Ejemplo: pais, moneda.
    """
    return upper(trim(col(column_name)))


def normalize_lower(column_name: str):
    """
    Normaliza textos categóricos a minusculas.
    Ejemplo: canal, estado, tipo de transaccion.
    """
    return lower(trim(col(column_name)))


def parse_amount(column_name: str):
    """
    Convierte montos recibidos como texto a decimal(18,2).
    Soporta coma decimal reemplazandola por punto.
    """
    return regexp_replace(col(column_name), ",", ".").cast("decimal(18,2)")


def parse_date_multi_format(column_name: str):
    """
    Parsea fechas con multiples formatos posibles:
    - yyyy-MM-dd
    - dd/MM/yyyy
    - yyyyMMdd
    """
    return (
        when(
            to_date(col(column_name), "yyyy-MM-dd").isNotNull(),
            to_date(col(column_name), "yyyy-MM-dd")
        )
        .when(
            to_date(col(column_name), "dd/MM/yyyy").isNotNull(),
            to_date(col(column_name), "dd/MM/yyyy")
        )
        .when(
            to_date(col(column_name), "yyyyMMdd").isNotNull(),
            to_date(col(column_name), "yyyyMMdd")
        )
        .otherwise(lit(None).cast("date"))
    )


# ============================================================
# LISTAS DE VALORES VALIDOS
# Útiles para documentación y validaciones
# ============================================================

VALID_CHANNELS = ["web", "app", "pos"]

VALID_TRANSACTION_TYPES = ["pago", "reversa", "retiro"]

VALID_TRANSACTION_STATUS = ["aprobado", "rechazado", "pendiente"]

VALID_CURRENCIES = ["PEN", "USD", "COP", "MXN", "CLP", "ARS"]

VALID_COUNTRIES = ["PE", "CO", "MX", "CL", "AR"]

VALID_MERCHANT_STATUS = ["activo", "inactivo", "suspendido"]

VALID_RISK_LEVELS = ["bajo", "medio", "alto"]

VALID_USER_SEGMENTS = ["premium", "estandar", "nuevo"]

VALID_MERCHANT_CATEGORIES = [
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
]