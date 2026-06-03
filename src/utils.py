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
    lit,
    to_json,
    struct
)


# ============================================================
# CONFIGURACION GENERAL DEL PROYECTO
# ============================================================

# ============================================================
# CONFIGURACION GENERAL DEL PROYECTO
# ============================================================

CATALOG = "fintech_finpay_prod"

DEFAULT_SCHEMA = "default"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OBSERVABILITY_SCHEMA = "observability"

LANDING_PATH = f"/Volumes/{CATALOG}/{DEFAULT_SCHEMA}/vol_landing"
ARCHETYPE_PATH = f"{LANDING_PATH}/metadata/ingestion_archetypes.json"

# ============================================================
# NOMBRES LOGICOS DE TABLAS BRONZE
# Estas son las tablas que crea bronze.py
# ============================================================

BRONZE_TRANSACTIONS = f"{CATALOG}.bronze.bronze_transactions_raw"
BRONZE_MERCHANTS = f"{CATALOG}.bronze.bronze_merchants_raw"
BRONZE_USERS = f"{CATALOG}.bronze.bronze_users_raw"


# ============================================================
# NOMBRES LOGICOS DE TABLAS SILVER
# Estas son las tablas que crea silver.py
# ============================================================

SILVER_TRANSACTIONS = f"{CATALOG}.silver.transactions"
SILVER_MERCHANTS = f"{CATALOG}.silver.merchants"
SILVER_USERS = f"{CATALOG}.silver.users"

SILVER_QUARANTINE = f"{CATALOG}.silver.quarantine"
SILVER_QUARANTINE_TRANSACTIONS = f"{CATALOG}.silver.quarantine_transactions"
SILVER_QUARANTINE_MERCHANTS = f"{CATALOG}.silver.quarantine_merchants"
SILVER_QUARANTINE_USERS = f"{CATALOG}.silver.quarantine_users"

# ============================================================
# NOMBRES LOGICOS DE TABLAS GOLD
# Estas son las tablas que crea gold.py
# ============================================================

GOLD_RISK_KPIS_BY_MERCHANT_CHANNEL = f"{CATALOG}.gold.risk_kpis_by_merchant_channel"
GOLD_DAILY_REVERSAL_RATE = f"{CATALOG}.gold.daily_reversal_rate"
GOLD_MERCHANT_ANOMALY_CANDIDATES = f"{CATALOG}.gold.merchant_anomaly_candidates"
GOLD_USER_CHANNEL_SUMMARY = f"{CATALOG}.gold.user_channel_summary"


# ============================================================
# OBSERVABILIDAD
# ============================================================

OBSERVABILITY_EVENT_LOG = f"{CATALOG}.{OBSERVABILITY_SCHEMA}.pipeline_event_log"


# ============================================================
# SCHEMAS RAW BRONZE
# En Bronze todos los campos llegan como STRING.
# El casteo y validacion real se hacen en Silver.
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
# Se conservan para documentar y validar el arquetipo,
# aunque bronze.py ahora declara tablas explicitas.
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
    Retorna el nombre logico de la tabla Bronze.
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

    with open(ARCHETYPE_PATH, "r", encoding="utf-8") as file:
        archetypes = json.load(file)

    if not isinstance(archetypes, list):
        raise TypeError("ingestion_archetypes.json debe ser una lista JSON.")

    active_archetypes = [
        archetype
        for archetype in archetypes
        if isinstance(archetype, dict) and archetype.get("active") is True
    ]

    if len(active_archetypes) == 0:
        raise ValueError("No hay fuentes activas en ingestion_archetypes.json.")

    required_fields = [
        "source_name",
        "source_path",
        "file_format",
        "target_table",
        "active"
    ]

    for archetype in active_archetypes:
        missing_fields = [
            field
            for field in required_fields
            if field not in archetype or archetype[field] in [None, ""]
        ]

        if missing_fields:
            raise ValueError(
                f"Arquetipo incompleto: {archetype}. "
                f"Campos faltantes: {missing_fields}"
            )

    return active_archetypes


# ============================================================
# COLUMNAS TECNICAS DE AUDITORIA
# ============================================================

def add_audit_columns(df, source_name: str):
    """
    Agrega columnas tecnicas de auditoria en Bronze.
    Usa _metadata.file_path, disponible cuando se lee con Auto Loader.
    """

    data_columns = [
        c for c in df.columns
        if c != "_metadata"
    ]

    return (
        df
        .withColumn("_source_name", lit(source_name))
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
    Limpia espacios al inicio y final.
    Convierte cadenas vacias a NULL.
    """

    cleaned = trim(col(column_name))

    return when(cleaned == "", lit(None)).otherwise(cleaned)


def normalize_code(column_name: str):
    """
    Normaliza codigos a mayusculas.
    Ejemplo: pais, moneda.
    """

    cleaned = upper(trim(col(column_name)))

    return when(cleaned == "", lit(None)).otherwise(cleaned)


def normalize_lower(column_name: str):
    """
    Normaliza textos categoricos a minusculas.
    Ejemplo: canal, estado, tipo de transaccion.
    """

    cleaned = lower(trim(col(column_name)))

    return when(cleaned == "", lit(None)).otherwise(cleaned)


def parse_amount(column_name: str):
    """
    Convierte montos recibidos como texto a decimal(18,2).
    Limpia simbolos de moneda y caracteres no numericos.
    """

    clean_col = regexp_replace(col(column_name), r"[^0-9\.\-]", "")

    return when(
        clean_col == "",
        lit(None).cast("decimal(18,2)")
    ).otherwise(
        clean_col.cast("decimal(18,2)")
    )


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
# FUNCION PARA TABLA DE CUARENTENA
# ============================================================

def quarantine_df(df, source_name: str, rejection_reason: str):
    """
    Construye registros rechazados para la tabla de cuarentena.
    """

    return (
        df
        .withColumn("source_name", lit(source_name))
        .withColumn("rejection_reason", lit(rejection_reason))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn(
            "raw_record",
            to_json(struct(*[col(c) for c in df.columns]))
        )
        .select(
            "source_name",
            "rejection_reason",
            "processing_timestamp",
            "raw_record"
        )
    )


# ============================================================
# LISTAS DE VALORES VALIDOS
# Utiles para validaciones y documentacion
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
