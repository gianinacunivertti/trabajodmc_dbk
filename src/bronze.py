# ============================================================
# bronze.py
# Proyecto FinPay - Lakehouse Fraud Detection
# Capa Bronze metadata-driven
# ============================================================

from pyspark import pipelines as dp

from src.utils import (
    read_ingestion_archetypes,
    get_schema_by_source,
    get_bronze_table_name_by_source,
    add_audit_columns
)


# ============================================================
# FACTORY DE TABLAS BRONZE
# ============================================================

def build_bronze_table(archetype: dict):
    """
    Construye dinamicamente una Streaming Table Bronze
    a partir de una entrada del archivo ingestion_archetypes.json.

    Campos esperados en el arquetipo:
    - source_name
    - source_path
    - file_format
    - delimiter
    - header
    - target_table
    - active
    """

    source_name = str(archetype["source_name"]).strip().lower()
    source_path = archetype["source_path"]
    file_format = str(archetype["file_format"]).strip().lower()
    delimiter = archetype.get("delimiter")
    header = archetype.get("header")

    schema = get_schema_by_source(source_name)
    target_table = get_bronze_table_name_by_source(source_name)

    @dp.table(
        name=target_table,
        comment=f"Bronze raw streaming table para fuente {source_name}"
    )
    def _bronze_table():

        # ----------------------------------------------------
        # CSV: transactions_*.csv
        # ----------------------------------------------------
        if file_format == "csv":
            reader = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaEvolutionMode", "none")
                .option("header", str(header).lower() if header is not None else "true")
                .schema(schema)
            )

            if delimiter:
                reader = reader.option("delimiter", delimiter)

            df = reader.load(source_path)

        # ----------------------------------------------------
        # TXT/TEXT: users_*.txt
        # Se procesa como CSV delimitado, normalmente con "|"
        # ----------------------------------------------------
        elif file_format in ["txt", "text"]:
            reader = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaEvolutionMode", "none")
                .option("header", str(header).lower() if header is not None else "true")
                .schema(schema)
            )

            if delimiter:
                reader = reader.option("delimiter", delimiter)

            df = reader.load(source_path)

        # ----------------------------------------------------
        # JSON: merchants.json
        # ----------------------------------------------------
        elif file_format == "json":
            df = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaEvolutionMode", "none")
                .schema(schema)
                .load(source_path)
            )

        else:
            raise ValueError(
                f"Formato no soportado: {file_format}. Fuente: {source_name}"
            )

        return add_audit_columns(df, source_name)

    return _bronze_table


# ============================================================
# REGISTRO DINAMICO DE FUENTES ACTIVAS
# ============================================================

for archetype in read_ingestion_archetypes():
    build_bronze_table(archetype)