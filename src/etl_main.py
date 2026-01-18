# src/etl_main.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import settings
import utils

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError as e:
    raise SystemExit(
        "PySpark no está instalado.\n"
        "Instala con: pip install pyspark\n"
        "y ejecuta: python src/etl_main.py"
    ) from e

log = utils.get_logger("v16.etl")


# -----------------------------
# Spark + filesystem helpers
# -----------------------------
def _create_spark(app_name: str = settings.SPARK_APP_NAME) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", settings.TIMEZONE)
        .config("spark.sql.shuffle.partitions", str(settings.SPARK_SHUFFLE_PARTITIONS))
        .getOrCreate()
    )


def _as_uri(p: Path) -> str:
    return "file://" + str(p.resolve())


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _is_dict(x) -> bool:
    return isinstance(x, dict)


def _is_empty_df(df: DataFrame) -> bool:
    # Evita count() completo; dispara un job pequeño
    return len(df.take(1)) == 0


# -----------------------------
# Bronze read (determinista)
# -----------------------------
def _bronze_paths() -> List[str]:
    """
    Estructura esperada:
      data/landing/bronze/accidentes_raw/ANYO=2024/accidentes.csv
      data/landing/bronze/accidentes_raw/ANYO=2026/accidentes.csv
    """
    base: Path = settings.BRONZE_ACCIDENTES_DIR
    p24 = settings.BRONZE_2024_DIR
    p26 = settings.BRONZE_2026_DIR

    if not p24.exists():
        raise FileNotFoundError(
            f"No existe Bronze 2024 en: {p24}\n"
            "Ejecuta antes: python src/ingesta_excel.py"
        )

    paths: List[str] = []

    # 2024 (si existe el archivo concreto, usa ese; si no, el dir)
    paths.append(
        str(settings.BRONZE_2024_FILE) if settings.BRONZE_2024_FILE.exists() else str(p24)
    )

    # 2026 opcional (comprobación RECURSIVA para tu estructura ANYO=2026/accidentes.csv)
    if p26.exists() and any(p26.rglob("*.csv")):
        paths.append(str(p26))
    else:
        log.warning(f"Bronze 2026 no encontrado en {p26}. Solo se procesará 2024.")

    log.info(f"Bronze base: {base}")
    return paths


def _read_bronze(spark: SparkSession) -> DataFrame:
    paths = _bronze_paths()
    log.info(f"Leyendo Bronze desde: {paths}")

    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("recursiveFileLookup", str(settings.SPARK_READ_RECURSIVE).lower())
        .option("pathGlobFilter", "*.csv")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(paths)
    )

    # trazabilidad extra
    df = df.withColumn("SOURCE_FILE", F.input_file_name())
    df = df.withColumn("INGEST_TS", F.current_timestamp())

    if _is_empty_df(df):
        raise RuntimeError("Bronze se leyó vacío. Revisa separador ';' y ficheros.")

    return df


# -----------------------------
# Casting seguro (sin reventar)
# -----------------------------
def _normalize_number_str(col: F.Column) -> F.Column:
    """
    Normaliza strings numéricas tolerando coma decimal:
      " 7,319 " -> "7.319"
    """
    return F.regexp_replace(F.trim(col.cast("string")), ",", ".")


def _safe_double(colname: str) -> F.Column:
    return _normalize_number_str(F.col(colname)).cast("double")


def _safe_int(colname: str) -> F.Column:
    d = _normalize_number_str(F.col(colname)).cast("double")
    return F.round(d).cast("int")


def _apply_casts(df: DataFrame) -> DataFrame:
    int_cols = [
        "ID_ACCIDENTE",
        "ANYO",
        "MES",
        "DIA_SEMANA",
        "HORA",
        "ZONA",
        "ZONA_AGRUPADA",
        "TIPO_VIA",
        "TITULARIDAD_VIA",
        "TIPO_ACCIDENTE",
        "CONDICION_FIRME",
        "CONDICION_ILUMINACION",
        "CONDICION_METEO",
        "VISIB_RESTRINGIDA_POR",
        "TOTAL_VICTIMAS_24H",
        "TOTAL_MU24H",
        "TOTAL_HG24H",
        "TOTAL_HL24H",
    ]
    double_cols = ["KM"]

    for c in int_cols:
        if c in df.columns:
            df = df.withColumn(c, _safe_int(c))

    for c in double_cols:
        if c in df.columns:
            df = df.withColumn(c, _safe_double(c))

    return df


# -----------------------------
# DQ (missing + dominio + rango)
# -----------------------------
def _dq_missing(df: DataFrame) -> DataFrame:
    required = settings.REQUIRED_COLUMNS
    exprs = []

    missing_schema = [c for c in required if c not in df.columns]
    if missing_schema and settings.FAIL_FAST_ON_MISSING_REQUIRED_COLUMNS:
        raise ValueError(f"Faltan columnas requeridas en Bronze: {missing_schema}")
    elif missing_schema:
        log.warning(f"Faltan columnas requeridas en Bronze: {missing_schema} (no fail-fast)")

    for name in required:
        if name in df.columns:
            exprs.append(F.when(F.col(name).isNull(), F.lit(name)).otherwise(F.lit(None)))

    if not exprs:
        return df.withColumn("DQ_MISSING_FIELDS", F.array().cast("array<string>"))

    df = df.withColumn("DQ_RAW_MISSING", F.array(*exprs))
    return (
        df.withColumn("DQ_MISSING_FIELDS", F.expr("filter(DQ_RAW_MISSING, x -> x is not null)"))
        .drop("DQ_RAW_MISSING")
    )


def _dq_domain(df: DataFrame) -> DataFrame:
    exprs = []
    for col, allowed in (settings.DOMAIN_CONSTRAINTS or {}).items():
        if col not in df.columns or not allowed:
            continue
        allowed_list = sorted(list(allowed))
        exprs.append(
            F.when(
                F.col(col).isNotNull() & (~F.col(col).isin(allowed_list)),
                F.lit(col),
            ).otherwise(F.lit(None))
        )

    if not exprs:
        return df.withColumn("DQ_INVALID_DOMAIN_FIELDS", F.array().cast("array<string>"))

    df = df.withColumn("DQ_RAW_DOMAIN", F.array(*exprs))
    return (
        df.withColumn("DQ_INVALID_DOMAIN_FIELDS", F.expr("filter(DQ_RAW_DOMAIN, x -> x is not null)"))
        .drop("DQ_RAW_DOMAIN")
    )


def _dq_range(df: DataFrame) -> DataFrame:
    """
    Construye DQ_INVALID_RANGE_FIELDS con columnas fuera de rango.
    Nota: no forzamos int(mn/mx). Comparamos con lit(mn/mx) tal cual.
    """
    exprs = []
    for col, (mn, mx) in (settings.RANGE_CONSTRAINTS or {}).items():
        if col not in df.columns:
            continue

        invalid = F.lit(False)
        if mn is not None:
            invalid = invalid | (F.col(col) < F.lit(mn))
        if mx is not None:
            invalid = invalid | (F.col(col) > F.lit(mx))

        exprs.append(
            F.when(F.col(col).isNotNull() & invalid, F.lit(col)).otherwise(F.lit(None))
        )

    if not exprs:
        return df.withColumn("DQ_INVALID_RANGE_FIELDS", F.array().cast("array<string>"))

    df = df.withColumn("DQ_RAW_RANGE", F.array(*exprs))
    return (
        df.withColumn("DQ_INVALID_RANGE_FIELDS", F.expr("filter(DQ_RAW_RANGE, x -> x is not null)"))
        .drop("DQ_RAW_RANGE")
    )


def _dq_finalize(df: DataFrame) -> DataFrame:
    # corrupt record solo si existe
    if "_corrupt_record" in df.columns:
        df = df.withColumn(
            "DQ_CORRUPT",
            F.when(F.col("_corrupt_record").isNotNull(), F.array(F.lit("_corrupt_record")))
            .otherwise(F.array().cast("array<string>")),
        )
    else:
        df = df.withColumn("DQ_CORRUPT", F.array().cast("array<string>"))

    # asegura existencia de arrays
    for c in ["DQ_MISSING_FIELDS", "DQ_INVALID_DOMAIN_FIELDS", "DQ_INVALID_RANGE_FIELDS"]:
        if c not in df.columns:
            df = df.withColumn(c, F.array().cast("array<string>"))

    # domain constraints también participan en validez
    df = df.withColumn(
        "DQ_IS_VALID",
        (F.size("DQ_MISSING_FIELDS") == 0)
        & (F.size("DQ_INVALID_DOMAIN_FIELDS") == 0)
        & (F.size("DQ_INVALID_RANGE_FIELDS") == 0)
        & (F.size("DQ_CORRUPT") == 0),
    )

    return df.withColumn("DQ_IS_VALID", F.coalesce(F.col("DQ_IS_VALID").cast("boolean"), F.lit(False)))


def _dq(df: DataFrame) -> DataFrame:
    return _dq_finalize(_dq_range(_dq_domain(_dq_missing(df))))


# -----------------------------
# Writes
# -----------------------------
def _write_parquet(df: DataFrame, out_dir: Path, partition_col: str | None = None) -> None:
    _ensure_dir(out_dir)

    writer = df
    if partition_col and partition_col in df.columns and settings.WRITE_REPARTITION_BY_PARTITION_COL:
        writer = writer.repartition(F.col(partition_col))

    if partition_col and partition_col in df.columns:
        writer.write.mode("overwrite").partitionBy(partition_col).parquet(_as_uri(out_dir))
    else:
        writer.write.mode("overwrite").parquet(_as_uri(out_dir))


# -----------------------------
# SILVER
# -----------------------------
def run_silver(spark: SparkSession) -> Path:
    log.info("[Silver] Bronze -> limpieza + tipado + DQ...")

    df = _dq(_apply_casts(_read_bronze(spark)))

    m = (
        df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col("DQ_IS_VALID"), F.lit(1)).otherwise(F.lit(0))).alias("good"),
        )
        .collect()[0]
    )

    total = int(m["total"])
    good = int(m["good"])
    bad = total - good
    log.info(f"DQ -> total={total:,} | good={good:,} | bad={bad:,}")

    silver_dir: Path = settings.SILVER_ACCIDENTES_DIR
    bad_dir: Path = settings.SILVER_BAD_RECORDS_DIR
    _ensure_dir(silver_dir)
    _ensure_dir(bad_dir)

    good_df = df.filter(F.col("DQ_IS_VALID")).drop("DQ_IS_VALID")
    bad_df = df.filter(~F.col("DQ_IS_VALID")).drop("DQ_IS_VALID")

    if good == 0:
        log.warning("DQ dejó 0 registros válidos. Guardando TODO en Silver y TODO en bad_records.")
        all_df = df.drop("DQ_IS_VALID")
        _write_parquet(all_df, silver_dir, partition_col="ANYO")
        _write_parquet(all_df, bad_dir, partition_col="ANYO")
    else:
        _write_parquet(good_df, silver_dir, partition_col="ANYO")
        _write_parquet(bad_df, bad_dir, partition_col="ANYO")

    log.info(f"Silver -> {silver_dir}")
    log.info(f"Bad records -> {bad_dir}")
    return silver_dir


# -----------------------------
# GOLD
# -----------------------------
def run_gold(spark: SparkSession, silver_dir: Path) -> None:
    log.info("[Gold] Enrichment + publicación...")

    df = spark.read.parquet(_as_uri(silver_dir))
    if _is_empty_df(df):
        raise RuntimeError("Silver está vacío. Revisa Bronze / casting / DQ.")

    def _assert_df(step: str) -> None:
        nonlocal df
        if df is None or not hasattr(df, "columns"):
            raise RuntimeError(f"df inválido en Gold ({step}): {type(df)}")

    # Mappings
    meteo_map: Dict = getattr(settings, "METEO_MAP", {}) or {}
    ilum_map: Dict = getattr(settings, "ILUMINACION_MAP", {}) or {}
    tipo_acc_map: Dict = getattr(settings, "TIPO_ACC_MAP", {}) or {}

    if "CONDICION_METEO" in df.columns and _is_dict(meteo_map) and meteo_map:
        df = utils.apply_mappings(df, meteo_map, "CONDICION_METEO", "METEO_DESC")
        _assert_df("apply_mappings METEO")

    if "CONDICION_ILUMINACION" in df.columns and _is_dict(ilum_map) and ilum_map:
        df = utils.apply_mappings(df, ilum_map, "CONDICION_ILUMINACION", "LUZ_DESC")
        _assert_df("apply_mappings ILUMINACION")

    if "TIPO_ACCIDENTE" in df.columns and _is_dict(tipo_acc_map) and tipo_acc_map:
        df = utils.apply_mappings(df, tipo_acc_map, "TIPO_ACCIDENTE", "TIPO_ACC_DESC")
        _assert_df("apply_mappings TIPO_ACC")

    # Reglas de negocio
    try:
        df = utils.calcular_criticidad_v16(df)
        _assert_df("calcular_criticidad_v16")
    except Exception as e:
        msg = f"GOLD_RULES_ERROR: criticidad_v16 ({type(e).__name__}: {e})"
        log.error(msg)

        if df is None:
            raise RuntimeError(
                "df=None tras calcular_criticidad_v16. Revisa utils.calcular_criticidad_v16 (debe devolver DF)."
            ) from e

        df = utils.with_pipeline_warning(df, msg)

        if settings.FAIL_ON_GOLD_RULES_ERROR:
            raise
        if "V16_ESTRATEGICA" not in df.columns:
            df = df.withColumn("V16_ESTRATEGICA", F.lit(None).cast("boolean"))

    # Publicación
    gold_dir: Path = settings.GOLD_ACCIDENTES_DIR
    kpis_dir: Path = settings.GOLD_KPIS_DIR
    _ensure_dir(gold_dir)
    _ensure_dir(kpis_dir)

    _write_parquet(df, gold_dir, partition_col="ANYO")
    log.info(f"Gold dataset -> {gold_dir}")

    # KPIs
    group_cols = [
        c for c in ["ANYO", "V16_ESTRATEGICA", "CONDICION_METEO", "CONDICION_ILUMINACION"]
        if c in df.columns
    ]

    aggs = [F.count(F.lit(1)).alias("ACCIDENTES")]
    if "TOTAL_VICTIMAS_24H" in df.columns:
        aggs += [
            F.sum("TOTAL_VICTIMAS_24H").alias("VICTIMAS_TOTALES_24H"),
            F.avg("TOTAL_VICTIMAS_24H").alias("VICTIMAS_MEDIA_24H"),
        ]

    kpis = df.groupBy(*group_cols).agg(*aggs) if group_cols else df.agg(*aggs)
    _write_parquet(kpis, kpis_dir, partition_col="ANYO" if "ANYO" in kpis.columns else None)
    log.info(f"Gold KPIs -> {kpis_dir}")


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    spark = _create_spark()
    try:
        silver_dir = run_silver(spark)
        run_gold(spark, silver_dir)
        log.info("Pipeline completo: Bronze -> Silver -> Gold listo para Streamlit.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

