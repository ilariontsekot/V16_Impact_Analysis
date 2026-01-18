from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional, Set, Tuple

try:
    import settings  
except ModuleNotFoundError:
    from src import settings  

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:
    DataFrame = object  
    F = None  


def get_logger(name: str = "v16") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


log = get_logger("v16")


def ensure_columns_present(df: "DataFrame", required: Iterable[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")


def apply_mappings(df, mapping_dict, source_col, target_col):
    """
    Transformación robusta: convierte códigos DGT en descripciones.
    """
    if F is None:
        raise RuntimeError("PySpark no está disponible. apply_mappings requiere pyspark.")
    if not isinstance(mapping_dict, dict) or not mapping_dict:
        return df

    flat_list = []
    for k, v in mapping_dict.items():
        flat_list.extend([F.lit(str(k)), F.lit(v)])

    mapping_expr = F.create_map(flat_list)

    df = df.withColumn(target_col, mapping_expr[F.col(source_col).cast("string")])
    return df


def day_codes_from_settings() -> Set[int]:
    """
    Detecta códigos de “día” desde settings.ILUMINACION_MAP.
    (Sirve para generador y dashboard con coherencia total)
    """
    day_codes: Set[int] = set()
    ilum_map = getattr(settings, "ILUMINACION_MAP", {}) or {}
    if not isinstance(ilum_map, dict):
        return set()

    for k, v in ilum_map.items():
        txt = str(v).lower()
        if any(w in txt for w in getattr(settings, "DAY_KEYWORDS", ("día", "dia", "diurn"))):
            try:
                day_codes.add(int(str(k)))
            except Exception:
                pass

    return day_codes


def _to_int_safe(x) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip().replace(",", ".")
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return int(round(float(s)))
    except Exception:
        return None


def target_sets_from_settings() -> Tuple[Set[int], Set[int], Set[str]]:
    target = getattr(settings, "V16_TARGET_CONDITIONS", {}) or {}
    meteo: Set[int] = set()
    ilum: Set[int] = set()
    zona: Set[str] = set()

    for x in target.get("METEO", []) or []:
        v = _to_int_safe(x)
        if v is not None:
            meteo.add(v)

    for x in target.get("ILUMINACION", []) or []:
        v = _to_int_safe(x)
        if v is not None:
            ilum.add(v)

    for x in target.get("ZONA", []) or []:
        s = str(x).strip()
        if s:
            zona.add(s)

    return meteo, ilum, zona


def with_pipeline_warning(df: "DataFrame", warning: str) -> "DataFrame":
    if F is None:
        raise RuntimeError("PySpark no está disponible. with_pipeline_warning requiere pyspark.")
    if "PIPE_WARNINGS" not in df.columns:
        df = df.withColumn("PIPE_WARNINGS", F.array().cast("array<string>"))

    return df.withColumn(
        "PIPE_WARNINGS",
        F.array_distinct(F.concat(F.col("PIPE_WARNINGS"), F.array(F.lit(warning)))),
    )


def calcular_criticidad_v16(df: "DataFrame") -> "DataFrame":
    if F is None:
        raise RuntimeError("PySpark no está disponible. calcular_criticidad_v16 requiere pyspark.")

    required_for_rule = ["CONDICION_METEO", "CONDICION_ILUMINACION", "ZONA_AGRUPADA"]
    ensure_columns_present(df, required_for_rule)

    target_meteo, target_ilum, target_zona = target_sets_from_settings()

    if not target_meteo and not target_ilum and not target_zona:
        log.warning("V16_TARGET_CONDITIONS vacío: V16_ESTRATEGICA=False para todos.")
        return df.withColumn("V16_ESTRATEGICA", F.lit(False))

    zona_ok = F.lit(True)
    if target_zona:
        zona_ok = F.trim(F.col("ZONA_AGRUPADA").cast("string")).isin([str(z).strip() for z in target_zona])

    meteo_ok = F.lit(False)
    if target_meteo:
        meteo_ok = F.col("CONDICION_METEO").isin(list(target_meteo))

    ilum_ok = F.lit(False)
    if target_ilum:
        ilum_ok = F.col("CONDICION_ILUMINACION").isin(list(target_ilum))

    is_target = zona_ok & (meteo_ok | ilum_ok)
    return df.withColumn("V16_ESTRATEGICA", F.coalesce(is_target, F.lit(False)))
