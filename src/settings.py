# src/settings.py
from __future__ import annotations

from pathlib import Path

# =========================
# Proyecto / entorno
# =========================
PROJECT_NAME: str = "V16_Impact_Analysis"
TIMEZONE: str = "Europe/Madrid"

# settings.py lives in <repo>/src/settings.py -> parents[1] is repo root
BASE_DIR: Path = Path(__file__).resolve().parents[1]
DATA_DIR: Path = BASE_DIR / "data"

# =========================
# Landing / Source
# =========================
LANDING_DIR: Path = DATA_DIR / "landing"
EXCEL_FILENAME: str = "tabla.xlsx"
EXCEL_PATH: Path = LANDING_DIR / EXCEL_FILENAME

# =========================
# Bronze (raw datasets)
# =========================
BRONZE_DIR: Path = LANDING_DIR / "bronze"
BRONZE_ACCIDENTES_DIR: Path = BRONZE_DIR / "accidentes_raw"
BRONZE_2024_DIR: Path = BRONZE_ACCIDENTES_DIR / "ANYO=2024"
BRONZE_2026_DIR: Path = BRONZE_ACCIDENTES_DIR / "ANYO=2026"
BRONZE_2024_FILE: Path = BRONZE_2024_DIR / "accidentes.csv"
BRONZE_2026_FILE: Path = BRONZE_2026_DIR / "accidentes.csv"

# =========================
# Silver (clean + schema + DQ)
# =========================
STAGING_DIR: Path = DATA_DIR / "staging"
SILVER_DIR: Path = STAGING_DIR / "silver"
SILVER_ACCIDENTES_DIR: Path = SILVER_DIR / "accidentes"
SILVER_BAD_RECORDS_DIR: Path = SILVER_DIR / "bad_records"

# =========================
# Gold (analytics / BI-ready)
# =========================
ANALYTICS_DIR: Path = DATA_DIR / "analytics"
GOLD_DIR: Path = ANALYTICS_DIR / "gold"
GOLD_ACCIDENTES_DIR: Path = GOLD_DIR / "accidentes"
GOLD_KPIS_DIR: Path = GOLD_DIR / "kpis"

# =========================
# Spark tuning (local friendly)
# =========================
SPARK_APP_NAME: str = "V16_Impact_ETL"
SPARK_SHUFFLE_PARTITIONS: int = 8
SPARK_READ_RECURSIVE: bool = True

# Para evitar “small files” extremos (mejora sin complicarte):
# Reparte por partición lógica (ANYO) antes de escribir.
WRITE_REPARTITION_BY_PARTITION_COL: bool = True

# =========================
# DGT dictionaries (mappings)
# =========================
METEO_MAP = {
    "1": "Despejado",
    "2": "Lluvia débil",
    "3": "Lluvia fuerte",
    "4": "Granizo",
    "5": "Nevada",
    "6": "Niebla/Bruma",
    "7": "Viento fuerte",
    "8": "Otros",
    "9": "Desconocido",
}

ILUMINACION_MAP = {
    "1": "Pleno día",
    "2": "Crepúsculo",
    "3": "Noche (Ilum. suficiente)",
    "4": "Noche (Ilum. insuficiente)",
    "5": "Noche (Sin iluminación)",
    "9": "Otros",
}

TIPO_ACC_MAP = {
    "13": "Obstáculo en calzada",
    "14": "Atropello",
    "0": "Otros",
}

MES_MAP = {
    "1": "Enero",
    "2": "Febrero",
    "3": "Marzo",
    "4": "Abril",
    "5": "Mayo",
    "6": "Junio",
    "7": "Julio",
    "8": "Agosto",
    "9": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre",
}

DIA_SEMANA_MAP = {
    "1": "Lunes",
    "2": "Martes",
    "3": "Miércoles",
    "4": "Jueves",
    "5": "Viernes",
    "6": "Sábado",
    "7": "Domingo",
}

# =========================
# V16 business rules
# =========================
V16_TARGET_CONDITIONS = {
    "METEO": [6],          # Niebla
    "ILUMINACION": [4, 5], # Poca o nula iluminación
    "ZONA": ["1"],         # Interurbana (según codificación ZONA_AGRUPADA)
}

# "Impact knob" (si en el futuro quieres simular reducción real)
V16_REDUCTION_FACTOR: float = 0.7

# =========================
# Data contract / DQ policy
# =========================
# Columnas mínimas para permitir análisis (y para calcular criticidad V16)
REQUIRED_COLUMNS = [
    "ANYO",
    "TOTAL_VICTIMAS_24H",
    "CONDICION_METEO",
    "CONDICION_ILUMINACION",
    "ZONA_AGRUPADA",
]

# Controles de rango (min, max). max=None => sin tope superior
RANGE_CONSTRAINTS = {
    "MES": (1, 12),
    "DIA_SEMANA": (1, 7),
    "HORA": (0, 23),
    "TOTAL_VICTIMAS_24H": (0, None),
}

# Dominios válidos (si existe la columna)
DOMAIN_CONSTRAINTS = {
    "CONDICION_METEO": set(int(k) for k in METEO_MAP.keys() if k.isdigit()),
    "CONDICION_ILUMINACION": set(int(k) for k in ILUMINACION_MAP.keys() if k.isdigit()),
}

# Política de fallo:
# - Si faltan columnas requeridas -> STOP (esto es “serio”)
FAIL_FAST_ON_MISSING_REQUIRED_COLUMNS: bool = True

# - Si falla regla de negocio Gold (criticidad) -> NO ocultar nunca.
#   True => rompe; False => publica con warning explícito.
FAIL_ON_GOLD_RULES_ERROR: bool = False

# =========================
# Generator 2026 (reproducible)
# =========================
GEN_2026_SEED: int = 20260114
GEN_2026_GROWTH: float = 0.01  # +1% suave (acotado en el generador)

# Detectores de "día" (para coherencia generador + dashboard)
DAY_KEYWORDS = ("día", "dia", "diurn", "plena luz", "luz diurna")
