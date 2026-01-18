from __future__ import annotations

from pathlib import Path

import pandas as pd

import settings


def transformar_fuente_original() -> None:
    """
    Capa BRONZE (Landing):
    - Lee el Excel legacy (DGT)
    - Normaliza cabeceras
    - Escribe un dataset raw (CSV) en carpeta Spark-friendly: .../bronze/accidentes_raw/anyo=2024/
    """
    print("[1/3] Iniciando conversión de Excel a Bronze (raw CSV)...")

    excel_path: Path = settings.EXCEL_PATH
    if not excel_path.exists():
        raise FileNotFoundError(f"No se encuentra el Excel en: {excel_path}")

    df = pd.read_excel(excel_path)

    # Limpieza de cabeceras (vital para Spark)
    df.columns = [str(c).strip() for c in df.columns]

    # Aseguramos directorio destino
    out_dir: Path = settings.BRONZE_2024_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path: Path = out_dir / "accidentes.csv"

    # Separador ';' para evitar conflictos con comas en textos
    df.to_csv(out_path, index=False, sep=";")

    # Mini-métrica rápida para trazabilidad local
    print(f"Bronze 2024 generado: {out_path} | filas={len(df):,} cols={len(df.columns)}")


if __name__ == "__main__":
    transformar_fuente_original()
