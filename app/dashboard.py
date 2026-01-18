# app/dashboard.py
from __future__ import annotations

from pathlib import Path
import sys
import glob

import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
import altair as alt

# -----------------------------
# sys.path estable (CRÍTICO)
# -----------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import settings  # noqa: E402
import utils     # noqa: E402


# -----------------------------
# Data read
# -----------------------------
@st.cache_data(show_spinner=False)
def _read_parquet_dir(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe: {path}")

    files = glob.glob(str(path / "**" / "*.parquet"), recursive=True)
    if not files:
        raise FileNotFoundError(f"No hay .parquet en: {path}")

    table = pq.read_table(files)
    return table.to_pandas()


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype("string").str.replace(",", ".", regex=False), errors="coerce")


def _parse_bool_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False)
    ss = s.astype("string").str.lower().str.strip()
    return ss.isin(["true", "1", "t", "yes", "y", "si", "sí"]).fillna(False)


def _ensure_desc_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Si el ETL ya metió desc, no tocamos
    if "CONDICION_ILUMINACION" in out.columns and "LUZ_DESC" not in out.columns:
        out["LUZ_DESC"] = _to_num(out["CONDICION_ILUMINACION"]).astype("Int64").astype("string").fillna("Desconocido")

    if "CONDICION_METEO" in out.columns and "METEO_DESC" not in out.columns:
        out["METEO_DESC"] = _to_num(out["CONDICION_METEO"]).astype("Int64").astype("string").fillna("Desconocido")

    return out


def _compute_by_year(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    if "TOTAL_VICTIMAS_24H" not in tmp.columns:
        tmp["TOTAL_VICTIMAS_24H"] = 0

    by_year = (
        tmp.groupby("ANYO", dropna=True, as_index=False)
        .agg(
            ACCIDENTES=("ANYO", "size"),
            VICTIMAS_24H=("TOTAL_VICTIMAS_24H", "sum"),
        )
        .sort_values("ANYO")
    )
    by_year["VICT_MEDIA"] = by_year["VICTIMAS_24H"] / by_year["ACCIDENTES"].replace(0, pd.NA)
    return by_year


def _val(by: pd.DataFrame, year: int, col: str):
    r = by[by["ANYO"] == year]
    return None if r.empty else float(r.iloc[0][col])


def _metric_row(title: str, v24, v26, fmt: str):
    c1, c2, c3 = st.columns([2.2, 1.2, 1.8])
    with c1:
        st.markdown(f"**{title}**")
    with c2:
        st.metric("2024", "—" if v24 is None else fmt.format(v24))
    with c3:
        if v26 is None:
            st.metric("2026", "—", delta="—")
        else:
            delta = None if v24 is None else (v26 - v24)
            d = "—" if delta is None else (fmt.format(delta) if "0f" in fmt else f"{delta:+.3f}")
            st.metric("2026", fmt.format(v26), delta=d)


def _chart_year_bars(by_year: pd.DataFrame):
    melted = by_year.melt(
        id_vars=["ANYO"],
        value_vars=["ACCIDENTES", "VICTIMAS_24H", "VICT_MEDIA"],
        var_name="MÉTRICA",
        value_name="VALOR",
    )

    chart = (
        alt.Chart(melted)
        .mark_bar()
        .encode(
            x=alt.X("ANYO:O", title="Año"),
            y=alt.Y("VALOR:Q", title=None),
            column=alt.Column("MÉTRICA:N", title=None),
            tooltip=["ANYO", "MÉTRICA", alt.Tooltip("VALOR:Q", format=",")],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def _line_by_category(df: pd.DataFrame, cat_col: str, value_col: str, title: str):
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{cat_col}:N", title=None, sort=None),
            y=alt.Y(f"{value_col}:Q", title=value_col),
            color=alt.Color("ANYO:N", title="Año"),
            tooltip=["ANYO", cat_col, "ACCIDENTES", "VICTIMAS_24H", "VICT_MEDIA"],
        )
        .properties(height=320, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


# -----------------------------
# App
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="V16 Impact Analysis", layout="wide")

    st.title("V16 Impact Analysis")
    st.caption("Comparativa **2024 real vs 2026 simulado**, consumiendo únicamente **Gold (Parquet)**. Sin transformaciones en el dashboard.")

    # Load Gold
    gold_acc_dir = settings.GOLD_ACCIDENTES_DIR
    try:
        df = _read_parquet_dir(gold_acc_dir)
    except Exception as e:
        st.error(f"No se pudo leer Gold: {e}")
        st.info("Ejecuta antes: python src/etl_main.py")
        st.stop()

    if df.empty:
        st.warning("Gold existe pero está vacío.")
        st.stop()

    # Tipado base mínimo
    if "ANYO" in df.columns:
        df["ANYO"] = _to_num(df["ANYO"]).astype("Int64")
    if "TOTAL_VICTIMAS_24H" in df.columns:
        df["TOTAL_VICTIMAS_24H"] = _to_num(df["TOTAL_VICTIMAS_24H"]).fillna(0)

    df = _ensure_desc_columns(df)

    # Forzamos foco del proyecto (2024 vs 2026) sin UI
    df = df[df["ANYO"].isin([2024, 2026])].copy()
    if df.empty:
        st.warning("No hay datos para 2024/2026 en Gold.")
        st.stop()

    # -----------------------------
    # Tabs
    # -----------------------------
    tab1, tab2, tab3 = st.tabs(["📌 Overview", "☀️ Día vs No día", "🧩 Segmento V16"])

    with tab1:
        st.subheader("KPIs (Δ 2026−2024)")
        by_year = _compute_by_year(df)

        _metric_row("Accidentes", _val(by_year, 2024, "ACCIDENTES"), _val(by_year, 2026, "ACCIDENTES"), "{:,.0f}")
        _metric_row("Víctimas 24h (total)", _val(by_year, 2024, "VICTIMAS_24H"), _val(by_year, 2026, "VICTIMAS_24H"), "{:,.0f}")
        _metric_row("Víctimas 24h (media)", _val(by_year, 2024, "VICT_MEDIA"), _val(by_year, 2026, "VICT_MEDIA"), "{:,.3f}")

        st.divider()
        st.subheader("Comparativa visual")
        _chart_year_bars(by_year)

    with tab2:
        st.subheader("Iluminación (día vs no día)")

        if "CONDICION_ILUMINACION" not in df.columns:
            st.info("No existe CONDICION_ILUMINACION en Gold.")
        else:
            day_codes = utils.day_codes_from_settings() or set()

            tmp = df.copy()
            tmp["ILUM_CODE"] = _to_num(tmp["CONDICION_ILUMINACION"]).astype("Int64")
            tmp["ILUM_DESC"] = tmp.get("LUZ_DESC", tmp["ILUM_CODE"].astype("string")).fillna("Desconocido")
            tmp["TRAMO_DIA"] = tmp["ILUM_CODE"].isin(list(day_codes)).map({True: "Día", False: "No día"})

            dcut = (
                tmp.groupby(["ANYO", "TRAMO_DIA"], dropna=False, as_index=False)
                .agg(
                    ACCIDENTES=("TRAMO_DIA", "size"),
                    VICTIMAS_24H=("TOTAL_VICTIMAS_24H", "sum"),
                )
            )
            dcut["VICT_MEDIA"] = dcut["VICTIMAS_24H"] / dcut["ACCIDENTES"].replace(0, pd.NA)

            c1, c2 = st.columns(2)
            for col, label in [(c1, "Día"), (c2, "No día")]:
                with col:
                    st.markdown(f"**{label}**")
                    part = dcut[dcut["TRAMO_DIA"] == label]
                    _metric_row("Accidentes", _val(part, 2024, "ACCIDENTES"), _val(part, 2026, "ACCIDENTES"), "{:,.0f}")
                    _metric_row("Víctimas media", _val(part, 2024, "VICT_MEDIA"), _val(part, 2026, "VICT_MEDIA"), "{:,.3f}")

            g = (
                tmp.groupby(["ANYO", "ILUM_DESC"], dropna=False, as_index=False)
                .agg(
                    ACCIDENTES=("ILUM_DESC", "size"),
                    VICTIMAS_24H=("TOTAL_VICTIMAS_24H", "sum"),
                )
            )
            g["VICT_MEDIA"] = g["VICTIMAS_24H"] / g["ACCIDENTES"].replace(0, pd.NA)

            _line_by_category(
                g.assign(ILUM_DESC=lambda d: d["ILUM_DESC"].astype(str)),
                "ILUM_DESC",
                "VICT_MEDIA",
                "Víctimas media por iluminación (2024 vs 2026)",
            )

    with tab3:
        st.subheader("Corte por V16 estratégica")

        if "V16_ESTRATEGICA" not in df.columns:
            st.info("No existe V16_ESTRATEGICA en Gold.")
        else:
            tmp = df.copy()
            tmp["V16_BOOL"] = _parse_bool_series(tmp["V16_ESTRATEGICA"])
            tmp["SEGMENTO_V16"] = tmp["V16_BOOL"].map({True: "V16 estratégica", False: "NO estratégica"})

            seg_year = (
                tmp.groupby(["ANYO", "SEGMENTO_V16"], dropna=False, as_index=False)
                .agg(
                    ACCIDENTES=("SEGMENTO_V16", "size"),
                    VICTIMAS_24H=("TOTAL_VICTIMAS_24H", "sum"),
                )
            )
            seg_year["VICT_MEDIA"] = seg_year["VICTIMAS_24H"] / seg_year["ACCIDENTES"].replace(0, pd.NA)

            chart = (
                alt.Chart(seg_year)
                .mark_bar()
                .encode(
                    x=alt.X("SEGMENTO_V16:N", title=None),
                    y=alt.Y("VICT_MEDIA:Q", title="Víctimas media"),
                    column=alt.Column("ANYO:N", title=None),
                    tooltip=["ANYO", "SEGMENTO_V16", "ACCIDENTES", "VICT_MEDIA"],
                )
                .properties(height=280)
            )
            st.altair_chart(chart, use_container_width=True)

if __name__ == "__main__":
    main()
