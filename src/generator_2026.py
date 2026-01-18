# src/generator_2026.py
from __future__ import annotations

import csv
import json
import random
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import settings
import utils


log = utils.get_logger("v16.generator")


# -----------------------------
# Safe parsing (para CSV legacy)
# -----------------------------
def _safe_int(x: object) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip().replace(",", ".")
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return int(round(float(s)))
    except Exception:
        return None


def _safe_str(x: object) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _mode_int(values: List[Optional[int]]) -> Optional[int]:
    counts: Dict[int, int] = {}
    for v in values:
        if v is None:
            continue
        counts[v] = counts.get(v, 0) + 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]


# -----------------------------
# Detectores de “día” y targets V16
# -----------------------------
def _day_codes() -> Set[int]:
    """
    Detecta códigos de día de forma determinista (settings -> keywords).
    Fallback: si no detecta, usa el modo del 2024.
    """
    codes = utils.day_codes_from_settings()
    return set(codes)


def _target_sets() -> Tuple[Set[int], Set[int], Set[str]]:
    return utils.target_sets_from_settings()


def _is_day(row: Dict[str, str], day_codes: Set[int]) -> bool:
    if not day_codes:
        return False
    v = _safe_int(row.get("CONDICION_ILUMINACION"))
    return v is not None and v in day_codes


def _is_target(
    row: Dict[str, str],
    target_meteo: Set[int],
    target_ilum: Set[int],
    target_zona: Set[str],
) -> bool:
    zona_ok = True
    if target_zona and "ZONA_AGRUPADA" in row:
        zona_ok = _safe_str(row.get("ZONA_AGRUPADA")) in target_zona

    meteo_ok = False
    if target_meteo and "CONDICION_METEO" in row:
        v = _safe_int(row.get("CONDICION_METEO"))
        meteo_ok = v is not None and v in target_meteo

    ilum_ok = False
    if target_ilum and "CONDICION_ILUMINACION" in row:
        v = _safe_int(row.get("CONDICION_ILUMINACION"))
        ilum_ok = v is not None and v in target_ilum

    if not target_meteo and not target_ilum and not target_zona:
        return False

    return zona_ok and (meteo_ok or ilum_ok)


# -----------------------------
# Ajuste “crítico realista” (moderado)
# -----------------------------
def _apply_critique_adjustment(
    row: Dict[str, str],
    rng: random.Random,
    is_target: bool,
    is_day: bool,
) -> None:
    """
    Ajuste defendible y moderado:
      - Target + día: impacto mayor (por cuestionar efectividad V16 de día)
      - Target no día: leve
      - Fuera de target: casi nulo

    Implementación:
      - Si víctimas=0, prob pequeña de pasar a 1
      - Si víctimas>0, prob pequeña de sumar 1
      - Si existe TOTAL_HL24H se incrementa ahí para coherencia
    """
    if "TOTAL_VICTIMAS_24H" not in row:
        return

    has_mu = "TOTAL_MU24H" in row
    has_hg = "TOTAL_HG24H" in row
    has_hl = "TOTAL_HL24H" in row
    has_total = "TOTAL_VICTIMAS_24H" in row

    total = _safe_int(row.get("TOTAL_VICTIMAS_24H")) or 0
    mu = _safe_int(row.get("TOTAL_MU24H")) or 0
    hg = _safe_int(row.get("TOTAL_HG24H")) or 0
    hl = _safe_int(row.get("TOTAL_HL24H")) or 0

    if has_mu and has_hg and has_hl:
        total = max(0, mu + hg + hl)

    if is_target and is_day:
        p_zero_to_one = 0.035
        p_plus_one = 0.090
    elif is_target and not is_day:
        p_zero_to_one = 0.015
        p_plus_one = 0.040
    else:
        p_zero_to_one = 0.004
        p_plus_one = 0.010

    add = 0
    if total <= 0:
        if rng.random() < p_zero_to_one:
            add = 1
    else:
        if rng.random() < p_plus_one:
            add = 1

    if add > 0:
        if has_hl:
            hl += add
            row["TOTAL_HL24H"] = str(hl)

        if has_mu and has_hg and has_hl:
            total = mu + hg + hl
        else:
            total = total + add

        row["TOTAL_VICTIMAS_24H"] = str(max(0, total))

def _safe_rmtree(dir_to_delete: Path, allowed_root: Path) -> None:
    """
    Seguridad anti-cagada: solo permite borrar dentro del root permitido.
    Evita borrar /data entero si settings apunta mal.
    """
    dir_to_delete = dir_to_delete.resolve()
    allowed_root = allowed_root.resolve()

    if allowed_root not in dir_to_delete.parents:
        raise RuntimeError(
            f"Borrado bloqueado por seguridad.\n"
            f"Intento borrar: {dir_to_delete}\n"
            f"Root permitido: {allowed_root}"
        )

    if dir_to_delete.exists():
        shutil.rmtree(dir_to_delete)

# -----------------------------
# Main generator
# -----------------------------
def main() -> None:
    in_2024: Path = settings.BRONZE_2024_FILE
    out_dir: Path = settings.BRONZE_2026_DIR
    out_file: Path = settings.BRONZE_2026_FILE

    if not in_2024.exists():
        raise SystemExit(
            f"No existe Bronze 2024: {in_2024}\n"
            "Ejecuta antes: python src/ingesta_excel.py"
        )

    # 1) Leer 2024
    with in_2024.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = reader.fieldnames or []
        rows_2024 = list(reader)

    if not rows_2024:
        raise SystemExit("El CSV 2024 está vacío.")
    if "ANYO" not in fieldnames:
        fieldnames = list(fieldnames) + ["ANYO"]

    # 2) Limpieza dura de 2026 (evita duplicados por runs previos) + guard rails
    _safe_rmtree(out_dir, settings.BRONZE_ACCIDENTES_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 3) Detectar códigos de día de settings (fallback: modo 2024)
    day_codes = _day_codes()
    if not day_codes:
        ilum_2024 = [_safe_int(r.get("CONDICION_ILUMINACION")) for r in rows_2024]
        m = _mode_int(ilum_2024)
        if m is not None:
            day_codes = {m}
        log.warning(f"No se detectó 'día' en settings. Fallback day_codes={day_codes}")

    # 4) Targets V16
    target_meteo, target_ilum, target_zona = _target_sets()

    # 5) Volumen realista: igual a 2024 +/- growth acotado
    n = len(rows_2024)
    growth = float(getattr(settings, "GEN_2026_GROWTH", 0.01))
    growth = max(-0.05, min(0.05, growth))  # acota [-5%, +5%]
    n_out = int(round(n * (1.0 + growth)))

    # 6) IDs únicos si existe
    max_id = 0
    for r in rows_2024:
        v = _safe_int(r.get("ID_ACCIDENTE"))
        if v is not None:
            max_id = max(max_id, v)
    next_id = max_id + 1

    # 7) Generación reproducible
    seed = int(getattr(settings, "GEN_2026_SEED", 20260114))
    rng = random.Random(seed)

    with out_file.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        for _ in range(n_out):
            base = dict(rng.choice(rows_2024))

            # Año 2026
            base["ANYO"] = "2026"

            # ID único si existe
            if "ID_ACCIDENTE" in base:
                base["ID_ACCIDENTE"] = str(next_id)
                next_id += 1

            day = _is_day(base, day_codes)
            target = _is_target(base, target_meteo, target_ilum, target_zona)

            # Ajuste crítico moderado
            _apply_critique_adjustment(base, rng, is_target=target, is_day=day)
            writer.writerow(base)

    # Guardar metadata (trazabilidad real)
    meta = {
        "seed": seed,
        "growth": growth,
        "n_2024": n,
        "n_2026_generated": n_out,
        "day_codes": sorted(list(day_codes)),
        "target_meteo": sorted(list(target_meteo)),
        "target_ilum": sorted(list(target_ilum)),
        "target_zona": sorted(list(target_zona)),
        "source_2024": str(in_2024),
        "output_2026": str(out_file),
    }
    meta_path = out_dir / "_GEN_METADATA.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info(f"Generado 2026 -> {out_file}")
    log.info("Se limpió anyo=2026 antes de escribir para evitar duplicados.")
    log.info(f"Metadata -> {meta_path}")
    log.info("Ahora ejecuta: python src/etl_main.py (dashboard solo leerá Gold).")


if __name__ == "__main__":
    main()
