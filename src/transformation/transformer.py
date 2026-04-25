"""
Olimpia Data Pipeline - Capa SILVER (Transformación y Limpieza)
================================================================
Responsabilidad: Tomar los datos de la capa BRONZE y producir
la capa SILVER con:
  - Fechas normalizadas a ISO-8601
  - Texto estandarizado (lower/title)
  - Deduplicación por ciudadano+día (conservar más reciente)
  - Campos derivados/enriquecidos
"""

import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("olimpia.transformer")

BASE_DIR   = Path(__file__).resolve().parents[2]
SILVER_DIR = BASE_DIR / "data" / "silver"
LOG_DIR    = BASE_DIR / "data" / "logs"

for d in [SILVER_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_dates(series: pd.Series, col_name: str) -> pd.Series:
    """Parsea fechas flexiblemente y retorna datetime64."""
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)
    bad = parsed.isna().sum()
    if bad:
        logger.warning("  [%s] %d fechas no parseables → NaT", col_name, bad)
    return parsed


def _quality_log(source: str, metrics: dict) -> None:
    log_path = LOG_DIR / "calidad_datos.jsonl"
    with open(log_path, "a", encoding="utf-8") as fp:
        fp.write(json.dumps({
            "evaluated_at": datetime.utcnow().isoformat(),
            "source": source,
            **metrics,
        }, default=str) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN CEA → SILVER
# ─────────────────────────────────────────────────────────────────────────────

def transform_cea(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y enriquecimiento de CEA (clases).

    Campos derivados:
      - fecha_date      : fecha como date
      - clase_norm      : clase en minúsculas estandarizado
      - instructor_norm : instructor title-case
      - es_practica     : bool
      - horas_acum      : horas acumuladas por ciudadano
    """
    logger.info("Transformando CEA…")
    df = df.copy()

    df["clase_norm"]      = df["clase"].str.strip().str.lower()
    df["instructor_norm"] = df["instructor"].str.strip().str.title()

    df["fecha_date"] = _parse_dates(df["fecha"], "cea.fecha").dt.date
    df = df.dropna(subset=["fecha_date"])

    df = df.sort_values("_ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano", "clase_norm", "fecha_date"], keep="first")
    df = df.sort_values(["ID_ciudadano", "fecha_date"]).reset_index(drop=True)

    df["es_practica"] = df["clase_norm"].isin(["practica", "práctica"])
    df["horas_acum_ciudadano"] = df.groupby("ID_ciudadano")["horas"].cumsum()

    total = len(df)
    _quality_log("cea_curated", {
        "total_registros":   total,
        "ciudadanos_unicos": int(df["ID_ciudadano"].nunique()),
        "nulos_fecha":       int(df["fecha_date"].isna().sum()),
        "pct_practica":      round(df["es_practica"].mean() * 100, 2),
        "horas_promedio":    round(df["horas"].mean(), 2),
    })

    logger.info("CEA Silver → %d filas, %d ciudadanos únicos", total, df["ID_ciudadano"].nunique())
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN CRC → SILVER
# ─────────────────────────────────────────────────────────────────────────────

def transform_crc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y enriquecimiento de CRC (exámenes).

    Campos derivados:
      - fecha_date          : fecha como date
      - tipo_examen_norm    : estandarizado
      - resultado_aprobado  : bool
      - examenes_aprobados_acum : conteo acumulado
    """
    logger.info("Transformando CRC…")
    df = df.copy()

    df["tipo_examen_norm"] = df["tipo_examen"].str.strip().str.lower()
    df["resultado_norm"]   = df["resultado"].str.strip().str.lower()

    df["fecha_date"] = _parse_dates(df["fecha"], "crc.fecha").dt.date
    df = df.dropna(subset=["fecha_date"])

    df = df.sort_values("_ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano", "tipo_examen_norm", "fecha_date"], keep="first")
    df = df.sort_values(["ID_ciudadano", "fecha_date"]).reset_index(drop=True)

    df["resultado_aprobado"] = df["resultado_norm"].isin(["aprobado", "aprobada"])
    df["examenes_aprobados_acum"] = df.groupby("ID_ciudadano")["resultado_aprobado"].cumsum()

    tipos_validos = {"medico", "psicologico", "coordinacion"}
    invalidos = df[~df["tipo_examen_norm"].isin(tipos_validos)]
    if len(invalidos):
        logger.warning("  %d tipos de examen no reconocidos: %s", len(invalidos),
                       invalidos["tipo_examen_norm"].unique().tolist())

    _quality_log("crc_curated", {
        "total_registros":   len(df),
        "ciudadanos_unicos": int(df["ID_ciudadano"].nunique()),
        "pct_aprobados":     round(df["resultado_aprobado"].mean() * 100, 2),
        "tipos_examen":      df["tipo_examen_norm"].value_counts().to_dict(),
        "tipos_invalidos":   len(invalidos),
    })

    logger.info("CRC Silver → %d filas", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN RUNT → SILVER
# ─────────────────────────────────────────────────────────────────────────────

def transform_runt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y enriquecimiento de RUNT (licencias).

    Campos derivados:
      - fecha_actualizacion_date : date
      - licencia_activa          : bool
      - dias_desde_actualizacion : int
    """
    logger.info("Transformando RUNT…")
    df = df.copy()

    df["estado_licencia_norm"] = df["estado_licencia"].str.strip().str.lower()

    df["fecha_actualizacion_date"] = _parse_dates(
        df["fecha_actualizacion"], "runt.fecha_actualizacion"
    ).dt.date
    df = df.dropna(subset=["fecha_actualizacion_date"])

    df = df.sort_values("_ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano", "fecha_actualizacion_date"], keep="first")
    df = df.sort_values(["ID_ciudadano", "fecha_actualizacion_date"]).reset_index(drop=True)

    df = df.sort_values("fecha_actualizacion_date", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano"], keep="first").reset_index(drop=True)

    df["licencia_activa"] = df["estado_licencia_norm"] == "activa"
    df["dias_desde_actualizacion"] = (
        pd.Timestamp.today().date() - pd.to_datetime(df["fecha_actualizacion_date"]).dt.date
    ).apply(lambda x: x.days if hasattr(x, "days") else None)

    _quality_log("runt_curated", {
        "total_registros":       len(df),
        "ciudadanos_unicos":     int(df["ID_ciudadano"].nunique()),
        "pct_licencias_activas": round(df["licencia_activa"].mean() * 100, 2),
        "estados_licencia":      df["estado_licencia_norm"].value_counts().to_dict(),
    })

    logger.info("RUNT Silver → %d filas", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR SILVER
# ─────────────────────────────────────────────────────────────────────────────

def run_silver(ingestion_results: dict) -> dict:
    """
    Recibe el dict de resultados de ingesta y produce las tablas SILVER
    (datos limpios, normalizados, deduplicados y enriquecidos).
    """
    cea_raw  = ingestion_results["cea_clases"]["clean"]
    crc_raw  = ingestion_results["crc_examenes"]["clean"]
    runt_raw = ingestion_results["runt_registros"]["clean"]

    cea_cur  = transform_cea(cea_raw)
    crc_cur  = transform_crc(crc_raw)
    runt_cur = transform_runt(runt_raw)

    # Persistir capa SILVER
    cea_cur.to_parquet(SILVER_DIR  / "cea_clases_silver.parquet",     index=False)
    crc_cur.to_parquet(SILVER_DIR  / "crc_examenes_silver.parquet",   index=False)
    runt_cur.to_parquet(SILVER_DIR / "runt_registros_silver.parquet", index=False)

    logger.info("=== Capa SILVER completa ===")

    return {
        "cea_curated":  cea_cur,
        "crc_curated":  crc_cur,
        "runt_curated": runt_cur,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETROCOMPATIBILIDAD
# ─────────────────────────────────────────────────────────────────────────────

def run_transformations(ingestion_results: dict) -> dict:
    """Retrocompatibilidad: ejecuta SILVER + GOLD completo."""
    from src.modelo.gold_model import run_gold
    silver = run_silver(ingestion_results)
    gold = run_gold(silver)
    return {**silver, **gold}


# Re-exportar funciones de gold_model para retrocompatibilidad con tests
from src.modelo.gold_model import (  # noqa: E402, F401
    build_dim_ciudadano, build_dim_fecha, build_dim_instructor,
    build_fact_cea, build_fact_crc, build_dim_runt,
)


if __name__ == "__main__":
    from src.ingestion.ingestor import run_all_ingestions
    results_ing  = run_all_ingestions()
    results_trans = run_transformations(results_ing)
