"""
Olimpia Data Pipeline - Módulo de Transformación y Limpieza
============================================================
Responsabilidad: Tomar los datos de la capa BRONZE y producir
la capa SILVER con:
  - Fechas normalizadas a ISO-8601
  - IDs normalizados
  - Deduplicación por ciudadano+día (conservar más reciente)
  - Campos derivados/enriquecidos
  - Separación en modelo dimensional (dimensiones + hechos) en capa GOLD
  - Aplicación de todas las reglas de negocio
"""

import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger("olimpia.transformer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s – %(message)s")

BASE_DIR     = Path(__file__).resolve().parents[2]
BRONZE_DIR   = BASE_DIR / "data" / "bronze"
SILVER_DIR   = BASE_DIR / "data" / "silver"
GOLD_DIR     = BASE_DIR / "data" / "gold"
LOG_DIR      = BASE_DIR / "data" / "logs"

for d in [SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_dates(series: pd.Series, col_name: str) -> pd.Series:
    """Parsea fechas flexiblemente y retorna datetime64."""
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)
    bad    = parsed.isna().sum()
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
# TRANSFORMACIÓN CEA
# ─────────────────────────────────────────────────────────────────────────────

def transform_cea(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y enriquecimiento de CEA (clases).

    Campos derivados:
      - fecha_date     : fecha como date
      - clase_norm     : clase en minúsculas estandarizado
      - instructor_norm: instructor title-case
      - es_practica    : bool
      - horas_acum     : horas acumuladas por ciudadano (hasta esa clase)
    """
    logger.info("Transformando CEA…")
    df = df.copy()

    # Normalizar texto
    df["clase_norm"]      = df["clase"].str.strip().str.lower()
    df["instructor_norm"] = df["instructor"].str.strip().str.title()

    # Normalizar fechas
    df["fecha_date"] = _parse_dates(df["fecha"], "cea.fecha").dt.date
    df = df.dropna(subset=["fecha_date"])           # Regla: fecha inválida → rechazar

    # Regla 3: múltiples registros mismo ciudadano + clase + día → conservar más reciente
    df = df.sort_values("_ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano", "clase_norm", "fecha_date"], keep="first")
    df = df.sort_values(["ID_ciudadano", "fecha_date"]).reset_index(drop=True)

    # Campo derivado: es práctica
    df["es_practica"] = df["clase_norm"].isin(["practica", "práctica"])

    # Campo derivado: horas acumuladas por ciudadano
    df["horas_acum_ciudadano"] = df.groupby("ID_ciudadano")["horas"].cumsum()

    # Métricas de calidad
    total = len(df)
    _quality_log("cea_curated", {
        "total_registros":   total,
        "ciudadanos_unicos": int(df["ID_ciudadano"].nunique()),
        "nulos_fecha":       int(df["fecha_date"].isna().sum()),
        "pct_practica":      round(df["es_practica"].mean() * 100, 2),
        "horas_promedio":    round(df["horas"].mean(), 2),
    })

    logger.info("CEA transformada → %d filas, %d ciudadanos únicos", total, df["ID_ciudadano"].nunique())
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN CRC
# ─────────────────────────────────────────────────────────────────────────────

def transform_crc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y enriquecimiento de CRC (exámenes).

    Campos derivados:
      - fecha_date       : fecha como date
      - tipo_examen_norm : estandarizado
      - resultado_bool   : True = Aprobado
      - examenes_aprobados_ciudadano : conteo acumulado aprobados
    """
    logger.info("Transformando CRC…")
    df = df.copy()

    # Normalizar texto
    df["tipo_examen_norm"] = df["tipo_examen"].str.strip().str.lower()
    df["resultado_norm"]   = df["resultado"].str.strip().str.lower()

    # Normalizar fechas
    df["fecha_date"] = _parse_dates(df["fecha"], "crc.fecha").dt.date
    df = df.dropna(subset=["fecha_date"])

    # Regla 3: mismo ciudadano + tipo_examen + día → más reciente
    df = df.sort_values("_ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano", "tipo_examen_norm", "fecha_date"], keep="first")
    df = df.sort_values(["ID_ciudadano", "fecha_date"]).reset_index(drop=True)

    # Campo derivado: resultado booleano
    df["resultado_aprobado"] = df["resultado_norm"].isin(["aprobado", "aprobada"])

    # Campo derivado: N° exámenes aprobados acumulados por ciudadano
    df["examenes_aprobados_acum"] = df.groupby("ID_ciudadano")["resultado_aprobado"].cumsum()

    # Tipos de examen esperados
    tipos_validos = {"medico", "psicologico", "coordinacion"}
    invalidos = df[~df["tipo_examen_norm"].isin(tipos_validos)]
    if len(invalidos):
        logger.warning("  %d tipos de examen no reconocidos: %s", len(invalidos),
                       invalidos["tipo_examen_norm"].unique().tolist())

    _quality_log("crc_curated", {
        "total_registros":      len(df),
        "ciudadanos_unicos":    int(df["ID_ciudadano"].nunique()),
        "pct_aprobados":        round(df["resultado_aprobado"].mean() * 100, 2),
        "tipos_examen":         df["tipo_examen_norm"].value_counts().to_dict(),
        "tipos_invalidos":      len(invalidos),
    })

    logger.info("CRC transformada → %d filas", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIÓN RUNT
# ─────────────────────────────────────────────────────────────────────────────

def transform_runt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y enriquecimiento de RUNT (licencias).

    Campos derivados:
      - fecha_actualizacion_date: date
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

    # Regla 3: mismo ciudadano + día → más reciente
    df = df.sort_values("_ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["ID_ciudadano", "fecha_actualizacion_date"], keep="first")
    df = df.sort_values(["ID_ciudadano", "fecha_actualizacion_date"]).reset_index(drop=True)

    # Para cada ciudadano, quedarse con el registro más reciente
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

    logger.info("RUNT transformada → %d filas", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# MODELO DIMENSIONAL – DIMENSIONES Y HECHOS
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_ciudadano(cea: pd.DataFrame, crc: pd.DataFrame, runt: pd.DataFrame) -> pd.DataFrame:
    """
    DIM_CIUDADANO: todos los IDs únicos vistos en cualquier fuente.
    Incluye _gold_run_id y _created_at para trazabilidad.
    """
    all_ids = pd.concat([
        cea[["ID_ciudadano"]],
        crc[["ID_ciudadano"]],
        runt[["ID_ciudadano"]],
    ]).drop_duplicates().reset_index(drop=True)
    all_ids["sk_ciudadano"] = all_ids.index + 1  # surrogate key
    all_ids["_gold_run_id"] = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    all_ids["_created_at"]  = datetime.utcnow().isoformat()
    logger.info("DIM_CIUDADANO → %d ciudadanos únicos", len(all_ids))
    return all_ids


def build_dim_fecha(cea: pd.DataFrame, crc: pd.DataFrame, runt: pd.DataFrame) -> pd.DataFrame:
    """
    DIM_FECHA: dimensión de calendario construida a partir de todas las fechas
    presentes en CEA, CRC y RUNT.
    """
    fechas = pd.concat([
        cea["fecha_date"].dropna(),
        crc["fecha_date"].dropna(),
        runt["fecha_actualizacion_date"].dropna(),
    ]).unique()
    df = pd.DataFrame({"fecha": pd.to_datetime(sorted(fechas))})
    df["sk_fecha"]    = df.index + 1
    df["anio"]        = df["fecha"].dt.year
    df["mes"]         = df["fecha"].dt.month
    df["dia"]         = df["fecha"].dt.day
    df["trimestre"]   = df["fecha"].dt.quarter
    df["nombre_mes"]  = df["fecha"].dt.strftime("%B")
    df["es_fin_semana"] = df["fecha"].dt.dayofweek >= 5
    df["fecha"]       = df["fecha"].dt.date          # guardar como date
    logger.info("DIM_FECHA → %d fechas únicas", len(df))
    return df


def build_dim_instructor(cea: pd.DataFrame) -> pd.DataFrame:
    """
    DIM_INSTRUCTOR: dimensión con los instructores únicos de CEA.
    """
    instructores = cea[["instructor_norm"]].drop_duplicates().dropna().reset_index(drop=True)
    instructores["sk_instructor"] = instructores.index + 1
    logger.info("DIM_INSTRUCTOR → %d instructores únicos", len(instructores))
    return instructores


def build_fact_cea(cea: pd.DataFrame, dim_ciudadano: pd.DataFrame,
                   dim_fecha: pd.DataFrame, dim_instructor: pd.DataFrame) -> pd.DataFrame:
    """FACT_CEA_CLASES: hecho granular de cada clase con FKs a dimensiones."""
    fact = cea.merge(dim_ciudadano[["ID_ciudadano", "sk_ciudadano"]], on="ID_ciudadano", how="left")
    # FK a dim_fecha
    fact["_fecha_merge"] = pd.to_datetime(fact["fecha_date"]).dt.date
    dim_fecha_m = dim_fecha[["fecha", "sk_fecha"]].copy()
    dim_fecha_m["fecha"] = pd.to_datetime(dim_fecha_m["fecha"]).dt.date
    fact = fact.merge(dim_fecha_m, left_on="_fecha_merge", right_on="fecha", how="left")
    fact.drop(columns=["_fecha_merge", "fecha"], inplace=True, errors="ignore")
    # FK a dim_instructor
    fact = fact.merge(dim_instructor[["instructor_norm", "sk_instructor"]], on="instructor_norm", how="left")
    # PK surrogate
    fact = fact.reset_index(drop=True)
    fact["sk_fact_cea"] = fact.index + 1
    cols = ["sk_fact_cea", "sk_ciudadano", "sk_fecha", "sk_instructor",
            "clase_norm", "horas", "es_practica",
            "horas_acum_ciudadano", "fecha_date"]
    return fact[[c for c in cols if c in fact.columns]]


def build_fact_crc(crc: pd.DataFrame, dim_ciudadano: pd.DataFrame,
                   dim_fecha: pd.DataFrame) -> pd.DataFrame:
    """FACT_CRC_EXAMENES: hecho granular de cada examen con FK a dimensiones."""
    fact = crc.merge(dim_ciudadano[["ID_ciudadano", "sk_ciudadano"]], on="ID_ciudadano", how="left")
    # FK a dim_fecha
    fact["_fecha_merge"] = pd.to_datetime(fact["fecha_date"]).dt.date
    dim_fecha_m = dim_fecha[["fecha", "sk_fecha"]].copy()
    dim_fecha_m["fecha"] = pd.to_datetime(dim_fecha_m["fecha"]).dt.date
    fact = fact.merge(dim_fecha_m, left_on="_fecha_merge", right_on="fecha", how="left")
    fact.drop(columns=["_fecha_merge", "fecha"], inplace=True, errors="ignore")
    # PK surrogate
    fact = fact.reset_index(drop=True)
    fact["sk_fact_crc"] = fact.index + 1
    cols = ["sk_fact_crc", "sk_ciudadano", "sk_fecha",
            "tipo_examen_norm", "resultado_aprobado",
            "examenes_aprobados_acum", "fecha_date"]
    return fact[[c for c in cols if c in fact.columns]]


def build_dim_runt(runt: pd.DataFrame, dim_ciudadano: pd.DataFrame,
                   dim_fecha: pd.DataFrame) -> pd.DataFrame:
    """DIM_RUNT: estado más reciente de licencia por ciudadano con FK a dimensiones."""
    dim = runt.merge(dim_ciudadano[["ID_ciudadano", "sk_ciudadano"]], on="ID_ciudadano", how="left")
    # FK a dim_fecha
    dim["_fecha_merge"] = pd.to_datetime(dim["fecha_actualizacion_date"]).dt.date
    dim_fecha_m = dim_fecha[["fecha", "sk_fecha"]].copy()
    dim_fecha_m["fecha"] = pd.to_datetime(dim_fecha_m["fecha"]).dt.date
    dim = dim.merge(dim_fecha_m, left_on="_fecha_merge", right_on="fecha", how="left")
    dim.drop(columns=["_fecha_merge", "fecha"], inplace=True, errors="ignore")
    # PK surrogate
    dim = dim.reset_index(drop=True)
    dim["sk_runt"] = dim.index + 1
    cols = ["sk_runt", "sk_ciudadano", "sk_fecha",
            "estado_licencia_norm", "licencia_activa",
            "dias_desde_actualizacion"]
    return dim[[c for c in cols if c in dim.columns]]


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR DE TRANSFORMACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def run_transformations(ingestion_results: dict) -> dict:
    """
    Recibe el dict de resultados de ingesta y produce todas las
    tablas curadas + modelo dimensional.
    """
    cea_raw  = ingestion_results["cea_clases"]["clean"]
    crc_raw  = ingestion_results["crc_examenes"]["clean"]
    runt_raw = ingestion_results["runt_registros"]["clean"]

    # Transformar
    cea_cur  = transform_cea(cea_raw)
    crc_cur  = transform_crc(crc_raw)
    runt_cur = transform_runt(runt_raw)

    # Guardar capa SILVER (datos limpios y normalizados)
    cea_cur.to_parquet(SILVER_DIR  / "cea_clases_silver.parquet",   index=False)
    crc_cur.to_parquet(SILVER_DIR  / "crc_examenes_silver.parquet", index=False)
    runt_cur.to_parquet(SILVER_DIR / "runt_registros_silver.parquet", index=False)

    # Modelo dimensional
    dim_ciudadano  = build_dim_ciudadano(cea_cur, crc_cur, runt_cur)
    dim_fecha      = build_dim_fecha(cea_cur, crc_cur, runt_cur)
    dim_instructor = build_dim_instructor(cea_cur)
    fact_cea       = build_fact_cea(cea_cur, dim_ciudadano, dim_fecha, dim_instructor)
    fact_crc       = build_fact_crc(crc_cur, dim_ciudadano, dim_fecha)
    dim_runt       = build_dim_runt(runt_cur, dim_ciudadano, dim_fecha)

    # Guardar capa GOLD (modelo dimensional – esquema estrella)
    dim_ciudadano.to_parquet(GOLD_DIR  / "dim_ciudadano.parquet",     index=False)
    dim_fecha.to_parquet(GOLD_DIR      / "dim_fecha.parquet",          index=False)
    dim_instructor.to_parquet(GOLD_DIR / "dim_instructor.parquet",     index=False)
    fact_cea.to_parquet(GOLD_DIR       / "fact_cea_clases.parquet",    index=False)
    fact_crc.to_parquet(GOLD_DIR       / "fact_crc_examenes.parquet",  index=False)
    dim_runt.to_parquet(GOLD_DIR       / "dim_runt.parquet",           index=False)

    logger.info("=== Transformación completa. Capas SILVER y GOLD generadas ===")

    return {
        "cea_curated":     cea_cur,
        "crc_curated":     crc_cur,
        "runt_curated":    runt_cur,
        "dim_ciudadano":   dim_ciudadano,
        "dim_fecha":       dim_fecha,
        "dim_instructor":  dim_instructor,
        "fact_cea":        fact_cea,
        "fact_crc":        fact_crc,
        "dim_runt":        dim_runt,
    }


if __name__ == "__main__":
    from src.ingestion.ingestor import run_all_ingestions
    results_ing  = run_all_ingestions()
    results_trans = run_transformations(results_ing)
