"""
Olimpia Data Pipeline - Capa GOLD (Modelo Dimensional)
=======================================================
Responsabilidad: Tomar los datos de la capa SILVER y construir
el modelo estrella (star schema) para consumo analítico:
  - Dimensiones: dim_ciudadano, dim_fecha, dim_instructor, dim_runt
  - Hechos: fact_cea_clases, fact_crc_examenes
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("olimpia.gold")

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = BASE_DIR / "data" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# DIMENSIONES
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
    all_ids["sk_ciudadano"] = all_ids.index + 1
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
    df["sk_fecha"]      = df.index + 1
    df["anio"]          = df["fecha"].dt.year
    df["mes"]           = df["fecha"].dt.month
    df["dia"]           = df["fecha"].dt.day
    df["trimestre"]     = df["fecha"].dt.quarter
    df["nombre_mes"]    = df["fecha"].dt.strftime("%B")
    df["es_fin_semana"] = df["fecha"].dt.dayofweek >= 5
    df["fecha"]         = df["fecha"].dt.date
    logger.info("DIM_FECHA → %d fechas únicas", len(df))
    return df


def build_dim_instructor(cea: pd.DataFrame) -> pd.DataFrame:
    """DIM_INSTRUCTOR: dimensión con los instructores únicos de CEA."""
    instructores = cea[["instructor_norm"]].drop_duplicates().dropna().reset_index(drop=True)
    instructores["sk_instructor"] = instructores.index + 1
    logger.info("DIM_INSTRUCTOR → %d instructores únicos", len(instructores))
    return instructores


# ─────────────────────────────────────────────────────────────────────────────
# HECHOS
# ─────────────────────────────────────────────────────────────────────────────

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
# ORQUESTADOR GOLD
# ─────────────────────────────────────────────────────────────────────────────

def run_gold(silver_results: dict) -> dict:
    """
    Recibe el dict de resultados SILVER y construye el modelo estrella GOLD
    (dimensiones + hechos) listo para Power BI / Fabric.
    """
    cea_cur  = silver_results["cea_curated"]
    crc_cur  = silver_results["crc_curated"]
    runt_cur = silver_results["runt_curated"]

    # Dimensiones
    dim_ciudadano  = build_dim_ciudadano(cea_cur, crc_cur, runt_cur)
    dim_fecha      = build_dim_fecha(cea_cur, crc_cur, runt_cur)
    dim_instructor = build_dim_instructor(cea_cur)

    # Hechos
    fact_cea = build_fact_cea(cea_cur, dim_ciudadano, dim_fecha, dim_instructor)
    fact_crc = build_fact_crc(crc_cur, dim_ciudadano, dim_fecha)
    dim_runt = build_dim_runt(runt_cur, dim_ciudadano, dim_fecha)

    # Persistir capa GOLD
    dim_ciudadano.to_parquet(GOLD_DIR  / "dim_ciudadano.parquet",    index=False)
    dim_fecha.to_parquet(GOLD_DIR      / "dim_fecha.parquet",         index=False)
    dim_instructor.to_parquet(GOLD_DIR / "dim_instructor.parquet",    index=False)
    fact_cea.to_parquet(GOLD_DIR       / "fact_cea_clases.parquet",   index=False)
    fact_crc.to_parquet(GOLD_DIR       / "fact_crc_examenes.parquet", index=False)
    dim_runt.to_parquet(GOLD_DIR       / "dim_runt.parquet",          index=False)

    logger.info("=== Capa GOLD completa. Modelo estrella generado ===")

    return {
        "dim_ciudadano":  dim_ciudadano,
        "dim_fecha":      dim_fecha,
        "dim_instructor": dim_instructor,
        "fact_cea":       fact_cea,
        "fact_crc":       fact_crc,
        "dim_runt":       dim_runt,
    }
