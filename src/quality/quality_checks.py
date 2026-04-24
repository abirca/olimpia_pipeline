"""
Olimpia Data Pipeline - Módulo de Calidad y Detección de Fraude
===============================================================
Responsabilidad:
  1. Generar reporte completo de calidad de datos con métricas.
  2. Detectar inconsistencias CRC/CEA vs RUNT.
  3. Sistema de detección temprana de fraude.
  4. Propuesta de alertas automáticas.
"""

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("olimpia.quality")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s – %(message)s")

BASE_DIR    = Path(__file__).resolve().parents[2]
GOLD_DIR    = BASE_DIR / "data" / "gold"
LOG_DIR     = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# 1. REPORTE DE CALIDAD DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

def calidad_dataframe(df: pd.DataFrame, nombre: str) -> dict:
    """Métricas de calidad para un DataFrame."""
    total = len(df)
    if total == 0:
        return {"fuente": nombre, "total_filas": 0}

    metricas = {
        "fuente":              nombre,
        "total_filas":         total,
        "filas_duplicadas":    int(df.duplicated().sum()),
        "pct_duplicadas":      round(df.duplicated().sum() / total * 100, 2),
        "nulos_por_columna":   df.isnull().sum().to_dict(),
        "pct_nulos_por_col":   (df.isnull().mean() * 100).round(2).to_dict(),
        "evaluado_en":         datetime.utcnow().isoformat(),
    }

    # Métricas específicas por tipo de columna
    for col in df.select_dtypes(include=["number"]).columns:
        metricas[f"stats_{col}"] = df[col].describe().round(2).to_dict()

    log_path = LOG_DIR / f"calidad_{nombre}.json"
    with open(log_path, "w", encoding="utf-8") as fp:
        json.dump(metricas, fp, indent=2, default=str)

    logger.info("[Calidad] %s → %d filas | %.1f%% nulos promedio",
                nombre, total,
                df.isnull().mean().mean() * 100)
    return metricas


def generar_reporte_calidad(transformed: dict) -> dict:
    """Genera el reporte unificado de calidad para todas las fuentes."""
    reporte = {}
    for nombre, df in [
        ("cea_curated",   transformed.get("cea_curated")),
        ("crc_curated",   transformed.get("crc_curated")),
        ("runt_curated",  transformed.get("runt_curated")),
        ("fact_cea",      transformed.get("fact_cea")),
        ("fact_crc",      transformed.get("fact_crc")),
    ]:
        if df is not None:
            reporte[nombre] = calidad_dataframe(df, nombre)

    # Guardar reporte consolidado
    rpt_path = LOG_DIR / "reporte_calidad_consolidado.json"
    with open(rpt_path, "w", encoding="utf-8") as fp:
        json.dump(reporte, fp, indent=2, default=str)
    logger.info("Reporte consolidado → %s", rpt_path)
    return reporte


# ─────────────────────────────────────────────────────────────────────────────
# 2. TABLA MAESTRA DE CUMPLIMIENTO
# ─────────────────────────────────────────────────────────────────────────────

def build_tabla_cumplimiento(transformed: dict) -> pd.DataFrame:
    """
    Tabla final que responde:
      - ¿Qué % de ciudadanos completó satisfactoriamente CRC y CEA?
      - ¿Cuántos presentan inconsistencias entre CRC/CEA y RUNT?
    
    Reglas de cumplimiento:
      CRC completo     = tiene los 3 tipos de examen (medico, psicologico, coordinacion)
                         y todos aprobados.
      CEA completo     = tiene al menos una clase teórica Y una práctica.
      RUNT consistente = licencia activa si CRC+CEA completos, o licencia no activa
                         si proceso incompleto.
    """
    fact_crc     = transformed["fact_crc"]
    fact_cea     = transformed["fact_cea"]
    dim_runt     = transformed["dim_runt"]
    dim_ciudadano= transformed["dim_ciudadano"]

    # Añadir ID_ciudadano a facts vía dim_ciudadano para agrupar
    _map_sk = dim_ciudadano[["sk_ciudadano", "ID_ciudadano"]]
    fact_crc_ext = fact_crc.merge(_map_sk, on="sk_ciudadano", how="left")
    fact_cea_ext = fact_cea.merge(_map_sk, on="sk_ciudadano", how="left")

    # ── Resumen CRC por ciudadano ─────────────────────────────────────────────
    TIPOS_REQUERIDOS = {"medico", "psicologico", "coordinacion"}

    crc_summary = fact_crc_ext.groupby("ID_ciudadano").apply(
        lambda g: pd.Series({
            "tipos_examen_realizados": set(g["tipo_examen_norm"].dropna().tolist()),
            "total_examenes":          len(g),
            "examenes_aprobados":      int(g["resultado_aprobado"].sum()),
            "examenes_reprobados":     int((~g["resultado_aprobado"]).sum()),
            "ultimo_examen_fecha":     g["fecha_date"].max(),
        })
    ).reset_index()

    crc_summary["crc_tipos_completos"] = crc_summary["tipos_examen_realizados"].apply(
        lambda s: TIPOS_REQUERIDOS.issubset(s)
    )
    crc_summary["crc_todos_aprobados"] = (
        crc_summary["examenes_reprobados"] == 0
    ) & (crc_summary["examenes_aprobados"] > 0)
    crc_summary["crc_completo"] = (
        crc_summary["crc_tipos_completos"] & crc_summary["crc_todos_aprobados"]
    )

    # ── Resumen CEA por ciudadano ─────────────────────────────────────────────
    cea_summary = fact_cea_ext.groupby("ID_ciudadano").apply(
        lambda g: pd.Series({
            "clases_teoricas":    int((g["clase_norm"].str.contains("teoric", na=False)).sum()),
            "clases_practicas":   int(g["es_practica"].sum()),
            "total_horas":        int(g["horas"].sum()),
            "ultima_clase_fecha": g["fecha_date"].max(),
        })
    ).reset_index()

    cea_summary["cea_completo"] = (
        (cea_summary["clases_teoricas"] >= 1) &
        (cea_summary["clases_practicas"] >= 1)
    )

    # ── JOIN general ──────────────────────────────────────────────────────────
    df = dim_ciudadano[["sk_ciudadano", "ID_ciudadano"]].merge(
        crc_summary, on="ID_ciudadano", how="left"
    )
    df = df.merge(cea_summary, on="ID_ciudadano", how="left")
    df = df.merge(
        dim_runt[["sk_ciudadano", "estado_licencia_norm", "licencia_activa",
                  "dias_desde_actualizacion"]].rename(
            columns={"sk_ciudadano": "_sk_runt"}),
        left_on="sk_ciudadano", right_on="_sk_runt", how="left"
    )
    df.drop(columns=["_sk_runt"], inplace=True, errors="ignore")

    # ── Campos de cumplimiento ────────────────────────────────────────────────
    df["crc_completo"]  = df["crc_completo"].fillna(False)
    df["cea_completo"]  = df["cea_completo"].fillna(False)
    df["proceso_completo"] = df["crc_completo"] & df["cea_completo"]

    # Inconsistencia: proceso completo pero licencia no activa, o viceversa
    df["inconsistencia_runt"] = (
        (df["proceso_completo"] & ~df["licencia_activa"].fillna(False)) |
        (~df["proceso_completo"] & df["licencia_activa"].fillna(False))
    )

    # Nivel de riesgo
    conditions = [
        df["inconsistencia_runt"] & df["proceso_completo"],   # Completo pero sin licencia activa
        df["inconsistencia_runt"] & ~df["proceso_completo"],  # Incompleto pero con licencia activa
        ~df["proceso_completo"],                               # Proceso incompleto
    ]
    choices = ["ALTO", "CRITICO", "MEDIO"]
    df["nivel_riesgo"] = np.select(conditions, choices, default="BAJO")

    # Guardar tabla maestra en capa GOLD
    out = GOLD_DIR / "tabla_cumplimiento.parquet"
    df.to_parquet(out, index=False)
    logger.info("tabla_cumplimiento → %s (%d ciudadanos)", out, len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. DETECCIÓN DE FRAUDE
# ─────────────────────────────────────────────────────────────────────────────

def detectar_anomalias(transformed: dict, cumplimiento: pd.DataFrame) -> pd.DataFrame:
    """
    Señales de fraude potencial:
      F1 – Instructor con >50% tasa de aprobación en CRC en misma fecha (colusión)
      F2 – Ciudadano con proceso CEA completado en < 3 días (demasiado rápido)
      F3 – Licencia RUNT activa sin ningún examen CRC registrado
      F4 – Mismo ciudadano con resultados contradictorios mismo día (mismo examen, diferente resultado)
      F5 – Instructor que aparece en >10 registros en un solo día
    """
    fact_crc = transformed["fact_crc"]
    fact_cea = transformed["fact_cea"]
    dim_ciudadano = transformed["dim_ciudadano"]
    dim_instructor = transformed.get("dim_instructor")
    alertas  = []

    # Mapa sk→ID para facts
    _map_sk = dim_ciudadano[["sk_ciudadano", "ID_ciudadano"]]
    fact_crc_ext = fact_crc.merge(_map_sk, on="sk_ciudadano", how="left")
    fact_cea_ext = fact_cea.merge(_map_sk, on="sk_ciudadano", how="left")

    # ── F2: CEA completado en menos de 3 días ─────────────────────────────────
    cea_span = fact_cea_ext.groupby("ID_ciudadano")["fecha_date"].agg(["min", "max"])
    cea_span["dias_proceso_cea"] = (
        pd.to_datetime(cea_span["max"]) - pd.to_datetime(cea_span["min"])
    ).dt.days
    rapidos = cea_span[cea_span["dias_proceso_cea"] < 3].reset_index()
    for _, row in rapidos.iterrows():
        alertas.append({
            "tipo_alerta":  "F2_CEA_MUY_RAPIDO",
            "ID_ciudadano": row["ID_ciudadano"],
            "detalle":      f"CEA completado en {row['dias_proceso_cea']} días",
            "severidad":    "ALTA",
        })

    # ── F3: RUNT activo sin ningún CRC ───────────────────────────────────────
    ids_con_crc   = set(fact_crc_ext["ID_ciudadano"].dropna().unique())
    f3_candidatos = cumplimiento[
        (cumplimiento["licencia_activa"] == True) &
        (~cumplimiento["ID_ciudadano"].isin(ids_con_crc))
    ]
    for _, row in f3_candidatos.iterrows():
        alertas.append({
            "tipo_alerta":  "F3_LICENCIA_SIN_CRC",
            "ID_ciudadano": row["ID_ciudadano"],
            "detalle":      "Licencia RUNT activa pero sin ningún examen CRC registrado",
            "severidad":    "CRITICA",
        })

    # ── F4: Mismo examen, mismo ciudadano, mismo día, resultados distintos ────
    crc_dups = fact_crc_ext.groupby(
        ["ID_ciudadano", "tipo_examen_norm", "fecha_date"]
    )["resultado_aprobado"].nunique()
    contradictorios = crc_dups[crc_dups > 1].reset_index()
    for _, row in contradictorios.iterrows():
        alertas.append({
            "tipo_alerta":  "F4_RESULTADO_CONTRADICTORIO",
            "ID_ciudadano": row["ID_ciudadano"],
            "detalle":      f"Examen {row['tipo_examen_norm']} con resultados contradictorios el {row['fecha_date']}",
            "severidad":    "CRITICA",
        })

    # ── F5: Instructor con >10 clases en un día ───────────────────────────────
    # Recuperar instructor_norm via dim_instructor
    if dim_instructor is not None and "sk_instructor" in fact_cea.columns:
        fact_cea_instr = fact_cea.merge(
            dim_instructor[["sk_instructor", "instructor_norm"]],
            on="sk_instructor", how="left"
        )
        instructor_carga = fact_cea_instr.groupby(
            ["instructor_norm", "fecha_date"]
        ).size().reset_index(name="clases_dia")
        sobrecargados = instructor_carga[instructor_carga["clases_dia"] > 10]
        for _, row in sobrecargados.iterrows():
            alertas.append({
                "tipo_alerta":  "F5_INSTRUCTOR_SOBRECARGADO",
                "ID_ciudadano": None,
                "detalle":      f"Instructor {row['instructor_norm']} con {row['clases_dia']} clases el {row['fecha_date']}",
                "severidad":    "MEDIA",
            })

    df_alertas = pd.DataFrame(alertas)
    if not df_alertas.empty:
        df_alertas["detectado_en"] = datetime.utcnow().isoformat()
        df_alertas = df_alertas.reset_index(drop=True)
        df_alertas["sk_alerta"] = df_alertas.index + 1
        # Reordenar con PK primero
        cols_order = ["sk_alerta", "ID_ciudadano", "tipo_alerta", "detalle",
                      "severidad", "detectado_en"]
        df_alertas = df_alertas[[c for c in cols_order if c in df_alertas.columns]]
        df_alertas.to_parquet(GOLD_DIR / "alertas_fraude.parquet", index=False)
        logger.warning("🚨 %d alertas de fraude detectadas", len(df_alertas))
    else:
        logger.info("✅ No se detectaron anomalías de fraude")

    return df_alertas


# ─────────────────────────────────────────────────────────────────────────────
# 4. KPIs RESUMEN
# ─────────────────────────────────────────────────────────────────────────────

def calcular_kpis(cumplimiento: pd.DataFrame, alertas: pd.DataFrame) -> dict:
    """KPIs para el dashboard de cumplimiento."""
    total = len(cumplimiento)
    kpis = {
        "total_ciudadanos":              total,
        "pct_proceso_completo":          round(cumplimiento["proceso_completo"].mean() * 100, 2),
        "pct_crc_completo":              round(cumplimiento["crc_completo"].mean() * 100, 2),
        "pct_cea_completo":              round(cumplimiento["cea_completo"].mean() * 100, 2),
        "ciudadanos_inconsistencia_runt":int(cumplimiento["inconsistencia_runt"].sum()),
        "pct_inconsistencia_runt":       round(cumplimiento["inconsistencia_runt"].mean() * 100, 2),
        "riesgo_critico":                int((cumplimiento["nivel_riesgo"] == "CRITICO").sum()),
        "riesgo_alto":                   int((cumplimiento["nivel_riesgo"] == "ALTO").sum()),
        "riesgo_medio":                  int((cumplimiento["nivel_riesgo"] == "MEDIO").sum()),
        "riesgo_bajo":                   int((cumplimiento["nivel_riesgo"] == "BAJO").sum()),
        "total_alertas_fraude":          len(alertas) if alertas is not None else 0,
        "alertas_criticas":              int((alertas["severidad"] == "CRITICA").sum()) if alertas is not None and not alertas.empty else 0,
        "calculado_en":                  datetime.utcnow().isoformat(),
    }

    kpi_path = LOG_DIR / "kpis.json"
    with open(kpi_path, "w", encoding="utf-8") as fp:
        json.dump(kpis, fp, indent=2)

    logger.info("KPIs calculados → %s", kpi_path)
    return kpis


def run_quality_checks(transformed: dict) -> dict:
    """Punto de entrada del módulo de calidad."""
    reporte_cal  = generar_reporte_calidad(transformed)
    cumplimiento = build_tabla_cumplimiento(transformed)
    alertas      = detectar_anomalias(transformed, cumplimiento)
    kpis         = calcular_kpis(cumplimiento, alertas)

    return {
        "reporte_calidad":   reporte_cal,
        "tabla_cumplimiento": cumplimiento,
        "alertas_fraude":    alertas,
        "kpis":              kpis,
    }


if __name__ == "__main__":
    from src.ingestion.ingestor      import run_all_ingestions
    from src.transformation.transformer import run_transformations

    ing   = run_all_ingestions()
    trans = run_transformations(ing)
    run_quality_checks(trans)
