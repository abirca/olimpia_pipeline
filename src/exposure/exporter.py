"""
Olimpia Data Pipeline - Módulo de Exposición
=============================================
Responsabilidad:
  - Exportar el dataset final (tabla_cumplimiento) a CSV para consumo.
  - Generar resumen ejecutivo para dashboards.
  - Punto de integración con Power BI / Fabric / API REST.
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger("olimpia.exposure")

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = BASE_DIR / "data" / "gold"
LOG_DIR  = BASE_DIR / "data" / "logs"


def export_dataset_final(cumplimiento: pd.DataFrame, alertas: pd.DataFrame, kpis: dict) -> Path:
    """
    Exporta el dataset final de cumplimiento a CSV listo para consumo
    por dashboards, reportes o API.

    Incluye:
      - Tabla de cumplimiento con nivel de riesgo por ciudadano.
      - Resumen de alertas de fraude consolidado.
      - KPIs clave del pipeline.
    """
    # ── 1. Exportar tabla cumplimiento a CSV ──────────────────────────────────
    cols_export = [
        "sk_ciudadano", "ID_ciudadano",
        # CRC
        "examen_medico", "examen_psicologico", "examen_coordinacion",
        "total_examenes", "examenes_aprobados", "examenes_reprobados",
        "ultimo_examen_fecha", "crc_completo",
        # CEA
        "clases_teoricas", "clases_practicas", "total_horas_cea",
        "ultima_clase_fecha", "cea_completo",
        # RUNT
        "estado_licencia_norm", "licencia_activa",
        "dias_desde_actualizacion",
        # Resultados
        "proceso_completo", "inconsistencia_runt", "nivel_riesgo",
    ]

    df_export = cumplimiento.copy()

    cols_disponibles = [c for c in cols_export if c in df_export.columns]
    df_export = df_export[cols_disponibles]

    csv_path = GOLD_DIR / "dataset_final_cumplimiento.csv"
    df_export.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info("Dataset final exportado → %s (%d filas)", csv_path, len(df_export))

    # ── 2. Exportar alertas a CSV ─────────────────────────────────────────────
    if alertas is not None and not alertas.empty:
        alertas_csv = GOLD_DIR / "alertas_fraude.csv"
        alertas.to_csv(alertas_csv, index=False, encoding="utf-8")
        logger.info("Alertas fraude CSV → %s (%d alertas)", alertas_csv, len(alertas))

    # ── 3. Resumen ejecutivo para dashboard ───────────────────────────────────
    resumen = {
        "generado_en":            datetime.utcnow().isoformat(),
        "total_ciudadanos":       kpis.get("total_ciudadanos", 0),
        "pct_proceso_completo":   kpis.get("pct_proceso_completo", 0),
        "pct_crc_completo":       kpis.get("pct_crc_completo", 0),
        "pct_cea_completo":       kpis.get("pct_cea_completo", 0),
        "inconsistencias_runt":   kpis.get("ciudadanos_inconsistencia_runt", 0),
        "pct_inconsistencia_runt": kpis.get("pct_inconsistencia_runt", 0),
        "alertas_fraude_total":   kpis.get("total_alertas_fraude", 0),
        "alertas_criticas":       kpis.get("alertas_criticas", 0),
        "distribucion_riesgo": {
            "BAJO":    kpis.get("riesgo_bajo", 0),
            "MEDIO":   kpis.get("riesgo_medio", 0),
            "ALTO":    kpis.get("riesgo_alto", 0),
            "CRITICO": kpis.get("riesgo_critico", 0),
        },
        "dataset_path":           str(csv_path),
    }

    resumen_path = LOG_DIR / "resumen_ejecutivo.json"
    with open(resumen_path, "w", encoding="utf-8") as fp:
        json.dump(resumen, fp, indent=2, default=str)
    logger.info("Resumen ejecutivo → %s", resumen_path)

    return csv_path


def run_exposure(quality_results: dict) -> dict:
    """Punto de entrada del módulo de exposición."""
    cumplimiento = quality_results["tabla_cumplimiento"]
    alertas      = quality_results["alertas_fraude"]
    kpis         = quality_results["kpis"]

    csv_path = export_dataset_final(cumplimiento, alertas, kpis)

    logger.info("=== Exposición completa. Dataset final listo para consumo ===")

    return {
        "dataset_path": csv_path,
        "kpis":         kpis,
    }
