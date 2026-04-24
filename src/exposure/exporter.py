"""
Olimpia Data Pipeline - Módulo de Exposición
=============================================
Responsabilidad:
  - Exportar el dataset final (tabla_cumplimiento) a CSV para consumo.
  - Generar resumen ejecutivo para dashboards.
  - Punto de integración con Power BI / Fabric / API REST.
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger("olimpia.exposure")

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_DIR = BASE_DIR / "data" / "gold"


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
        "ID_ciudadano", "crc_completo", "cea_completo",
        "proceso_completo", "inconsistencia_runt", "nivel_riesgo",
        "licencia_activa", "estado_licencia_norm",
    ]
    cols_disponibles = [c for c in cols_export if c in cumplimiento.columns]
    df_export = cumplimiento[cols_disponibles].copy()

    # Convertir booleanos a Sí/No para legibilidad
    bool_cols = ["crc_completo", "cea_completo", "proceso_completo",
                 "inconsistencia_runt", "licencia_activa"]
    for col in bool_cols:
        if col in df_export.columns:
            df_export[col] = df_export[col].map({True: "Sí", False: "No"})

    csv_path = GOLD_DIR / "dataset_final_cumplimiento.csv"
    df_export.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info("Dataset final exportado → %s (%d filas)", csv_path, len(df_export))

    # ── 2. Exportar alertas a CSV ─────────────────────────────────────────────
    if alertas is not None and not alertas.empty:
        alertas_csv = GOLD_DIR / "alertas_fraude.csv"
        alertas.to_csv(alertas_csv, index=False, encoding="utf-8")
        logger.info("Alertas fraude CSV → %s (%d alertas)", alertas_csv, len(alertas))

    # ── 3. Resumen ejecutivo para dashboard ───────────────────────────────────
    logger.info("Dataset y alertas exportados correctamente")

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
