"""
Olimpia Data Pipeline - Orquestador Principal
=============================================
Ejecuta el pipeline completo end-to-end (Arquitectura Medallón):
  1. Ingesta (BRONZE)
  2. Transformación y limpieza (SILVER + GOLD)
  3. Calidad, cumplimiento y fraude
  4. Exportar resultados finales

Uso:
  python pipeline.py
  python pipeline.py --source-dir /ruta/a/csvs
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("olimpia.pipeline")

BASE_DIR    = Path(__file__).resolve().parent
GOLD_DIR    = BASE_DIR / "data" / "gold"

sys.path.insert(0, str(BASE_DIR))

from src.ingestion.ingestor           import run_all_ingestions, ingest_source
from src.transformation.transformer   import run_transformations
from src.quality.quality_checks       import run_quality_checks
from src.exposure.exporter            import run_exposure


def run_pipeline(source_dir: Path = None) -> dict:
    started = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("OLIMPIA DATA PIPELINE – Inicio: %s", started.isoformat())
    logger.info("=" * 60)

    # ── ETAPA 1: INGESTA (BRONZE) ──────────────────────────────────────────
    logger.info("\n─── ETAPA 1: INGESTA → BRONZE ───────────────────────────────────")
    if source_dir:
        from src.ingestion.ingestor import ingest_source
        ingestion = {}
        for s in ["cea_clases", "crc_examenes", "runt_registros"]:
            fpath = source_dir / f"{s}.csv"
            clean, errors = ingest_source(s, fpath)
            ingestion[s] = {"clean": clean, "errors": errors}
    else:
        ingestion = run_all_ingestions()

    total_ingestados = sum(len(v["clean"]) for v in ingestion.values())
    total_errores    = sum(len(v["errors"]) for v in ingestion.values())
    logger.info("Total ingestados: %d | Errores: %d", total_ingestados, total_errores)

    # ── ETAPA 2: TRANSFORMACIÓN (SILVER + GOLD) ─────────────────────────────
    logger.info("\n─── ETAPA 2: TRANSFORMACIÓN → SILVER + GOLD ───────────────")
    transformed = run_transformations(ingestion)

    # ── ETAPA 3: CALIDAD + CUMPLIMIENTO + FRAUDE ──────────────────────────────
    logger.info("\n─── ETAPA 3: CALIDAD, CUMPLIMIENTO Y FRAUDE ─────────────────")
    quality = run_quality_checks(transformed)
    # ── ETAPA 4: EXPOSICIÓN (Dataset final CSV) ─────────────────────
    logger.info("\n─── ETAPA 4: EXPOSICIÓN → Dataset Final ───────────────────")
    exposure = run_exposure(quality)
    # ── RESUMEN FINAL ─────────────────────────────────────────────────────────
    ended    = datetime.utcnow()
    duration = (ended - started).total_seconds()

    kpis = quality["kpis"]
    alertas = quality["alertas_fraude"]

    summary = {
        "pipeline_version":        "1.0.0",
        "started_at":              started.isoformat(),
        "finished_at":             ended.isoformat(),
        "duration_seconds":        round(duration, 2),
        "total_rows_ingested":     total_ingestados,
        "total_rows_with_errors":  total_errores,
        "kpis": kpis,
    }

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETADO en %.1fs", duration)
    logger.info("=" * 60)
    logger.info("📊 Ciudadanos totales:         %d", kpis.get("total_ciudadanos", 0))
    logger.info("✅ Proceso completo:           %.1f%%", kpis.get("pct_proceso_completo", 0))
    logger.info("⚠️  Inconsistencias RUNT:      %d (%.1f%%)",
                kpis.get("ciudadanos_inconsistencia_runt", 0),
                kpis.get("pct_inconsistencia_runt", 0))
    logger.info("🚨 Alertas fraude:             %d (críticas: %d)",
                kpis.get("total_alertas_fraude", 0),
                kpis.get("alertas_criticas", 0))
    logger.info("=" * 60)

    return {
        "ingestion":    ingestion,
        "transformed":  transformed,
        "quality":      quality,
        "exposure":     exposure,
        "summary":      summary,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olimpia Data Pipeline")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Directorio con los CSV de entrada (por defecto: data/raw/)",
    )
    args = parser.parse_args()
    run_pipeline(source_dir=args.source_dir)
