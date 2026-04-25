"""
Olimpia Data Pipeline - Módulo de Orquestación
================================================
Coordina la ejecución secuencial de las capas del medallón:
  1. Ingesta   (BRONZE)  → src.ingestion
  2. Silver    (SILVER)  → src.transformation
  3. Modelo    (GOLD)    → src.modelo

Centraliza la lógica de orquestación para que pipeline.py
solo se encargue de logging, resumen y CLI.
"""

import logging

from src.ingestion.ingestor            import run_all_ingestions, ingest_source
from src.transformation.transformer    import run_silver
from src.modelo.gold_model             import run_gold

logger = logging.getLogger("olimpia.orquestacion")


def run_ingestion(source_dir=None) -> dict:
    """Ejecuta la ingesta de todas las fuentes CSV → BRONZE."""
    if source_dir:
        from pathlib import Path
        ingestion = {}
        for s in ["cea_clases", "crc_examenes", "runt_registros"]:
            fpath = Path(source_dir) / f"{s}.csv"
            clean, errors = ingest_source(s, fpath)
            ingestion[s] = {"clean": clean, "errors": errors}
        return ingestion
    return run_all_ingestions()


def run_etl(ingestion_results: dict) -> dict:
    """
    Ejecuta las etapas de transformación del medallón:
      BRONZE → SILVER (limpieza) → GOLD (modelo estrella)

    Retorna dict con todas las tablas silver + gold.
    """
    # ── SILVER ──
    silver = run_silver(ingestion_results)

    # ── GOLD ──
    gold = run_gold(silver)

    logger.info("=== Orquestación ETL completa: SILVER + GOLD generados ===")
    return {**silver, **gold}
