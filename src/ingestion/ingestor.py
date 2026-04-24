"""
Olimpia Data Pipeline - Módulo de Ingesta
==========================================
Responsabilidad: Cargar archivos CSV crudos hacia la capa BRONZE,
registrar trazabilidad completa de cada ingesta y detectar
registros corruptos/duplicados antes de pasar a curación (Silver).
"""

import pandas as pd
import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Tuple

# ── Configuración de logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("olimpia.ingestor")

# ── Rutas del proyecto (Medallion: Bronze / Silver / Gold) ─────────────────────
BASE_DIR    = Path(__file__).resolve().parents[2]
BRONZE_DIR  = BASE_DIR / "data" / "bronze"
LOG_DIR     = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ── Esquemas esperados por fuente ─────────────────────────────────────────────
SCHEMAS = {
    "cea_clases": {
        "required_cols": ["ID_ciudadano", "clase", "horas", "instructor", "fecha"],
        "dtype": {
            "ID_ciudadano": "Int64",
            "clase":        str,
            "horas":        "Int64",
            "instructor":   str,
            "fecha":        str,
        },
    },
    "crc_examenes": {
        "required_cols": ["ID_ciudadano", "tipo_examen", "resultado", "fecha"],
        "dtype": {
            "ID_ciudadano": "Int64",
            "tipo_examen":  str,
            "resultado":    str,
            "fecha":        str,
        },
    },
    "runt_registros": {
        "required_cols": ["ID_ciudadano", "estado_licencia", "fecha_actualizacion"],
        "dtype": {
            "ID_ciudadano":        "Int64",
            "estado_licencia":     str,
            "fecha_actualizacion": str,
        },
    },
}


# ── Funciones auxiliares ──────────────────────────────────────────────────────

def _file_hash(path: Path) -> str:
    """SHA-256 del archivo para auditoría de integridad."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _audit_log(source: str, audit: dict) -> None:
    """Escribe el registro de auditoría en JSON Lines."""
    log_path = LOG_DIR / "ingesta_audit.jsonl"
    with open(log_path, "a", encoding="utf-8") as fp:
        fp.write(json.dumps({"source": source, **audit}, default=str) + "\n")
    logger.info("Auditoría guardada → %s", log_path)


# ── Pipeline de ingesta ───────────────────────────────────────────────────────

def ingest_source(source_name: str, file_path: Path = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carga una fuente CSV hacia la capa RAW con:
      - Validación de esquema
      - Detección de corruptos (ID nulo, filas mal formadas)
      - Registro de auditoría
      - Hash de integridad del archivo

    Returns
    -------
    df_clean    : registros válidos listos para curación
    df_errors   : registros rechazados con motivo
    """
    started_at = datetime.utcnow().isoformat()
    schema     = SCHEMAS[source_name]

    if file_path is None:
        file_path = BRONZE_DIR / f"{source_name}.csv"

    logger.info("Iniciando ingesta: %s → %s", source_name, file_path)

    # ── 1. Leer CSV con manejo de errores por fila ────────────────────────────
    try:
        df_raw = pd.read_csv(
            file_path,
            dtype=str,          # Leer TODO como str primero para detectar corruptos
            on_bad_lines="warn",
            encoding="utf-8",
        )
    except Exception as exc:
        logger.error("Fallo crítico al leer %s: %s", file_path, exc)
        raise

    total_raw = len(df_raw)
    logger.info("%s filas leídas desde %s", total_raw, file_path)

    # ── 2. Validar columnas requeridas ────────────────────────────────────────
    missing_cols = [c for c in schema["required_cols"] if c not in df_raw.columns]
    if missing_cols:
        raise ValueError(f"[{source_name}] Columnas faltantes: {missing_cols}")

    # ── 3. Añadir metadatos de ingesta ────────────────────────────────────────
    df_raw["_source"]      = source_name
    df_raw["_ingested_at"] = started_at
    df_raw["_file_hash"]   = _file_hash(file_path)
    df_raw["_row_hash"]    = df_raw[schema["required_cols"]].apply(
        lambda r: hashlib.md5("|".join(str(v) for v in r).encode()).hexdigest(), axis=1
    )

    # ── 4. Separar registros corruptos (ID_ciudadano nulo o no numérico) ──────
    mask_valid_id = df_raw["ID_ciudadano"].str.strip().str.match(r"^\d+$", na=False)
    df_errors = df_raw[~mask_valid_id].copy()
    df_errors["_error_reason"] = "ID_ciudadano inválido o nulo"
    df_clean  = df_raw[mask_valid_id].copy()

    # ── 5. Eliminar duplicados exactos (misma fila completa) ──────────────────
    dup_mask     = df_clean.duplicated(subset=schema["required_cols"], keep="last")
    dup_count    = dup_mask.sum()
    df_dups      = df_clean[dup_mask].copy()
    df_dups["_error_reason"] = "Duplicado exacto – descartado"
    df_errors    = pd.concat([df_errors, df_dups], ignore_index=True)
    df_clean     = df_clean[~dup_mask].copy()

    # ── 6. Castear tipos ──────────────────────────────────────────────────────
    df_clean["ID_ciudadano"] = df_clean["ID_ciudadano"].astype("Int64")
    if "horas" in df_clean.columns:
        df_clean["horas"] = pd.to_numeric(df_clean["horas"], errors="coerce").astype("Int64")

    # ── 7. Guardar capa BRONZE + errores ──────────────────────────────────────
    out_clean  = BRONZE_DIR / f"{source_name}_bronze.parquet"
    out_errors = LOG_DIR / f"{source_name}_errores.parquet"
    df_clean.to_parquet(out_clean,  index=False)
    df_errors.to_parquet(out_errors, index=False)

    # ── 8. Registro de auditoría ──────────────────────────────────────────────
    audit = {
        "ingested_at":      started_at,
        "finished_at":      datetime.utcnow().isoformat(),
        "file_path":        str(file_path),
        "file_hash_sha256": _file_hash(file_path),
        "total_raw_rows":   total_raw,
        "valid_rows":       len(df_clean),
        "error_rows":       len(df_errors),
        "duplicates_found": int(dup_count),
        "output_clean":     str(out_clean),
        "output_errors":    str(out_errors),
    }
    _audit_log(source_name, audit)

    logger.info(
        "[%s] Ingesta OK → %d válidos | %d errores | %d duplicados",
        source_name, len(df_clean), len(df_errors), dup_count,
    )
    return df_clean, df_errors


def run_all_ingestions() -> dict:
    """Ejecuta la ingesta de las tres fuentes y retorna un resumen."""
    results = {}
    for source in ["cea_clases", "crc_examenes", "runt_registros"]:
        df_clean, df_errors = ingest_source(source)
        results[source] = {
            "clean":  df_clean,
            "errors": df_errors,
        }
    logger.info("=== Ingesta completa para todas las fuentes ===")
    return results


if __name__ == "__main__":
    run_all_ingestions()
