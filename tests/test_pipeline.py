"""
Tests unitarios – Olimpia Data Pipeline
"""

import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


# ─────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_cea():
    return pd.DataFrame({
        "ID_ciudadano":  [1, 1, 2, None, 3],
        "clase":         ["Teorica", "Practica", "Teorica", "Practica", "Practica"],
        "horas":         [3, 4, 2, 3, 5],
        "instructor":    ["garcia", "Lopez", "Diaz", "Gomez", "garcia"],
        "fecha":         ["2025-01-10", "2025-01-15", "2025-02-01", "2025-03-01", "2025-01-10"],
        "_source":       ["cea_clases"] * 5,
        "_ingested_at":  ["2026-04-01T00:00:00"] * 5,
        "_file_hash":    ["abc123"] * 5,
        "_row_hash":     ["h1", "h2", "h3", "h4", "h5"],
    }).astype({"ID_ciudadano": "Int64", "horas": "Int64"})


@pytest.fixture
def sample_crc():
    return pd.DataFrame({
        "ID_ciudadano":  [1, 1, 1, 2, 2],
        "tipo_examen":   ["Medico", "Psicologico", "Coordinacion", "Medico", "Psicologico"],
        "resultado":     ["Aprobado", "Aprobado", "Aprobado", "Reprobado", "Aprobado"],
        "fecha":         ["2025-01-05", "2025-01-06", "2025-01-07", "2025-02-01", "2025-02-02"],
        "_source":       ["crc_examenes"] * 5,
        "_ingested_at":  ["2026-04-01T00:00:00"] * 5,
        "_file_hash":    ["def456"] * 5,
        "_row_hash":     ["r1", "r2", "r3", "r4", "r5"],
    }).astype({"ID_ciudadano": "Int64"})


@pytest.fixture
def sample_runt():
    return pd.DataFrame({
        "ID_ciudadano":        [1, 2, 3],
        "estado_licencia":     ["Activa", "Suspendida", "Activa"],
        "fecha_actualizacion": ["2025-06-01", "2025-07-01", "2025-08-01"],
        "_source":             ["runt_registros"] * 3,
        "_ingested_at":        ["2026-04-01T00:00:00"] * 3,
        "_file_hash":          ["ghi789"] * 3,
        "_row_hash":           ["s1", "s2", "s3"],
    }).astype({"ID_ciudadano": "Int64"})


# ─────────────────────────────────────────────────────────────────────────────
# TESTS DE INGESTA
# ─────────────────────────────────────────────────────────────────────────────

class TestIngesta:
    def test_rechaza_id_nulo(self):
        """Regla R1: ciudadanos sin ID deben ser rechazados."""
        from src.ingestion.ingestor import _file_hash
        # Simular el filtro de ID inválido
        df = pd.DataFrame({"ID_ciudadano": ["123", None, "abc", "456", ""]})
        mask = df["ID_ciudadano"].astype(str).str.strip().str.match(r"^\d+$", na=False)
        assert mask.sum() == 2, "Solo IDs puramente numéricos deben pasar"

    def test_duplicados_exactos_detectados(self):
        """Filas completamente duplicadas deben ser marcadas."""
        df = pd.DataFrame({
            "ID_ciudadano": [1, 1, 2],
            "clase":        ["Teorica", "Teorica", "Practica"],
            "horas":        [3, 3, 4],
            "instructor":   ["Garcia", "Garcia", "Lopez"],
            "fecha":        ["2025-01-10", "2025-01-10", "2025-02-01"],
        })
        cols = ["ID_ciudadano", "clase", "horas", "instructor", "fecha"]
        dup_count = df.duplicated(subset=cols, keep="last").sum()
        assert dup_count == 1, "Debe detectar exactamente 1 duplicado"


# ─────────────────────────────────────────────────────────────────────────────
# TESTS DE TRANSFORMACIÓN
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformacion:
    def test_cea_normaliza_clase(self, sample_cea):
        """clase debe quedar en minúsculas."""
        from src.transformation.transformer import transform_cea
        df = sample_cea.dropna(subset=["ID_ciudadano"]).copy()
        result = transform_cea(df)
        assert result["clase_norm"].str.islower().all(), "clase_norm debe estar en minúsculas"

    def test_cea_campo_es_practica(self, sample_cea):
        """es_practica debe ser True solo para clases prácticas."""
        from src.transformation.transformer import transform_cea
        df = sample_cea.dropna(subset=["ID_ciudadano"]).copy()
        result = transform_cea(df)
        practicas = result[result["clase_norm"].str.contains("practica")]
        assert practicas["es_practica"].all(), "Todas las prácticas deben tener es_practica=True"

    def test_cea_horas_acumuladas_positivas(self, sample_cea):
        """horas_acum_ciudadano debe ser siempre positivo."""
        from src.transformation.transformer import transform_cea
        df = sample_cea.dropna(subset=["ID_ciudadano"]).copy()
        result = transform_cea(df)
        assert (result["horas_acum_ciudadano"] > 0).all()

    def test_crc_resultado_aprobado_bool(self, sample_crc):
        """resultado_aprobado debe ser booleano."""
        from src.transformation.transformer import transform_crc
        result = transform_crc(sample_crc)
        assert result["resultado_aprobado"].dtype == bool

    def test_runt_licencia_activa_bool(self, sample_runt):
        """licencia_activa debe ser booleano."""
        from src.transformation.transformer import transform_runt
        result = transform_runt(sample_runt)
        assert result["licencia_activa"].dtype == bool

    def test_runt_un_registro_por_ciudadano(self, sample_runt):
        """RUNT debe dejar solo el registro más reciente por ciudadano."""
        from src.transformation.transformer import transform_runt
        # Añadir duplicado
        extra = sample_runt.iloc[0:1].copy()
        extra["fecha_actualizacion"] = "2025-12-31"
        df = pd.concat([sample_runt, extra], ignore_index=True)
        result = transform_runt(df)
        assert result["ID_ciudadano"].duplicated().sum() == 0

    def test_regla3_duplicados_mismo_dia(self, sample_cea):
        """Misma clase+ciudadano+día → conservar más reciente (R3)."""
        from src.transformation.transformer import transform_cea
        df = sample_cea.dropna(subset=["ID_ciudadano"]).copy()
        # Añadir duplicado con misma fecha para ciudadano 1, Teorica
        dup = df[
            (df["ID_ciudadano"] == 1) & (df["clase"] == "Teorica")
        ].copy()
        dup["horas"] = 99
        dup["_ingested_at"] = "2026-04-02T00:00:00"  # Más reciente
        df2 = pd.concat([df, dup], ignore_index=True)
        result = transform_cea(df2)
        # Debe quedar solo un registro ciudadano=1, clase=teorica, fecha=2025-01-10
        sub = result[
            (result["ID_ciudadano"] == 1) &
            (result["clase_norm"] == "teorica") &
            (result["fecha_date"].astype(str) == "2025-01-10")
        ]
        assert len(sub) == 1
        assert sub.iloc[0]["horas"] == 99, "Debe conservar el registro más reciente"


# ─────────────────────────────────────────────────────────────────────────────
# TESTS DE CALIDAD Y CUMPLIMIENTO
# ─────────────────────────────────────────────────────────────────────────────

class TestCumplimiento:
    def _build_transformed(self, sample_cea, sample_crc, sample_runt):
        from src.transformation.transformer import (
            transform_cea, transform_crc, transform_runt,
            build_dim_ciudadano, build_dim_fecha, build_dim_instructor,
            build_fact_cea, build_fact_crc, build_dim_runt,
        )
        cea_c  = transform_cea(sample_cea.dropna(subset=["ID_ciudadano"]).copy())
        crc_c  = transform_crc(sample_crc.copy())
        runt_c = transform_runt(sample_runt.copy())
        dim    = build_dim_ciudadano(cea_c, crc_c, runt_c)
        dim_f  = build_dim_fecha(cea_c, crc_c, runt_c)
        dim_i  = build_dim_instructor(cea_c)
        return {
            "cea_curated":    cea_c,
            "crc_curated":    crc_c,
            "runt_curated":   runt_c,
            "fact_cea":       build_fact_cea(cea_c, dim, dim_f, dim_i),
            "fact_crc":       build_fact_crc(crc_c, dim, dim_f),
            "dim_runt":       build_dim_runt(runt_c, dim, dim_f),
            "dim_ciudadano":  dim,
            "dim_fecha":      dim_f,
            "dim_instructor": dim_i,
        }

    def test_ciudadano1_crc_completo(self, sample_cea, sample_crc, sample_runt):
        """Ciudadano 1 tiene los 3 exámenes aprobados → crc_completo=True."""
        from src.quality.quality_checks import build_tabla_cumplimiento
        trans = self._build_transformed(sample_cea, sample_crc, sample_runt)
        tabla = build_tabla_cumplimiento(trans)
        row1  = tabla[tabla["ID_ciudadano"] == 1]
        assert not row1.empty
        assert bool(row1.iloc[0]["crc_completo"]) is True

    def test_ciudadano2_crc_incompleto(self, sample_cea, sample_crc, sample_runt):
        """Ciudadano 2 solo tiene 2 tipos de examen → crc_completo=False."""
        from src.quality.quality_checks import build_tabla_cumplimiento
        trans = self._build_transformed(sample_cea, sample_crc, sample_runt)
        tabla = build_tabla_cumplimiento(trans)
        row2  = tabla[tabla["ID_ciudadano"] == 2]
        assert not row2.empty
        assert bool(row2.iloc[0]["crc_completo"]) is False

    def test_tabla_cumplimiento_sin_duplicados(self, sample_cea, sample_crc, sample_runt):
        """Tabla de cumplimiento debe tener un registro por ciudadano."""
        from src.quality.quality_checks import build_tabla_cumplimiento
        trans = self._build_transformed(sample_cea, sample_crc, sample_runt)
        tabla = build_tabla_cumplimiento(trans)
        assert tabla["ID_ciudadano"].duplicated().sum() == 0

    def test_nivel_riesgo_valores_validos(self, sample_cea, sample_crc, sample_runt):
        """nivel_riesgo debe contener solo valores permitidos."""
        from src.quality.quality_checks import build_tabla_cumplimiento
        trans  = self._build_transformed(sample_cea, sample_crc, sample_runt)
        tabla  = build_tabla_cumplimiento(trans)
        validos = {"BAJO", "MEDIO", "ALTO", "CRITICO"}
        assert set(tabla["nivel_riesgo"].unique()).issubset(validos)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
