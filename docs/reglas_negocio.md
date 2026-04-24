# Reglas de Negocio – Olimpia Data Pipeline

## Reglas Obligatorias Implementadas

### R1: No debe haber ciudadanos sin ID

| Aspecto | Detalle |
|---------|---------|
| **Módulo** | `src/ingestion/ingestor.py` |
| **Implementación** | Filtro regex `^\d+$` sobre `ID_ciudadano` |
| **Acción** | Registros con ID nulo, vacío o no numérico → rechazados a `*_errores.parquet` |
| **Motivo del rechazo** | `"ID_ciudadano inválido o nulo"` |
| **Trazabilidad** | Se registra en `data/logs/ingesta_audit.jsonl` con conteo de errores |

```python
mask_valid_id = df_raw["ID_ciudadano"].str.strip().str.match(r"^\d+$", na=False)
df_errors = df_raw[~mask_valid_id].copy()
df_errors["_error_reason"] = "ID_ciudadano inválido o nulo"
```

### R2: Las fechas deben ser válidas y razonables

| Aspecto | Detalle |
|---------|---------|
| **Módulo** | `src/transformation/transformer.py` |
| **Implementación** | `pd.to_datetime(errors='coerce')` + drop de NaT |
| **Acción** | Fechas no parseables se convierten a NaT y se eliminan del dataset |
| **Log** | Se registra el conteo de fechas inválidas por columna en `calidad_datos.jsonl` |

```python
parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)
bad = parsed.isna().sum()
if bad:
    logger.warning("[%s] %d fechas no parseables → NaT", col_name, bad)
```

### R3: Múltiples registros mismo ciudadano + mismo día → conservar más reciente

| Aspecto | Detalle |
|---------|---------|
| **Módulo** | `src/transformation/transformer.py` |
| **Aplica a** | CEA (ciudadano+clase+día), CRC (ciudadano+examen+día), RUNT (ciudadano+día) |
| **Implementación** | Sort por `_ingested_at` DESC + `drop_duplicates(keep='first')` |
| **Criterio** | El registro con `_ingested_at` más reciente prevalece |

```python
df = df.sort_values("_ingested_at", ascending=False)
df = df.drop_duplicates(subset=["ID_ciudadano", "clase_norm", "fecha_date"], keep="first")
```

### R5: Registrar errores de calidad en estructura separada

| Aspecto | Detalle |
|---------|---------|
| **Módulo** | `src/ingestion/ingestor.py` + `src/transformation/transformer.py` |
| **Estructuras de error** | |
| → Errores de ingesta | `data/bronze/*_errores.parquet` (registros rechazados con `_error_reason`) |
| → Auditoría de ingesta | `data/logs/ingesta_audit.jsonl` (JSON Lines por fuente) |
| → Calidad de transformación | `data/logs/calidad_datos.jsonl` (métricas por fuente) |
| → Reporte consolidado | `data/logs/reporte_calidad_consolidado.json` |
| → KPIs finales | `data/logs/kpis.json` |

---

## Reglas de Cumplimiento (Derivadas)

### CRC Completo
Un ciudadano tiene CRC completo si:
- Tiene los 3 tipos de examen: `médico`, `psicológico`, `coordinación`.
- **Todos** los exámenes fueron aprobados (ningún reprobado).

### CEA Completo
Un ciudadano tiene CEA completo si:
- Tiene al menos **1 clase teórica** Y al menos **1 clase práctica**.

### Proceso Completo
- `proceso_completo = crc_completo AND cea_completo`

### Inconsistencia RUNT
Se detecta inconsistencia cuando:
- Proceso completo **PERO** licencia NO activa (Estado: `ALTO`).
- Proceso incompleto **PERO** licencia activa (Estado: `CRITICO` – posible fraude).

### Niveles de Riesgo

| Nivel | Condición | Acción Sugerida |
|-------|-----------|-----------------|
| `BAJO` | Proceso completo + licencia consistente | Ninguna |
| `MEDIO` | Proceso incompleto, sin inconsistencia | Seguimiento estándar |
| `ALTO` | Proceso completo pero licencia NO activa | Revisión administrativa |
| `CRITICO` | Proceso incompleto pero licencia activa | **Investigación de fraude** |

---

## Reglas de Detección de Fraude

| Código | Patrón | Severidad | Descripción |
|--------|--------|-----------|-------------|
| F2 | CEA completado en < 3 días | ALTA | Proceso de formación sospechosamente rápido |
| F3 | Licencia RUNT activa sin ningún CRC | CRITICA | Licencia sin exámenes – posible falsificación |
| F4 | Mismo examen + mismo día + resultados contradictorios | CRITICA | Manipulación de registros |
| F5 | Instructor con >10 clases en un solo día | MEDIA | Posible colusión o registro masivo fraudulento |
