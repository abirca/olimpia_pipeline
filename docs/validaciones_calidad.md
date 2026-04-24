# Validaciones de Calidad de Datos – Olimpia Data Pipeline

## Resumen de Validaciones

El pipeline implementa validaciones en **tres niveles** alineados con la arquitectura medallón:

```
BRONZE (Ingesta)        → Validación de esquema, integridad, IDs
SILVER (Transformación) → Normalización, deduplicación, consistencia
GOLD (Calidad)          → Métricas agregadas, cumplimiento, anomalías
```

---

## 1. Validaciones en Capa Bronze (Ingesta)

| Validación | Descripción | Acción ante fallo |
|------------|-------------|-------------------|
| Esquema de columnas | Verifica que las columnas requeridas existan en el CSV | Error fatal (detiene la ingesta de esa fuente) |
| ID_ciudadano válido | Regex `^\d+$` – debe ser numérico | Registro → `*_errores.parquet` |
| Duplicados exactos | Misma fila completa duplicada | Se descarta (keep='last') y se registra |
| Hash de integridad | SHA-256 del archivo completo | Se almacena en auditoría |
| Hash por fila | MD5 de columnas clave | Permite detectar cambios entre ingestas |

**Archivo de auditoría**: `data/logs/ingesta_audit.jsonl`

Ejemplo de registro:
```json
{
  "source": "cea_clases",
  "ingested_at": "2026-04-24T14:25:44",
  "file_hash_sha256": "a1b2c3...",
  "total_raw_rows": 500,
  "valid_rows": 498,
  "error_rows": 2,
  "duplicates_found": 0
}
```

---

## 2. Validaciones en Capa Silver (Transformación)

| Validación | Fuente | Descripción |
|------------|--------|-------------|
| Fechas parseables | CEA, CRC, RUNT | `pd.to_datetime(errors='coerce')` → fechas inválidas se eliminan |
| Normalización texto | CEA, CRC, RUNT | Clases → lowercase, instructores → Title Case, estados → lowercase |
| Deduplicación por día | CEA, CRC, RUNT | Mismo ciudadano + mismo campo + mismo día → conservar más reciente |
| RUNT: 1 registro/ciudadano | RUNT | Se conserva solo el registro con fecha más reciente |
| Tipos de examen válidos | CRC | Se identifican tipos fuera de {medico, psicologico, coordinacion} |

**Archivo de métricas**: `data/logs/calidad_datos.jsonl`

Ejemplo:
```json
{
  "source": "cea_curated",
  "total_registros": 500,
  "ciudadanos_unicos": 342,
  "nulos_fecha": 0,
  "pct_practica": 30.0,
  "horas_promedio": 3.5
}
```

---

## 3. Validaciones en Capa Gold (Calidad y Exposición)

### Reporte de Calidad por DataFrame

Para cada tabla (cea_curated, crc_curated, runt_curated, fact_cea, fact_crc) se calcula:

| Métrica | Descripción |
|---------|-------------|
| `total_filas` | Conteo total de registros |
| `filas_duplicadas` | Registros duplicados restantes |
| `pct_duplicadas` | Porcentaje de duplicación |
| `nulos_por_columna` | Conteo de nulos por columna |
| `pct_nulos_por_col` | Porcentaje de nulos por columna |
| `stats_*` | Estadísticas descriptivas para columnas numéricas (mean, std, min, max, etc.) |

**Archivo consolidado**: `data/logs/reporte_calidad_consolidado.json`

### KPIs Finales de Calidad

| KPI | Descripción |
|-----|-------------|
| `total_ciudadanos` | Total de ciudadanos únicos en el sistema |
| `pct_proceso_completo` | % de ciudadanos con CRC + CEA completos |
| `pct_crc_completo` | % con todos los exámenes CRC aprobados |
| `pct_cea_completo` | % con clases teóricas + prácticas |
| `ciudadanos_inconsistencia_runt` | Cantidad con desajuste CRC/CEA vs RUNT |
| `pct_inconsistencia_runt` | % de inconsistencias |
| `total_alertas_fraude` | Alertas generadas por el módulo de fraude |
| `alertas_criticas` | Alertas con severidad CRITICA |

**Archivo**: `data/logs/kpis.json`

---

## Flujo de Validación Completo

```
CSV → [Validar esquema] → [Filtrar IDs inválidos] → [Detectar duplicados]
       ↓ errores.parquet    ↓                         ↓ audit.jsonl
       
Bronze.parquet → [Normalizar fechas] → [Normalizar texto] → [Deduplicar por día]
                  ↓ calidad_datos.jsonl                       ↓

Silver.parquet → [Modelo dimensional] → [Reporte calidad] → [Cumplimiento + Fraude]
                                         ↓ consolidado.json   ↓ kpis.json
                                         
Gold/ → dim_ciudadano + fact_cea + fact_crc + dim_runt + tabla_cumplimiento + alertas_fraude
```

---

## Archivos de Log Generados

| Archivo | Formato | Contenido |
|---------|---------|-----------|
| `data/logs/ingesta_audit.jsonl` | JSON Lines | Auditoría por fuente (hash, conteos, tiempos) |
| `data/logs/calidad_datos.jsonl` | JSON Lines | Métricas de calidad por transformación |
| `data/logs/reporte_calidad_consolidado.json` | JSON | Reporte unificado de todas las fuentes |
| `data/logs/kpis.json` | JSON | KPIs finales del pipeline |
| `data/logs/pipeline_summary.json` | JSON | Resumen de ejecución end-to-end |
| `data/logs/resumen_ejecutivo.json` | JSON | Resumen para dashboard / BI |
| `data/bronze/*_errores.parquet` | Parquet | Registros rechazados con motivo |
