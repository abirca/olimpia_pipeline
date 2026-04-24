# Olimpia Data Pipeline – Data Engineer Assessment

Pipeline de datos end-to-end para integrar, limpiar y disponibilizar
información de **CRC**, **CEA** y **RUNT** para análisis de cumplimiento
normativo y detección de anomalías, desarrollado para Olimpia (uso interno).

---

## Estructura del proyecto

```
olimpia_pipeline/
├── pipeline.py                     # Orquestador principal (entry point)
├── src/
│   ├── ingestion/
│   │   └── ingestor.py             # Carga BRONZE, validación, auditoría
│   ├── transformation/
│   │   └── transformer.py          # Normalización, enriquecimiento, modelo dimensional
│   ├── quality/
│   │   └── quality_checks.py       # Calidad, cumplimiento, fraude, KPIs
│   └── exposure/
│       └── exporter.py             # Exportación dataset final CSV + resumen ejecutivo
├── data/
│   ├── bronze/                     # 🥉 Capa Bronze: CSV originales + parquet validado
│   ├── silver/                     # 🥈 Capa Silver: datos limpios y normalizados
│   ├── gold/                       # 🥇 Capa Gold: modelo dimensional + cumplimiento + dataset final
│   └── logs/                       # Auditoría, calidad, alertas, KPIs
├── docs/
│   ├── arquitectura.md             # Diseño de arquitectura medallón + diagramas ER (Mermaid)
│   ├── modelo_datos.md             # Modelo de datos: esquema estrella, tablas, relaciones
│   ├── reglas_negocio.md           # Reglas de negocio y detección de fraude
│   ├── validaciones_calidad.md     # Validaciones de calidad por capa
│   ├── explicacion_tecnica.md      # Decisiones técnicas, stack, migración a Fabric
│   ├── mejoras_futuras.md          # Roadmap de mejoras a corto/mediano/largo plazo
│   └── presentacion.md            # Guion de presentación (5 diapositivas)
├── tests/
│   └── test_pipeline.py            # Tests unitarios
└── README.md
```

---

## Instalación y ejecución

### Requisitos

```bash
Python >= 3.9
pip install pandas pyarrow
```

### Ejecución rápida

```bash
# 1. Clonar o descomprimir el repositorio
cd olimpia_pipeline

# 2. Instalar dependencias
pip install pandas pyarrow

# 3. Colocar los CSV en data/raw/
cp cea_clases_500.csv   data/raw/cea_clases.csv
cp crc_examenes_500.csv data/raw/crc_examenes.csv
cp runt_registros_500.csv data/raw/runt_registros.csv

# 4. Ejecutar el pipeline completo
python pipeline.py

# 5. Con directorio personalizado de fuentes
python pipeline.py --source-dir /ruta/alternativa/
```

### Salidas generadas

| Archivo | Capa | Descripción |
|---------|------|-------------|
| `data/bronze/*_bronze.parquet` | BRONZE | Datos validados con metadatos de ingesta |
| `data/silver/*_silver.parquet` | SILVER | Datos normalizados con campos derivados |
| `data/gold/dim_ciudadano.parquet` | GOLD | Dimensión ciudadano (surrogate key) |
| `data/gold/fact_cea_clases.parquet` | GOLD | Hechos de clases CEA |
| `data/gold/fact_crc_examenes.parquet` | GOLD | Hechos de exámenes CRC |
| `data/gold/dim_runt.parquet` | GOLD | Dimensión estado licencia RUNT |
| `data/gold/tabla_cumplimiento.parquet` | GOLD | **Tabla maestra de análisis** |
| `data/gold/alertas_fraude.parquet` | GOLD | Alertas de fraude detectadas |
| `data/gold/dataset_final_cumplimiento.csv` | GOLD | **Dataset final CSV para consumo** |
| `data/gold/alertas_fraude.csv` | GOLD | Alertas de fraude en CSV |
| `data/logs/ingesta_audit.jsonl` | LOG | Trazabilidad de cada ingesta |
| `data/logs/calidad_datos.jsonl` | LOG | Métricas de calidad por fuente |
| `data/logs/kpis.json` | LOG | KPIs consolidados del pipeline |
| `data/logs/pipeline_summary.json` | LOG | Resumen end-to-end de la ejecución |
| `data/logs/resumen_ejecutivo.json` | LOG | Resumen ejecutivo para dashboards |

---

## Pipeline end-to-end

```
CSV Fuentes (CEA / CRC / RUNT)
        │
        ▼
┌───────────────────┐
│  INGESTA (BRONZE)  │  → Validación esquema, hash SHA-256, trazabilidad
│  ingestor.py       │  → Separación de registros corruptos
└────────┬──────────┘  → Parquet bronze + log de errores
         │
         ▼
┌───────────────────────┐
│  TRANSFORMACIÓN        │  → Normalización fechas ISO-8601
│  (SILVER + GOLD)      │  → Estandarización de texto (lower, title-case)
│  transformer.py        │  → Deduplicación: mismo ciudadano + día → más reciente
│                        │  → Campos enriquecidos (es_practica, horas_acum, etc.)
│                        │  → Modelo dimensional: DIM + FACT
└────────┬──────────────┘
         │
         ▼
┌─────────────────────────┐
│  CALIDAD + CUMPLIMIENTO │  → Reporte de calidad con métricas
│  + FRAUDE               │  → Tabla maestra de cumplimiento CRC/CEA/RUNT
│  quality_checks.py      │  → Niveles de riesgo por ciudadano
│                         │  → 5 patrones de detección de fraude
│                         │  → KPIs para dashboard de cumplimiento
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  EXPOSICIÓN             │  → Dataset final CSV (tabla_cumplimiento legible)
│  exporter.py            │  → Alertas de fraude en CSV
│                         │  → Resumen ejecutivo JSON para dashboards
└─────────────────────────┘
         │
         ▼
   📊 Dashboard Power BI / API REST / Fabric Lakehouse
```

---

## Reglas de negocio implementadas

| Regla | Implementación |
|-------|----------------|
| R1: Sin ciudadanos sin ID | `ingestor.py` – rechaza filas con ID nulo/inválido |
| R2: Fechas válidas y razonables | `transformer.py` – `pd.to_datetime(errors='coerce')` + drop NaT |
| R3: Múltiples registros mismo día → más reciente | `transformer.py` – `sort + drop_duplicates(keep='first')` |
| R5: Log de errores separado | `ingestor.py` → `data/logs/*_errores.parquet` |

---

## Detección de fraude – Patrones

| Código | Patrón | Severidad |
|--------|--------|-----------|
| F2 | CEA completado en < 3 días | ALTA |
| F3 | Licencia RUNT activa sin ningún CRC registrado | CRÍTICA |
| F4 | Mismo examen, mismo ciudadano, mismo día, resultados contradictorios | CRÍTICA |
| F5 | Instructor con > 10 clases en un solo día | MEDIA |

---

## KPIs del dashboard de cumplimiento

- **% proceso completo**: ciudadanos con CRC (3 exámenes aprobados) + CEA (teoría + práctica)
- **% inconsistencia RUNT**: ciudadanos donde el estado RUNT no corresponde con su proceso
- **Nivel de riesgo**: BAJO / MEDIO / ALTO / CRÍTICO por ciudadano
- **Alertas de fraude**: conteo y severidad

---

## Supuestos y limitaciones

1. Los CSV son archivos planos sin cabecera de versión ni firma digital.
2. No se dispone de fecha de nacimiento en los datos, por lo que la regla R2 de "fechas de nacimiento razonables" se aplica a fechas de proceso (descartar fechas no parseables).
3. El modelo dimensional se construye sin base de datos relacional (Parquet en lago local); en producción se reemplaza por Delta Lake en Microsoft Fabric o similar.
4. La detección de fraude usa heurísticas; en producción se complementa con modelos ML (Isolation Forest, etc.).
5. Los datos simulados no contienen errores intencionales; en producción los rechazos serían mayores.

---

## Mejoras futuras

- Orquestación con **Apache Airflow** o **Microsoft Fabric Pipelines**
- Formato **Delta Lake** para soporte de ACID y time travel
- **Great Expectations** para contratos de datos formales
- Modelo ML de detección de fraude (Isolation Forest / Autoencoder)
- API REST con **FastAPI** para exposición de la tabla_cumplimiento
- Integración con **Power BI** vía DirectLake en Microsoft Fabric
- Notificaciones automáticas por correo/Teams vía alertas CRÍTICAS
