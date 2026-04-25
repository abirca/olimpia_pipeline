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
│   │   └── transformer.py          # Normalización SILVER, enriquecimiento
│   ├── modelo/
│   │   └── gold_model.py           # Modelo dimensional GOLD (esquema estrella)
│   ├── orquestacion/
│   │   └── orchestrator.py         # Orquestación de ingesta + ETL
│   ├── quality/
│   │   └── quality_checks.py       # Calidad, cumplimiento, fraude, KPIs
│   └── exposure/
│       └── exporter.py             # Exportación dataset final CSV + resumen ejecutivo
├── data/
│   ├── bronze/                     # 🥉 Capa Bronze: CSV originales + parquet validado
│   ├── silver/                     # 🥈 Capa Silver: datos limpios y normalizados
│   ├── gold/                       # 🥇 Capa Gold: modelo dimensional + cumplimiento + dataset final
│   └── logs/                       # Auditoría, calidad, alertas, KPIs
├── dashboard/
│   └── Superintendencia de Transporte.pbix  # Reporte Power BI conectado a Gold
├── docs/
│   ├── arquitectura.md             # Diseño de arquitectura medallón + diagramas ER (Mermaid)
│   ├── modelo_datos.md             # Modelo de datos: esquema estrella, tablas, relaciones
│   ├── reglas_negocio.md           # Reglas de negocio y detección de fraude
│   ├── validaciones_calidad.md     # Validaciones de calidad por capa
│   ├── explicacion_tecnica.md      # Decisiones técnicas, stack, migración a Fabric
│   └── mejoras_futuras.md          # Roadmap de mejoras a corto/mediano/largo plazo
├── tests/
│   └── test_pipeline.py            # Tests unitarios (13 tests)
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
| `data/gold/dim_fecha.parquet` | GOLD | Dimensión calendario |
| `data/gold/dim_instructor.parquet` | GOLD | Dimensión instructores CEA |
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
   📊 Dashboard Power BI
```

---

## Dashboard Power BI – Superintendencia de Transporte

El reporte Power BI (`dashboard/Superintendencia de Transporte.pbix`) se conecta
directamente a los archivos Parquet de la capa **Gold** e implementa el modelo
estrella completo.

### Contenido del modelo semántico

| Componente | Detalle |
|---|---|
| **Tablas** | 8 tablas: dim_ciudadano, dim_fecha, dim_instructor, dim_runt, fact_cea_clases, fact_crc_examenes, tabla_cumplimiento, alertas_fraude |
| **Relaciones** | 9 relaciones (7 activas + 2 inactivas para evitar ambigüedad de filtro) |
| **Medidas DAX** | 26 medidas organizadas en carpetas: KPIs Generales, CRC, CEA, RUNT, Fraude |
| **Tabla de fechas** | dim_fecha marcada como tabla de fechas para time intelligence |

### Medidas DAX disponibles

| Carpeta | Medida | Expresión |
|---------|--------|-----------|
| KPIs Generales | Total Ciudadanos | `COUNTROWS(dim_ciudadano)` |
| KPIs Generales | % Proceso Completo | `DIVIDE([Proceso Completo], [Total Ciudadanos])` |
| KPIs Generales | Riesgo Alto | `CALCULATE(COUNTROWS(...), nivel_riesgo = "ALTO")` |
| CRC | Total Examenes CRC | `COUNTROWS(fact_crc_examenes)` |
| CRC | % Aprobacion CRC | `DIVIDE([Examenes Aprobados CRC], [Total Examenes CRC])` |
| CEA | Total Clases CEA / Total Horas CEA | `COUNTROWS(...)` / `SUM(horas)` |
| RUNT | Inconsistencias RUNT / Licencias Activas | Conteos filtrados |
| Fraude | Total Alertas Fraude / Alertas Alta Severidad | Conteos filtrados |

### Cómo usar

1. Abrir `dashboard/Superintendencia de Transporte.pbix` en Power BI Desktop.
2. Si los datos no cargan, ir a **Transformar datos → Configuración de origen** y actualizar la ruta de los Parquet a la ubicación local de `data/gold/`.
3. Hacer clic en **Actualizar** para refrescar los datos.

---

## Modelo Dimensional – Esquema Estrella (Star Schema)

La capa **Gold** implementa un esquema estrella con `DIM_CIUDADANO` como
dimensión central, `DIM_FECHA` y `DIM_INSTRUCTOR` como dimensiones
conformadas, y dos tablas de hechos transaccionales.

### Diagrama

```
                          ┌───────────────────┐
                          │   DIM_FECHA       │
                          │ sk_fecha (PK)     │
                          │ fecha, anio, mes  │
                          │ trimestre, dia    │
                          └────────┬──────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
             ┌──────┴──────┐ ┌────┴─────┐ ┌──────┴──────┐
             │FACT_CEA     │ │FACT_CRC  │ │ DIM_RUNT    │
             │CLASES       │ │EXAMENES  │ │ sk_runt(PK) │
             │sk_fact_cea  │ │sk_fact_crc│ │ sk_fecha(FK)│
             │(PK)         │ │(PK)      │ └──────┬──────┘
             └──────┬──────┘ └────┬─────┘        │
                    │             │               │
┌──────────────┐  ┌─┴─────────────┴─┐  ┌─────────┘
│DIM_INSTRUCTOR│──│  DIM_CIUDADANO   │──┘
│sk_instructor │  │  ⭐ sk_ciudadano │
│(PK)          │  │  (PK)           │
└──────────────┘  └───────┬─────────┘
                          │
             ┌────────────┼────────────┐
             │                         │
  ┌──────────┴───────────┐  ┌──────────┴──────────┐
  │ TABLA_CUMPLIMIENTO   │  │  ALERTAS_FRAUDE     │
  │ sk_ciudadano (PK/FK) │  │  sk_alerta (PK)     │
  └──────────────────────┘  └─────────────────────┘
```

### Tablas y columnas

#### DIM_CIUDADANO (Dimensión Central)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | BIGINT (PK) | Surrogate key autoincremental |
| `ID_ciudadano` | BIGINT (NK) | ID natural del ciudadano |
| `_gold_run_id` | STRING | Identificador de la corrida Gold |
| `_created_at` | TIMESTAMP | Fecha/hora de creación del registro |

> **Grain**: 1 fila por ciudadano único.

#### DIM_FECHA (Dimensión Calendario)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_fecha` | INT (PK) | Surrogate key |
| `fecha` | DATE | Fecha calendario |
| `anio` | INT | Año |
| `mes` | INT | Mes (1-12) |
| `dia` | INT | Día del mes |
| `trimestre` | INT | Trimestre (1-4) |
| `nombre_mes` | STRING | Nombre del mes |
| `es_fin_semana` | BOOL | Sábado o domingo |

> **Grain**: 1 fila por fecha única vista en todas las fuentes.

#### DIM_INSTRUCTOR (Dimensión Instructor)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_instructor` | BIGINT (PK) | Surrogate key |
| `instructor_norm` | STRING | Nombre del instructor (Title Case) |

> **Grain**: 1 fila por instructor único de CEA.

#### FACT_CEA_CLASES (Hechos)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_fact_cea` | BIGINT (PK) | Surrogate key del hecho |
| `sk_ciudadano` | BIGINT (FK) | → DIM_CIUDADANO |
| `sk_fecha` | INT (FK) | → DIM_FECHA |
| `sk_instructor` | BIGINT (FK) | → DIM_INSTRUCTOR |
| `clase_norm` | STRING | teorica / practica |
| `horas` | INT | Horas de la clase |
| `es_practica` | BOOL | Indicador de clase práctica |
| `horas_acum_ciudadano` | INT | Horas acumuladas por ciudadano |
| `fecha_date` | DATE | Fecha de la clase |

> **Grain**: 1 fila por ciudadano + clase + fecha.

#### FACT_CRC_EXAMENES (Hechos)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_fact_crc` | BIGINT (PK) | Surrogate key del hecho |
| `sk_ciudadano` | BIGINT (FK) | → DIM_CIUDADANO |
| `sk_fecha` | INT (FK) | → DIM_FECHA |
| `tipo_examen_norm` | STRING | medico / psicologico / coordinacion |
| `resultado_aprobado` | BOOL | Aprobó el examen |
| `examenes_aprobados_acum` | INT | Acumulado de aprobados |
| `fecha_date` | DATE | Fecha del examen |

> **Grain**: 1 fila por ciudadano + tipo de examen + fecha.

#### DIM_RUNT (Dimensión – SCD Tipo 1)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_runt` | BIGINT (PK) | Surrogate key |
| `sk_ciudadano` | BIGINT (FK) | → DIM_CIUDADANO |
| `sk_fecha` | INT (FK) | → DIM_FECHA |
| `estado_licencia_norm` | STRING | activa / suspendida / cancelada |
| `licencia_activa` | BOOL | Licencia vigente |
| `dias_desde_actualizacion` | INT | Días desde última actualización |

> **Grain**: 1 fila por ciudadano (registro más reciente). SCD Tipo 1.

#### TABLA_CUMPLIMIENTO (Fact Table Agregada)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | BIGINT (PK/FK) | → DIM_CIUDADANO |
| `crc_completo` | BOOL | 3 tipos de examen aprobados |
| `cea_completo` | BOOL | Al menos 1 teórica + 1 práctica |
| `proceso_completo` | BOOL | CRC + CEA ambos completos |
| `inconsistencia_runt` | BOOL | Desajuste entre proceso y RUNT |
| `nivel_riesgo` | STRING | BAJO / MEDIO / ALTO / CRITICO |

> **Grain**: 1 fila por ciudadano. Responde directamente las preguntas de negocio.

#### ALERTAS_FRAUDE

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_alerta` | BIGINT (PK) | Surrogate key |
| `ID_ciudadano` | BIGINT (FK) | Ciudadano afectado |
| `tipo_alerta` | STRING | Código F1–F5 |
| `detalle` | STRING | Descripción de la anomalía |
| `severidad` | STRING | CRITICA / ALTA / MEDIA |
| `detectado_en` | TIMESTAMP | Timestamp de detección |

### Relaciones y cardinalidades

```
dim_ciudadano.sk_ciudadano   → 1:N → fact_cea_clases.sk_ciudadano
dim_ciudadano.sk_ciudadano   → 1:N → fact_crc_examenes.sk_ciudadano
dim_fecha.sk_fecha           → 1:N → fact_cea_clases.sk_fecha
dim_fecha.sk_fecha           → 1:N → fact_crc_examenes.sk_fecha
dim_fecha.sk_fecha           → 1:N → dim_runt.sk_fecha
dim_instructor.sk_instructor → 1:N → fact_cea_clases.sk_instructor
dim_ciudadano.sk_ciudadano   → 1:1 → dim_runt.sk_ciudadano
dim_ciudadano.sk_ciudadano   → 1:1 → tabla_cumplimiento.sk_ciudadano
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
- ~~Integración con **Power BI**~~ ✅ Implementado (`dashboard/Superintendencia de Transporte.pbix`)
- Migración a **Microsoft Fabric** con notebooks PySpark y Delta Lake
- Migración a **DirectLake** en Microsoft Fabric (actualmente Import desde Parquet)
- Notificaciones automáticas por correo/Teams vía alertas CRÍTICAS
