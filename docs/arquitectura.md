# Diseño de Arquitectura – Olimpia Data Pipeline

## 1. Arquitectura Medallón (Bronze → Silver → Gold)

Se adopta el patrón **Medallion Architecture** con las tres capas estándar:

```
BRONZE  →  SILVER  →  GOLD
(raw)      (clean)    (business)
```

### ¿Por qué este patrón y no otro?

| Alternativa | Por qué NO se eligió |
|-------------|----------------------|
| Data Warehouse clásico (star schema puro) | Requiere esquema rígido desde el inicio; difícil de evolucionar con nuevas fuentes |
| Normalización 3FN en base relacional | Excesiva complejidad de JOINs para análisis; no escala bien con volumen |
| Raw directo a exposición | Sin linaje, sin calidad, sin auditoría |
| **Medallion / Data Lakehouse** ✅ | Flexibilidad de esquema en Bronze, calidad incremental, trazabilidad, compatible con Fabric |

---

## 2. Capas del Lakehouse

### BRONZE (Capa de Ingesta)
- **Qué contiene**: datos tal como llegan de las fuentes, enriquecidos solo con metadatos de ingesta.
- **Formato**: CSV originales + Parquet validado.
- **Quién escribe**: `ingestor.py` únicamente.
- **Retención**: indefinida (auditoría regulatoria).
- **Metadatos añadidos**: `_source`, `_ingested_at`, `_file_hash`, `_row_hash`.
- **Directorio**: `data/bronze/`

### SILVER (Capa de Conformidad)
- **Qué contiene**: datos limpios, normalizados, con campos derivados.
- **Formato**: Parquet (en producción: Delta Lake para ACID + time travel).
- **Reglas aplicadas**: deduplicación, normalización fechas/texto, campos enriquecidos.
- **Quién escribe**: `transformer.py`.
- **Directorio**: `data/silver/`

### GOLD (Capa de Negocio)
- **Qué contiene**: modelo dimensional (esquema estrella) + tablas de análisis.
- **Tablas**:
  - `dim_ciudadano` – dimensión central, todos los IDs únicos con surrogate key.
  - `fact_cea_clases` – hechos granulares de clases (grain: ciudadano + clase + fecha).
  - `fact_crc_examenes` – hechos granulares de exámenes (grain: ciudadano + tipo + fecha).
  - `dim_runt` – estado más reciente de licencia por ciudadano.
  - `tabla_cumplimiento` – vista analítica cruzada, lista para dashboards.
  - `alertas_fraude` – señales de anomalías detectadas.
- **Directorio**: `data/gold/`

---

## 3. Flujo de Datos por Capas

```mermaid
flowchart LR
    subgraph Fuentes["📥 Fuentes Externas"]
        CSV_CEA["cea_clases.csv"]
        CSV_CRC["crc_examenes.csv"]
        CSV_RUNT["runt_registros.csv"]
    end

    subgraph Bronze["🥉 BRONZE – Ingesta"]
        B_CEA["cea_clases_bronze.parquet"]
        B_CRC["crc_examenes_bronze.parquet"]
        B_RUNT["runt_registros_bronze.parquet"]
    end

    subgraph Silver["🥈 SILVER – Conformidad"]
        S_CEA["cea_clases_silver.parquet"]
        S_CRC["crc_examenes_silver.parquet"]
        S_RUNT["runt_registros_silver.parquet"]
    end

    subgraph Gold["🥇 GOLD – Modelo Estrella"]
        DIM_C["dim_ciudadano"]
        FACT_CEA["fact_cea_clases"]
        FACT_CRC["fact_crc_examenes"]
        DIM_R["dim_runt"]
        CUMPL["tabla_cumplimiento"]
        ALERT["alertas_fraude"]
    end

    CSV_CEA --> B_CEA
    CSV_CRC --> B_CRC
    CSV_RUNT --> B_RUNT

    B_CEA --> S_CEA
    B_CRC --> S_CRC
    B_RUNT --> S_RUNT

    S_CEA --> FACT_CEA
    S_CRC --> FACT_CRC
    S_RUNT --> DIM_R
    S_CEA --> DIM_C
    S_CRC --> DIM_C
    S_RUNT --> DIM_C

    FACT_CEA --> CUMPL
    FACT_CRC --> CUMPL
    DIM_R --> CUMPL
    DIM_C --> CUMPL
    CUMPL --> ALERT
```

---

## 4. Modelo de Datos – Esquema Estrella (Star Schema)

### ¿Por qué Estrella y no Copo de Nieve?

| Criterio | Star Schema ✅ | Snowflake ❌ |
|----------|---------------|-------------|
| Complejidad de JOINs | Baja (1 nivel) | Alta (múltiples niveles) |
| Rendimiento en consultas | Óptimo para BI | Más lento por JOINs cascada |
| Facilidad para Power BI | Nativa (DirectLake) | Requiere vistas intermedias |
| Redundancia controlada | Sí (desnormalizado) | No (normalizado) |
| Mantenimiento | Sencillo | Complejo |

**Se eligió Esquema Estrella** porque:
1. Las dimensiones son pocas y simples (ciudadano, RUNT).
2. No hay jerarquías profundas que justifiquen normalizar dimensiones en sub-tablas.
3. Power BI trabaja nativamente con star schema.
4. El grain de los hechos es el más granular posible (cada clase / cada examen).
5. `tabla_cumplimiento` es una **fact table desnormalizada** (una fila por ciudadano) optimizada para el dashboard.

### Diagrama Entidad-Relación (Gold Layer)

```mermaid
erDiagram
    DIM_CIUDADANO {
        int sk_ciudadano PK "Surrogate Key"
        int ID_ciudadano UK "ID natural del ciudadano"
    }

    FACT_CEA_CLASES {
        int sk_ciudadano FK "FK dim_ciudadano"
        int ID_ciudadano FK "ID natural"
        string clase_norm "Tipo de clase (teorica/practica)"
        int horas "Horas de la clase"
        string instructor_norm "Nombre instructor"
        date fecha_date "Fecha de la clase"
        boolean es_practica "Es clase practica"
        int horas_acum_ciudadano "Horas acumuladas"
        string _source "Fuente origen"
        datetime _ingested_at "Timestamp ingesta"
    }

    FACT_CRC_EXAMENES {
        int sk_ciudadano FK "FK dim_ciudadano"
        int ID_ciudadano FK "ID natural"
        string tipo_examen_norm "medico psicologico coordinacion"
        boolean resultado_aprobado "Aprobo el examen"
        date fecha_date "Fecha del examen"
        int examenes_aprobados_acum "Acumulado aprobados"
        string _source "Fuente origen"
        datetime _ingested_at "Timestamp ingesta"
    }

    DIM_RUNT {
        int sk_ciudadano FK "FK dim_ciudadano"
        int ID_ciudadano FK "ID natural"
        string estado_licencia_norm "activa suspendida cancelada"
        boolean licencia_activa "Licencia vigente"
        date fecha_actualizacion_date "Ultima actualizacion"
        int dias_desde_actualizacion "Dias desde ultima actualizacion"
        string _source "Fuente origen"
        datetime _ingested_at "Timestamp ingesta"
    }

    TABLA_CUMPLIMIENTO {
        int sk_ciudadano FK "FK dim_ciudadano"
        int ID_ciudadano FK "ID natural"
        boolean crc_completo "3 examenes aprobados"
        boolean cea_completo "Teorica y practica"
        boolean proceso_completo "CRC y CEA completos"
        boolean inconsistencia_runt "Desajuste con RUNT"
        string nivel_riesgo "BAJO MEDIO ALTO CRITICO"
    }

    ALERTAS_FRAUDE {
        string tipo_alerta "F1 a F5 codigo de alerta"
        int ID_ciudadano FK "Ciudadano afectado"
        string detalle "Descripcion de la anomalia"
        string severidad "CRITICA ALTA MEDIA"
        datetime detectado_en "Timestamp deteccion"
    }

    DIM_CIUDADANO ||--o{ FACT_CEA_CLASES : "tiene clases"
    DIM_CIUDADANO ||--o{ FACT_CRC_EXAMENES : "tiene examenes"
    DIM_CIUDADANO ||--o| DIM_RUNT : "tiene licencia"
    DIM_CIUDADANO ||--o| TABLA_CUMPLIMIENTO : "resumen cumplimiento"
    DIM_CIUDADANO ||--o{ ALERTAS_FRAUDE : "alertas asociadas"
```

### Estructura Visual del Esquema Estrella

```mermaid
graph TD
    CENTER["⭐ DIM_CIUDADANO<br/>sk_ciudadano PK<br/>ID_ciudadano UK"]

    FACT1["📋 FACT_CEA_CLASES<br/>grain: ciudadano+clase+fecha<br/>horas, instructor, es_practica"]
    FACT2["📋 FACT_CRC_EXAMENES<br/>grain: ciudadano+examen+fecha<br/>tipo_examen, resultado_aprobado"]
    DIM1["📦 DIM_RUNT<br/>estado licencia mas reciente<br/>licencia_activa, dias_actualizacion"]
    AGG["📊 TABLA_CUMPLIMIENTO<br/>1 fila por ciudadano<br/>crc_completo, cea_completo, nivel_riesgo"]
    FRAUD["🚨 ALERTAS_FRAUDE<br/>anomalias detectadas<br/>tipo_alerta, severidad"]

    CENTER --- FACT1
    CENTER --- FACT2
    CENTER --- DIM1
    CENTER --- AGG
    CENTER --- FRAUD

    style CENTER fill:#FFD700,stroke:#333,color:#000
    style FACT1 fill:#87CEEB,stroke:#333,color:#000
    style FACT2 fill:#87CEEB,stroke:#333,color:#000
    style DIM1 fill:#90EE90,stroke:#333,color:#000
    style AGG fill:#DDA0DD,stroke:#333,color:#000
    style FRAUD fill:#FF6B6B,stroke:#333,color:#000
```

---

## 5. Detalle de Relaciones (Foreign Keys)

| Tabla Origen | Columna FK | → Tabla Destino | Columna PK | Cardinalidad | Descripción |
|-------------|-----------|----------------|-----------|-------------|-------------|
| `fact_cea_clases` | `sk_ciudadano` | `dim_ciudadano` | `sk_ciudadano` | N:1 | Cada clase pertenece a un ciudadano |
| `fact_crc_examenes` | `sk_ciudadano` | `dim_ciudadano` | `sk_ciudadano` | N:1 | Cada examen pertenece a un ciudadano |
| `dim_runt` | `sk_ciudadano` | `dim_ciudadano` | `sk_ciudadano` | 1:1 | Un registro RUNT por ciudadano (SCD Tipo 1) |
| `tabla_cumplimiento` | `sk_ciudadano` | `dim_ciudadano` | `sk_ciudadano` | 1:1 | Resumen analítico por ciudadano |
| `alertas_fraude` | `ID_ciudadano` | `dim_ciudadano` | `ID_ciudadano` | N:1 | Múltiples alertas por ciudadano |

### Reglas de Negocio del Modelo

- **CRC completo** = tiene los 3 tipos de examen (médico, psicológico, coordinación) y todos aprobados.
- **CEA completo** = tiene al menos una clase teórica Y una práctica.
- **RUNT consistente** = licencia activa si CRC+CEA completos, o licencia no activa si proceso incompleto.
- **Inconsistencia RUNT** = proceso completo pero sin licencia activa, o viceversa.
- **Nivel de riesgo**:
  - `BAJO` = proceso completo y licencia consistente.
  - `MEDIO` = proceso incompleto, sin inconsistencia.
  - `ALTO` = proceso completo pero licencia NO activa.
  - `CRITICO` = proceso incompleto pero licencia activa (posible fraude).

---

## 6. Grain (Granularidad) de cada Tabla

| Tabla | Grain | Ejemplo |
|-------|-------|---------|
| `dim_ciudadano` | 1 fila por ciudadano único | Ciudadano 101 |
| `fact_cea_clases` | 1 fila por ciudadano + clase + fecha | Ciudadano 101, clase teórica, 2025-01-15 |
| `fact_crc_examenes` | 1 fila por ciudadano + tipo examen + fecha | Ciudadano 101, examen médico, 2025-02-01 |
| `dim_runt` | 1 fila por ciudadano (registro más reciente) | Ciudadano 101, licencia activa |
| `tabla_cumplimiento` | 1 fila por ciudadano (vista agregada) | Ciudadano 101, proceso completo, riesgo BAJO |
| `alertas_fraude` | 1 fila por alerta detectada | Ciudadano 101, F3_LICENCIA_SIN_CRC |

---

## 7. Arquitectura en Microsoft Fabric

```mermaid
flowchart TB
    subgraph Fabric["Microsoft Fabric Workspace"]
        subgraph Ingest["Data Factory / Pipelines"]
            DF["Copy Data Activity"]
        end

        subgraph Lake["Fabric Lakehouse"]
            BZ["🥉 Bronze<br/>Delta Tables"]
            SV["🥈 Silver<br/>Delta Tables"]
            GL["🥇 Gold<br/>Delta Tables"]
        end

        subgraph Compute["Spark Notebooks"]
            NB1["ingestor.py<br/>(PySpark)"]
            NB2["transformer.py<br/>(PySpark)"]
            NB3["quality_checks.py<br/>(PySpark)"]
        end

        subgraph Expose["Capa de Exposicion"]
            SM["Semantic Model<br/>(DirectLake)"]
            PBI["Power BI Dashboard<br/>Cumplimiento"]
            DA["Data Activator<br/>Alertas Automaticas"]
        end
    end

    DF --> NB1
    NB1 --> BZ
    BZ --> NB2
    NB2 --> SV
    NB2 --> GL
    GL --> NB3
    NB3 --> GL
    GL --> SM
    SM --> PBI
    GL --> DA

    style BZ fill:#CD7F32,stroke:#333,color:#fff
    style SV fill:#C0C0C0,stroke:#333,color:#000
    style GL fill:#FFD700,stroke:#333,color:#000
```

### Mapa de módulos Python → componentes Fabric

| Módulo Python | Equivalente en Fabric |
|---------------|----------------------|
| `ingestor.py` | **Data Pipeline** + actividad Copy Data |
| `transformer.py` | **Spark Notebook** (PySpark) en Lakehouse |
| `quality_checks.py` | **Spark Notebook** + **Data Activator** para alertas |
| `tabla_cumplimiento` | **Semantic Model** → Power BI DirectLake |
| `alertas_fraude` | **Data Activator** → notificaciones Teams |

---

## 8. Decisiones Técnicas

| Decisión | Elección | Justificación |
|----------|----------|---------------|
| Lenguaje | Python 3.9+ | Ecosistema maduro, compatible con Fabric Notebooks y PySpark |
| Formato de almacenamiento | Parquet (local) / Delta Lake (Fabric) | Columnar, comprimido, soporte ACID en Delta |
| Motor | Pandas (dev) / PySpark (prod) | Mismo código, diferente escala |
| Orquestación | Script directo (dev) / Fabric Pipelines (prod) | Progresión natural sin lock-in |
| **Modelo de datos** | **Esquema Estrella** | Óptimo para Power BI, JOINs simples, sin jerarquías profundas |
| **Arquitectura de capas** | **Medallion (Bronze/Silver/Gold)** | Linaje completo, recuperabilidad, auditoría regulatoria |
| Dimensión RUNT | SCD Tipo 1 (sobrescribir) | Solo interesa el estado más reciente de la licencia |
| Surrogate keys | `sk_ciudadano` autoincremental | Desacopla el modelo analítico del ID natural |
