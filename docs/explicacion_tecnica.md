# Explicación Técnica – Olimpia Data Pipeline

## Visión General

Pipeline de datos end-to-end que integra tres fuentes del sector transporte colombiano (CEA, CRC, RUNT) para análisis de cumplimiento normativo y detección de fraude, implementado bajo **arquitectura medallón** con modelo dimensional **esquema estrella**.

---

## Stack Tecnológico

| Componente | Tecnología | Justificación |
|-----------|------------|---------------|
| Lenguaje | Python 3.9+ | Ecosistema maduro, compatible con PySpark y Fabric |
| Procesamiento | Pandas | Desarrollo local rápido, mismo API que PySpark |
| Almacenamiento | Parquet | Columnar, comprimido, tipado, sin esquema externo |
| Orquestación | Script Python | Simplicidad; en producción → Fabric Pipelines |
| Testing | Pytest | Estándar de la industria para Python |

---

## Arquitectura del Pipeline

```
pipeline.py (orquestador)
    │
    ├── ETAPA 1: INGESTA → BRONZE
    │   └── src/ingestion/ingestor.py
    │       ├── Lee CSV de data/bronze/
    │       ├── Valida esquema + IDs
    │       ├── Genera hash SHA-256 y row hash MD5
    │       ├── Separa errores → *_errores.parquet
    │       └── Escribe *_bronze.parquet + audit log
    │
    ├── ETAPA 2: TRANSFORMACIÓN → SILVER + GOLD
    │   └── src/transformation/transformer.py
    │       ├── Normaliza fechas (ISO-8601)
    │       ├── Normaliza texto (lower/title)
    │       ├── Deduplica por ciudadano+campo+día
    │       ├── Genera campos derivados
    │       ├── Escribe *_silver.parquet (capa Silver)
    │       └── Construye modelo dimensional (capa Gold)
    │           ├── dim_ciudadano.parquet
    │           ├── fact_cea_clases.parquet
    │           ├── fact_crc_examenes.parquet
    │           └── dim_runt.parquet
    │
    ├── ETAPA 3: CALIDAD + CUMPLIMIENTO + FRAUDE
    │   └── src/quality/quality_checks.py
    │       ├── Reporte de calidad por DataFrame
    │       ├── Tabla de cumplimiento (CRC+CEA vs RUNT)
    │       ├── Detección de fraude (F2–F5)
    │       └── Cálculo de KPIs
    │
    └── ETAPA 4: EXPOSICIÓN → Dataset Final
        └── src/exposure/exporter.py
            ├── Exporta CSV legible (Sí/No)
            ├── Exporta alertas a CSV
            └── Genera resumen ejecutivo JSON
```

---

## Decisiones de Diseño Clave

### 1. Arquitectura Medallón (Bronze/Silver/Gold) en lugar de ETL monolítico
- Permite reprocesar cualquier capa sin perder datos originales.
- Bronze preserva los datos crudos para auditoría regulatoria.
- Silver limpia y normaliza.
- Gold presenta el modelo listo para consumo.

### 2. Esquema Estrella en lugar de Copo de Nieve o 3FN
- Solo 2 dimensiones simples → no hay jerarquías que normalizar.
- Power BI opera nativamente con star schema.
- Consultas analíticas con 1 solo nivel de JOIN.

### 3. Surrogate Keys (sk_ciudadano)
- Desacopla el modelo analítico del ID natural.
- Permite evolución del esquema sin romper relaciones.

### 4. SCD Tipo 1 para RUNT
- Solo interesa el estado más reciente de la licencia.
- Se sobrescribe en cada ejecución.

### 5. Parquet como formato de almacenamiento
- Columnar: lecturas analíticas eficientes.
- Comprimido: menor espacio en disco.
- Tipado: preserva tipos sin esquema externo.
- Migración directa a Delta Lake en Fabric.

---

## Migración a Producción (Microsoft Fabric)

| Componente Local | Equivalente en Fabric |
|------------------|-----------------------|
| CSV en `data/bronze/` | OneLake Files / Copy Data |
| `ingestor.py` (Pandas) | Data Pipeline + Spark Notebook |
| `transformer.py` (Pandas) | Spark Notebook (PySpark) |
| `quality_checks.py` | Spark Notebook + Data Activator |
| Parquet en `data/gold/` | Delta Tables en Lakehouse |
| `exporter.py` → CSV | Semantic Model → DirectLake |
| `kpis.json` | Power BI Dashboard |
| `alertas_fraude` | Data Activator → Teams/Email |

### Ejemplo de migración Pandas → PySpark

```python
# Local (Pandas)
df = pd.read_csv("data/bronze/cea_clases.csv")
df["clase_norm"] = df["clase"].str.strip().str.lower()

# Fabric (PySpark)
df = spark.read.csv("abfss://.../bronze/cea_clases.csv", header=True)
df = df.withColumn("clase_norm", lower(trim(col("clase"))))
```

---

## Propuesta de Dashboard de Cumplimiento

La `tabla_cumplimiento` (Gold) alimenta directamente un dashboard Power BI:

| Visualización | Dato | Tabla Fuente |
|---------------|------|-------------|
| KPI Card | % proceso completo | tabla_cumplimiento |
| KPI Card | % inconsistencias RUNT | tabla_cumplimiento |
| Tabla condicional | Ciudadanos por nivel de riesgo | tabla_cumplimiento |
| Gráfico de barras | Distribución de riesgo | tabla_cumplimiento |
| Panel de alertas | Fraudes detectados por severidad | alertas_fraude |
| Timeline | Alertas por fecha | alertas_fraude |

---

## Propuesta de Sistema de Alertas Automáticas

```
Evento detectado (quality_checks.py)
        │
        ▼
  alertas_fraude.parquet (Gold)
        │
        ▼
  Data Activator (Fabric)
        │
        ├── CRITICA → Notificación inmediata (<5 min) → Teams + Email
        ├── ALTA    → Resumen diario → Canal Teams
        └── MEDIA   → Reporte semanal → Dashboard
```

**Reglas de disparo:**
- Severidad `CRITICA` → notificación push a supervisores en < 5 minutos.
- Severidad `ALTA` → email consolidado diario.
- Severidad `MEDIA` → incluida en reporte semanal de cumplimiento.
- Cada alerta genera un ticket automático en el sistema de gestión.
