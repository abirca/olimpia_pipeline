# Modelo de Datos – Olimpia Data Pipeline

## Tipo de Modelo: Esquema Estrella (Star Schema)

### Justificación de la Elección

Se evaluaron tres alternativas de modelado:

| Modelo | Ventajas | Desventajas | Decisión |
|--------|----------|-------------|----------|
| **Estrella (Star Schema)** | JOINs simples (1 nivel), óptimo para BI, menor tiempo de consulta | Redundancia controlada en dimensiones | ✅ **Elegido** |
| Copo de Nieve (Snowflake) | Menor redundancia, más normalizado | JOINs multinivel, peor rendimiento en BI, más complejo | ❌ |
| Tercera Forma Normal (3FN) | Integridad máxima, sin redundancia | Excesivos JOINs para análisis, no óptimo para Power BI | ❌ |

**Razones principales:**
1. Hay 4 dimensiones conformadas (ciudadano, fecha, instructor, RUNT) que se conectan a 2 tablas de hechos.
2. Power BI trabaja nativamente con star schema (DirectLake).
3. Las consultas analíticas son más rápidas con menos JOINs.
4. La `tabla_cumplimiento` actúa como fact table agregada (1 fila/ciudadano) para dashboards.
5. `DIM_FECHA` permite análisis temporal (por trimestre, mes, fin de semana) sin lógica en consultas.
6. `DIM_INSTRUCTOR` permite análisis de carga y rendimiento por instructor.

---

## Tablas del Modelo (Capa Gold)

### DIM_CIUDADANO (Dimensión Central)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | BIGINT (PK) | Surrogate key autoincremental |
| `ID_ciudadano` | BIGINT (NK) | ID natural del ciudadano |
| `_gold_run_id` | STRING | Identificador de la corrida Gold |
| `_created_at` | TIMESTAMP | Fecha/hora de creación del registro |

- **Grain**: 1 fila por ciudadano único.
- **Fuente**: Unión de IDs vistos en CEA, CRC y RUNT.
- **Surrogate key**: Desacopla el modelo analítico del identificador natural.

### DIM_FECHA (Dimensión Calendario)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_fecha` | INT (PK) | Surrogate key |
| `fecha` | DATE | Fecha calendario |
| `anio` | INT | Año |
| `mes` | INT | Mes (1-12) |
| `dia` | INT | Día del mes |
| `trimestre` | INT | Trimestre (1-4) |
| `nombre_mes` | STRING | Nombre del mes (e.g. January) |
| `es_fin_semana` | BOOL | True si sábado o domingo |

- **Grain**: 1 fila por fecha única vista en todas las fuentes.
- **Propósito**: Permite filtrar/agregar por año, trimestre, mes, día de la semana sin lógica en la consulta.

### DIM_INSTRUCTOR (Dimensión Instructor)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_instructor` | BIGINT (PK) | Surrogate key |
| `instructor_norm` | STRING | Nombre del instructor (Title Case) |

- **Grain**: 1 fila por instructor único de CEA.
- **Propósito**: Permite análisis de carga de trabajo y detección de fraude (F5) por instructor.

### FACT_CEA_CLASES (Tabla de Hechos)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_fact_cea` | BIGINT (PK) | Surrogate key del hecho |
| `sk_ciudadano` | BIGINT (FK) | → DIM_CIUDADANO |
| `sk_fecha` | INT (FK) | → DIM_FECHA |
| `sk_instructor` | BIGINT (FK) | → DIM_INSTRUCTOR |
| `clase_norm` | STRING | Tipo de clase (teorica/practica) |
| `horas` | INT | Horas de la clase |
| `es_practica` | BOOL | Indicador de clase práctica |
| `horas_acum_ciudadano` | INT | Horas acumuladas por ciudadano |
| `fecha_date` | DATE | Fecha de la clase (desnormalizada para conveniencia) |

- **Grain**: 1 fila por ciudadano + clase + fecha.
- **Regla de deduplicación**: Si hay múltiples registros en el mismo día para la misma clase, se conserva el más reciente (`_ingested_at`).

### FACT_CRC_EXAMENES (Tabla de Hechos)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_fact_crc` | BIGINT (PK) | Surrogate key del hecho |
| `sk_ciudadano` | BIGINT (FK) | → DIM_CIUDADANO |
| `sk_fecha` | INT (FK) | → DIM_FECHA |
| `tipo_examen_norm` | STRING | medico / psicologico / coordinacion |
| `resultado_aprobado` | BOOL | Aprobó el examen |
| `examenes_aprobados_acum` | INT | Acumulado de exámenes aprobados |
| `fecha_date` | DATE | Fecha del examen (desnormalizada para conveniencia) |

- **Grain**: 1 fila por ciudadano + tipo de examen + fecha.

### DIM_RUNT (Dimensión – SCD Tipo 1)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_runt` | BIGINT (PK) | Surrogate key |
| `sk_ciudadano` | BIGINT (FK) | → DIM_CIUDADANO |
| `sk_fecha` | INT (FK) | → DIM_FECHA |
| `estado_licencia_norm` | STRING | activa / suspendida / cancelada |
| `licencia_activa` | BOOL | Licencia vigente |
| `dias_desde_actualizacion` | INT | Días desde última actualización |

- **Grain**: 1 fila por ciudadano (registro más reciente).
- **SCD Tipo 1**: Se sobrescribe con el estado más reciente (solo interesa la foto actual).

### TABLA_CUMPLIMIENTO (Fact Table Agregada)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | BIGINT (PK/FK) | → DIM_CIUDADANO |
| `crc_completo` | BOOL | 3 tipos de examen aprobados |
| `cea_completo` | BOOL | Al menos 1 teórica + 1 práctica |
| `proceso_completo` | BOOL | CRC + CEA ambos completos |
| `inconsistencia_runt` | BOOL | Desajuste entre proceso y RUNT |
| `nivel_riesgo` | STRING | BAJO / MEDIO / ALTO / CRITICO |

- **Grain**: 1 fila por ciudadano.
- **Propósito**: Responder directamente las preguntas de negocio del dashboard.

### ALERTAS_FRAUDE

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_alerta` | BIGINT (PK) | Surrogate key |
| `ID_ciudadano` | BIGINT (FK) | Ciudadano afectado (puede ser NULL) |
| `tipo_alerta` | STRING | Código F1–F5 |
| `detalle` | STRING | Descripción de la anomalía |
| `severidad` | STRING | CRITICA / ALTA / MEDIA |
| `detectado_en` | TIMESTAMP | Timestamp de detección |

---

## Relaciones y Cardinalidades

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

## Diagrama Estrella

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
