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
1. Solo hay 2 dimensiones (ciudadano, RUNT) → no hay jerarquías profundas que justifiquen snowflake.
2. Power BI trabaja nativamente con star schema (DirectLake).
3. Las consultas analíticas son más rápidas con menos JOINs.
4. La `tabla_cumplimiento` actúa como fact table agregada (1 fila/ciudadano) para dashboards.

---

## Tablas del Modelo (Capa Gold)

### DIM_CIUDADANO (Dimensión Central)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | INT (PK) | Surrogate key autoincremental |
| `ID_ciudadano` | INT (UK) | ID natural del ciudadano |

- **Grain**: 1 fila por ciudadano único.
- **Fuente**: Unión de IDs vistos en CEA, CRC y RUNT.
- **Surrogate key**: Desacopla el modelo analítico del identificador natural.

### FACT_CEA_CLASES (Tabla de Hechos)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | INT (FK) | → dim_ciudadano |
| `ID_ciudadano` | INT | ID natural |
| `clase_norm` | STRING | Tipo de clase (teorica/practica) |
| `horas` | INT | Horas de la clase |
| `instructor_norm` | STRING | Nombre del instructor (Title Case) |
| `fecha_date` | DATE | Fecha de la clase |
| `es_practica` | BOOLEAN | Indicador de clase práctica |
| `horas_acum_ciudadano` | INT | Horas acumuladas por ciudadano |

- **Grain**: 1 fila por ciudadano + clase + fecha.
- **Regla de deduplicación**: Si hay múltiples registros en el mismo día para la misma clase, se conserva el más reciente (`_ingested_at`).

### FACT_CRC_EXAMENES (Tabla de Hechos)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | INT (FK) | → dim_ciudadano |
| `ID_ciudadano` | INT | ID natural |
| `tipo_examen_norm` | STRING | medico / psicologico / coordinacion |
| `resultado_aprobado` | BOOLEAN | Aprobó el examen |
| `fecha_date` | DATE | Fecha del examen |
| `examenes_aprobados_acum` | INT | Acumulado de exámenes aprobados |

- **Grain**: 1 fila por ciudadano + tipo de examen + fecha.

### DIM_RUNT (Dimensión – SCD Tipo 1)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `sk_ciudadano` | INT (FK) | → dim_ciudadano |
| `ID_ciudadano` | INT | ID natural |
| `estado_licencia_norm` | STRING | activa / suspendida / cancelada |
| `licencia_activa` | BOOLEAN | Licencia vigente |
| `fecha_actualizacion_date` | DATE | Última actualización |
| `dias_desde_actualizacion` | INT | Días desde última actualización |

- **Grain**: 1 fila por ciudadano (registro más reciente).
- **SCD Tipo 1**: Se sobrescribe con el estado más reciente (solo interesa la foto actual).

### TABLA_CUMPLIMIENTO (Fact Table Agregada)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `ID_ciudadano` | INT | ID natural |
| `crc_completo` | BOOLEAN | 3 tipos de examen aprobados |
| `cea_completo` | BOOLEAN | Al menos 1 teórica + 1 práctica |
| `proceso_completo` | BOOLEAN | CRC + CEA ambos completos |
| `inconsistencia_runt` | BOOLEAN | Desajuste entre proceso y RUNT |
| `nivel_riesgo` | STRING | BAJO / MEDIO / ALTO / CRITICO |
| `licencia_activa` | BOOLEAN | Estado actual de licencia |

- **Grain**: 1 fila por ciudadano.
- **Propósito**: Responder directamente las preguntas de negocio del dashboard.

### ALERTAS_FRAUDE

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `tipo_alerta` | STRING | Código F1–F5 |
| `ID_ciudadano` | INT | Ciudadano afectado (puede ser NULL) |
| `detalle` | STRING | Descripción de la anomalía |
| `severidad` | STRING | CRITICA / ALTA / MEDIA |
| `detectado_en` | DATETIME | Timestamp de detección |

---

## Relaciones y Cardinalidades

```
DIM_CIUDADANO (1) ──── (N) FACT_CEA_CLASES
DIM_CIUDADANO (1) ──── (N) FACT_CRC_EXAMENES
DIM_CIUDADANO (1) ──── (1) DIM_RUNT
DIM_CIUDADANO (1) ──── (1) TABLA_CUMPLIMIENTO
DIM_CIUDADANO (1) ──── (N) ALERTAS_FRAUDE
```

---

## Diagrama Estrella

```
                    ┌──────────────────────┐
                    │   FACT_CEA_CLASES    │
                    │  (N clases/ciudadano)│
                    └──────────┬───────────┘
                               │
┌──────────────────┐    ┌──────┴──────┐    ┌──────────────────────┐
│    DIM_RUNT      │────│ DIM_CIUDADANO│────│  FACT_CRC_EXAMENES  │
│ (1:1 por ciud.)  │    │   ⭐ Centro  │    │ (N exámenes/ciud.)  │
└──────────────────┘    └──────┬──────┘    └──────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                                 │
   ┌──────────┴───────────┐          ┌──────────┴───────────┐
   │ TABLA_CUMPLIMIENTO   │          │  ALERTAS_FRAUDE      │
   │ (1:1 por ciudadano)  │          │ (N alertas/ciud.)    │
   └──────────────────────┘          └──────────────────────┘
```
