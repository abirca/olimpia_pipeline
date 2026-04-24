# Análisis de Datos – Olimpia Data Pipeline
## Superintendencia de Transporte – Documento para Presentación

> **Fecha de generación**: 24 de abril de 2026  
> **Universo**: 548 ciudadanos únicos | 1,500 registros fuente | 304 alertas de fraude

---

## 1. Resumen Ejecutivo

| KPI | Valor |
|-----|-------|
| Total ciudadanos | **548** |
| Proceso completo (CRC + CEA) | **1** (0.2%) |
| CRC completo | 3 (0.5%) |
| CEA completo | 69 (12.6%) |
| Inconsistencias RUNT | **222** (40.5%) |
| Alertas de fraude | **304** (89 críticas) |
| Nivel CRITICO | 221 ciudadanos |
| Nivel ALTO | 1 ciudadano |
| Nivel MEDIO | 326 ciudadanos |
| Nivel BAJO | 0 ciudadanos |

---

## 2. Análisis por Capas del Pipeline

### 2.1. BRONZE – Ingesta y Validación

Los 3 archivos CSV fuente fueron ingestados con validación de esquema, hash SHA-256 de integridad y detección de IDs inválidos.

| Fuente | Filas RAW | IDs inválidos | Duplicados exactos | Errores rechazados |
|--------|-----------|---------------|--------------------|--------------------|
| CEA (clases) | 500 | 0 | 0 | 0 |
| CRC (exámenes) | 500 | 0 | 0 | 0 |
| RUNT (licencias) | 500 | 0 | 0 | 0 |
| **Total** | **1,500** | **0** | **0** | **0** |

**Validaciones aplicadas en Bronze:**
- Columnas requeridas presentes (esquema validado)
- `ID_ciudadano` numérico y no nulo (regex `^\d+$`)
- Duplicados exactos (misma fila completa) eliminados
- Hash SHA-256 del archivo para auditoría de integridad
- Metadatos añadidos: `_source`, `_ingested_at`, `_file_hash`, `_row_hash`

### 2.2. SILVER – Transformación y Limpieza

#### Fechas inválidas (NaT)

| Fuente | Columna de fecha | Fechas no parseables | Acción |
|--------|-----------------|---------------------|--------|
| CEA | `fecha` | **0** | `pd.to_datetime(errors='coerce')` → elimina filas NaT |
| CRC | `fecha` | **0** | Mismo tratamiento |
| RUNT | `fecha_actualizacion` | **0** | Mismo tratamiento |

> **Nota**: Los datos simulados no presentaron fechas corruptas. En producción, cualquier fecha no parseable se convierte a `NaT` y la fila se **elimina** del Silver.

#### Nulos encontrados

Los datos fuente no presentaron nulos en columnas críticas de Bronze. Los nulos en el modelo Gold se generan por **ausencia de registros** cuando un ciudadano no existe en alguna fuente:

| Campo con NULL en Gold | Ciudadanos afectados | Razón |
|---|---|---|
| Campos CRC (total_examenes, etc.) | **215** | Ciudadanos sin exámenes CRC registrados |
| Campos CEA (clases, horas) | **206** | Ciudadanos sin clases CEA registradas |
| Campos RUNT (licencia, estado) | **214** | Ciudadanos sin registro en RUNT |

#### Deduplicación (Regla R3)

| Fuente | Bronze | Silver | Eliminadas | Ciudadanos con múltiples registros |
|--------|--------|--------|------------|-----------------------------------|
| CEA | 500 | **500** | 0 | 0 |
| CRC | 500 | **500** | 0 | 0 |
| RUNT | 500 | **334** | **166** | **125** (máx. 5 registros por ciudadano) |

**Reglas de deduplicación:**
- **CEA**: mismo `ID_ciudadano` + `clase` + `fecha` → conservar más reciente por `_ingested_at`
- **CRC**: mismo `ID_ciudadano` + `tipo_examen` + `fecha` → conservar más reciente por `_ingested_at`
- **RUNT**: mismo `ID_ciudadano` → conservar **solo el registro más reciente** por `fecha_actualizacion` (SCD Tipo 1)

> **RUNT fue la única fuente con deduplicación real**: 125 ciudadanos tenían entre 2 y 5 registros. Se conservó únicamente el más reciente.

#### Normalización de texto

| Campo original | Campo normalizado | Transformación | Ejemplo |
|---|---|---|---|
| `clase` | `clase_norm` | `.str.lower()` | "Practica" → "practica" |
| `tipo_examen` | `tipo_examen_norm` | `.str.lower()` | "Medico" → "medico" |
| `instructor` | `instructor_norm` | `.str.title()` | "garcia lopez" → "Garcia Lopez" |
| `estado_licencia` | `estado_licencia_norm` | `.str.lower()` | "Activa" → "activa" |

#### Campos derivados creados

| Fuente | Campo derivado | Tipo | Descripción |
|---|---|---|---|
| CEA | `es_practica` | bool | `True` si clase es práctica |
| CEA | `horas_acum_ciudadano` | int | Horas acumuladas progresivas por ciudadano |
| CRC | `resultado_aprobado` | bool | `True` si resultado = "aprobado" |
| CRC | `examenes_aprobados_acum` | int | Conteo acumulado de exámenes aprobados |
| RUNT | `licencia_activa` | bool | `True` si estado = "activa" |
| RUNT | `dias_desde_actualizacion` | int | Días desde la última actualización hasta hoy |

#### Nota sobre fecha de nacimiento

**Los datos NO contienen columna de fecha de nacimiento.** Las fechas procesadas son:
- `fecha` (CEA) = fecha de la clase de formación vial
- `fecha` (CRC) = fecha del examen médico/psicológico
- `fecha_actualizacion` (RUNT) = última actualización del registro de licencia

La regla R2 ("fechas razonables") se aplicó a estas fechas de proceso con `pd.to_datetime(errors='coerce')`, descartando cualquier valor no parseable.

### 2.3. GOLD – Modelo Dimensional

| Tabla | Filas | Columnas | Nulos significativos |
|---|---|---|---|
| dim_ciudadano | 548 | 4 | Ninguno |
| dim_fecha | 433 | 8 | Ninguno |
| dim_instructor | 12 | 2 | Ninguno |
| dim_runt | 334 | 6 | Ninguno |
| fact_cea_clases | 500 | 9 | Ninguno |
| fact_crc_examenes | 500 | 7 | Ninguno |
| tabla_cumplimiento | 548 | 21 | Sí (por ciudadanos sin fuente correspondiente) |
| alertas_fraude | 304 | 6 | Ninguno |

---

## 3. Proceso Completo – Análisis Detallado

### Definición

Un ciudadano tiene **proceso completo** cuando cumple **ambas** condiciones:
- **CRC completo** = aprobó los 3 tipos de examen (médico, psicológico, coordinación)
- **CEA completo** = cursó al menos 1 clase teórica Y 1 clase práctica

### Resultados

| Indicador | Cantidad | Porcentaje |
|-----------|----------|------------|
| CRC completo | 3 de 548 | **0.5%** |
| CEA completo | 69 de 548 | **12.6%** |
| **Proceso completo (ambos)** | **1 de 548** | **0.2%** |

### ¿Por qué tan pocos? – Cuellos de botella

#### Cuello de botella CRC (el más crítico)

| Tipos de examen realizados | Ciudadanos | % de los que tienen CRC |
|---|---|---|
| Solo 1 tipo (de 3 requeridos) | 249 | 74.9% |
| 2 tipos | 77 | 23.1% |
| 3 tipos (completo) | 7 | 2.1% |

De los 7 que realizaron los 3 tipos, solo **3 los aprobaron todos** → CRC completo.

> **Hallazgo**: La mayoría de ciudadanos solo ha presentado 1 de los 3 exámenes requeridos. El cuello de botella principal está en el CRC.

#### Cuello de botella CEA

| Situación | Ciudadanos |
|---|---|
| Solo clases prácticas (sin teórica) | 146 |
| Solo clases teóricas (sin práctica) | 127 |
| **Ambas (teórica + práctica)** | **69** → estos son los CEA completo |

### El único ciudadano con proceso completo

| Atributo | Valor |
|----------|-------|
| ID_ciudadano | **416** |
| CRC | ✅ 3 exámenes aprobados |
| CEA | ✅ Teórica + práctica |
| Licencia RUNT | ❌ **Sin información** (None) |
| Inconsistencia | Sí — completó todo pero no tiene licencia |
| Nivel riesgo | **ALTO** |

> **Ironía**: El único ciudadano que completó todo el proceso **no tiene licencia registrada en el RUNT**.

---

## 4. Inconsistencia RUNT – Análisis Detallado

### Definición

Existe inconsistencia cuando el estado de la licencia RUNT **no corresponde** con el proceso del ciudadano:
- Tiene licencia activa **PERO** proceso incompleto → **sospechoso (CRITICO)**
- Tiene proceso completo **PERO** no tiene licencia → **anomalía administrativa (ALTO)**

### Resultados

| Escenario | Ciudadanos | % | Nivel riesgo |
|-----------|-----------|---|-------------|
| Proceso incompleto + **licencia activa** | **221** | 40.3% | **CRITICO** |
| Proceso completo + sin licencia | **1** | 0.2% | **ALTO** |
| Proceso incompleto + sin licencia activa | 326 | 59.5% | **MEDIO** |

### Distribución de ciudadanos con datos RUNT

```
548 ciudadanos totales
├── 334 tienen registro RUNT (61%)
│   ├── 221 licencia ACTIVA → ¡pero solo 1 completó proceso! → 220 CRÍTICOS
│   ├── 73 licencia SUSPENDIDA
│   └── 40 licencia CANCELADA
└── 214 SIN registro RUNT (39%) → sin información de licencia
```

### Detalle de los 221 CRÍTICOS (licencia activa sin proceso)

| Situación | Cantidad |
|---|---|
| No completaron NI CRC NI CEA | **190** |
| Solo completaron CEA | 30 |
| Solo completaron CRC | 1 |
| **Total con licencia activa sin proceso** | **221** |

> **Hallazgo principal**: 221 ciudadanos (40%) tienen licencia activa sin haber completado el proceso. De estos, 190 no completaron **ninguna** de las dos certificaciones. Esto sugiere **emisión irregular de licencias** o datos desactualizados en el RUNT.

---

## 5. Detección de Fraude – Análisis Completo

### 5.1. Sistema implementado

El módulo `quality_checks.py` implementa 5 patrones de detección de fraude ejecutados sobre los datos de la capa Gold:

| Código | Patrón | Severidad | Lógica |
|--------|--------|-----------|--------|
| **F1** | Colusión instructor-CRC | MEDIA | Instructor con >50% aprobación en misma fecha |
| **F2** | CEA completado muy rápido | **ALTA** | Proceso CEA en < 3 días |
| **F3** | Licencia sin CRC | **CRITICA** | Licencia RUNT activa sin ningún examen CRC |
| **F4** | Resultados contradictorios | **CRITICA** | Mismo examen, mismo día, resultados diferentes |
| **F5** | Instructor sobrecargado | MEDIA | Instructor con >10 clases en un solo día |

### 5.2. Resultados de la detección

| Alerta | Ciudadanos afectados | Severidad | Descripción |
|--------|---------------------|-----------|-------------|
| **F2_CEA_MUY_RAPIDO** | **215** | ALTA | CEA completado en 0 días |
| **F3_LICENCIA_SIN_CRC** | **89** | CRITICA | Licencia activa sin exámenes CRC |
| F1 (colusión) | 0 | — | No detectado en datos actuales |
| F4 (contradicciones) | 0 | — | No detectado en datos actuales |
| F5 (instructor) | 0 | — | No detectado en datos actuales |
| **Total alertas** | **304** | — | 273 ciudadanos únicos afectados |

### 5.3. F2 – CEA completado demasiado rápido (215 alertas)

**Regla**: Se marca como sospechoso a todo ciudadano cuyo proceso CEA (primera a última clase) se completó en menos de 3 días.

**Hallazgo**: Los 215 ciudadanos flaggeados completaron su CEA en **0 días** (todas las clases en la misma fecha).

| Detalle | Valor |
|---------|-------|
| Ciudadanos con clases en 1 solo día | **215** |
| Ciudadanos con clases en 2+ días | 127 |
| Días mínimo detectado | **0** (mismo día) |

**¿Cómo se detectó?**
```python
# Se agrupan clases por ciudadano y se calcula el rango de fechas
cea_span = fact_cea.groupby("ID_ciudadano")["fecha_date"].agg(["min", "max"])
cea_span["dias_proceso_cea"] = (max - min).days
rapidos = cea_span[dias_proceso_cea < 3]  # → 215 ciudadanos
```

**Interpretación**: Un ciudadano que toma clase teórica Y práctica el mismo día es sospechoso. El proceso CEA normalmente toma semanas. Tomar todo en 0 días sugiere:
- Registro fraudulento de asistencia
- Emisión de certificados sin formación real
- Error de carga de datos en la fuente

### 5.4. F3 – Licencia activa sin exámenes CRC (89 alertas)

**Regla**: Se marca como CRÍTICO a todo ciudadano que tiene licencia RUNT activa pero **cero exámenes CRC registrados**.

| Detalle | Valor |
|---------|-------|
| Ciudadanos con licencia activa | 221 |
| De esos, sin NINGÚN examen CRC | **89** (40.3% de licencias activas) |
| Severidad | **CRITICA** |

**¿Cómo se detectó?**
```python
# Ciudadanos con CRC registrado
ids_con_crc = set(fact_crc["ID_ciudadano"].unique())
# Ciudadanos con licencia activa pero sin CRC
f3 = cumplimiento[
    (cumplimiento["licencia_activa"] == True) &
    (~cumplimiento["ID_ciudadano"].isin(ids_con_crc))
]  # → 89 ciudadanos
```

**Interpretación**: Es imposible obtener una licencia de conducir sin pasar exámenes médicos. 89 ciudadanos tienen licencia activa sin **ningún** registro CRC. Esto indica:
- Posible emisión de licencias sin proceso CRC (fraude documental)
- Datos faltantes en la fuente CRC
- Migración incompleta de datos históricos

### 5.5. Cruce entre alertas

| Combinación | Ciudadanos |
|---|---|
| Solo F2 (CEA rápido) | 184 |
| Solo F3 (licencia sin CRC) | 58 |
| **F2 + F3 (ambas alertas)** | **31** ← más sospechosos |
| Total con al menos una alerta | **273** (49.8% del universo) |

> **Los 31 ciudadanos con ambas alertas** son el grupo de mayor riesgo: completaron CEA en 0 días Y tienen licencia activa sin exámenes CRC. Representan el escenario de fraude más probable.

### 5.6. Correlación alertas ↔ nivel de riesgo

| Alerta | Son CRITICO | Son ALTO | Son MEDIO |
|--------|------------|----------|-----------|
| F3 (89 ciudadanos) | **89** (100%) | 0 | 0 |
| F2 (215 ciudadanos) | 31 | 0 | 184 |

> Todas las alertas F3 corresponden a nivel CRITICO porque su definición (licencia activa + proceso incompleto) es exactamente la condición de CRITICO.

---

## 6. Flujo de Datos – Resumen Visual

```
1,500 filas RAW (3 CSV)
    │
    ▼ BRONZE: Validación de esquema + IDs + hashes
1,500 filas validadas (0 rechazos)
    │
    ▼ SILVER: Normalización + deduplicación + campos derivados
1,334 filas limpias (500 CEA + 500 CRC + 334 RUNT)
    │  ↳ 166 filas RUNT eliminadas (125 ciudadanos con múltiples registros)
    │
    ▼ GOLD: Modelo estrella
    ├── 548 ciudadanos únicos (dim_ciudadano)
    ├── 433 fechas únicas (dim_fecha)
    ├── 12 instructores (dim_instructor)
    ├── 334 registros RUNT (dim_runt)
    ├── 500 clases CEA (fact_cea_clases)
    ├── 500 exámenes CRC (fact_crc_examenes)
    ├── 548 filas cumplimiento (tabla_cumplimiento)
    └── 304 alertas fraude (alertas_fraude)
         ├── 215 ALTA (F2: CEA en 0 días)
         └── 89 CRITICA (F3: licencia sin CRC)
```

---

## 7. Datos Clave para la Presentación

### Diapositiva 1 – El problema

> De 548 ciudadanos analizados, **solo 1** completó satisfactoriamente todo el proceso (CRC + CEA). El 40% tiene una licencia activa sin haber completado los requisitos.

### Diapositiva 2 – Lo que se hizo (Silver)

| Acción | Impacto |
|--------|---------|
| Normalización de fechas | 0 fechas inválidas detectadas |
| Deduplicación RUNT | 166 registros eliminados (125 ciudadanos) |
| Normalización de texto | 4 campos estandarizados |
| 6 campos derivados | Enriquecen el análisis |

### Diapositiva 3 – Inconsistencias RUNT

> **221 ciudadanos** tienen licencia activa con proceso incompleto (40.3%). De estos, 190 no completaron NI CRC NI CEA. El único ciudadano con proceso completo NO tiene licencia.

### Diapositiva 4 – Fraude detectado

> **304 alertas** en 273 ciudadanos (49.8%). Los 215 con CEA en 0 días sugieren registro ficticio. Los 89 con licencia sin CRC sugieren emisión irregular. **31 ciudadanos** tienen ambas alertas → máxima sospecha.

### Diapositiva 5 – Propuesta de acción

| Severidad | Acción recomendada | Ciudadanos |
|-----------|-------------------|------------|
| CRITICA | Investigación inmediata + suspensión preventiva | 89 |
| ALTA | Revisión en 48 horas + auditoría CEA | 215 |
| Doble alerta | Prioridad máxima → los 31 con F2 + F3 | 31 |

---

## 8. Pipeline Técnico – Último Run

| Parámetro | Valor |
|-----------|-------|
| Versión | 1.0.0 |
| Inicio | 2026-04-24 18:38:32 |
| Fin | 2026-04-24 18:38:33 |
| Duración | **0.99 segundos** |
| Filas ingestadas | 1,500 |
| Errores en ingesta | 0 |
| Alertas generadas | 304 |

---

## 9. Calidad de Datos – Métricas por Fuente (Silver)

### CEA (Clases)

| Métrica | Valor |
|---------|-------|
| Total registros | 500 |
| Ciudadanos únicos | 342 |
| Nulos en fecha | 0 |
| % clases prácticas | 53.6% |
| Horas promedio por clase | 3.39 |

### CRC (Exámenes)

| Métrica | Valor |
|---------|-------|
| Total registros | 500 |
| Ciudadanos únicos | 333 |
| % aprobados | 72.8% |
| Tipos de examen | psicológico: 172, médico: 168, coordinación: 160 |
| Tipos inválidos | 0 |

### RUNT (Licencias)

| Métrica | Valor |
|---------|-------|
| Total registros (post-dedup) | 334 |
| Ciudadanos únicos | 334 |
| % licencias activas | 66.2% |
| Estados | activa: 221, suspendida: 73, cancelada: 40 |
