# Presentación – Olimpia Data Pipeline
## Data Engineer Assessment

> **Nota**: Este archivo sirve como guion para las 5 diapositivas de la presentación.
> Puede importarse directamente a PowerPoint / Google Slides / Marp.

---

## Diapositiva 1: Contexto y Problema

### El Reto
Integrar **3 fuentes de datos** del sector transporte colombiano para verificar cumplimiento normativo:

| Fuente | Contenido | Registros |
|--------|-----------|-----------|
| **CEA** | Clases de formación vial | 500 |
| **CRC** | Exámenes médicos/psicológicos | 500 |
| **RUNT** | Estado de licencia de conducir | 500 |

### Preguntas Clave
- ¿Qué % de ciudadanos completó satisfactoriamente CRC y CEA?
- ¿Cuántos presentan inconsistencias entre CRC/CEA y RUNT?
- ¿Cómo detectar fraude de forma temprana?

---

## Diapositiva 2: Arquitectura Medallón

### Pipeline End-to-End en 4 Etapas

```
CSV Fuentes → 🥉 BRONZE → 🥈 SILVER → 🥇 GOLD → 📊 EXPOSICIÓN
               (Ingesta)   (Limpieza)  (Modelo)   (CSV/Dashboard)
```

| Capa | Qué hace | Tecnología |
|------|----------|------------|
| **Bronze** | Validar esquema, filtrar IDs inválidos, auditar | Parquet + SHA-256 |
| **Silver** | Normalizar fechas/texto, deduplicar, enriquecer | Pandas + Parquet |
| **Gold** | Modelo estrella: dim + fact tables + cumplimiento | Parquet (→ Delta Lake) |
| **Exposición** | Dataset CSV final + resumen ejecutivo | CSV + JSON |

### ¿Por qué Medallón?
- Trazabilidad completa desde dato crudo hasta exposición.
- Reprocesable: cualquier capa se regenera sin perder datos originales.
- Compatible con Microsoft Fabric (migración directa).

---

## Diapositiva 3: Modelo de Datos (Estrella)

### Esquema Estrella – 4 Dimensiones + 2 Hechos

```
  DIM_FECHA ──┬── FACT_CEA_CLASES ──── DIM_CIUDADANO ──── FACT_CRC_EXAMENES ──┬── DIM_FECHA
              │         │                    │                    │            │
  DIM_INSTRUCTOR ───────┘      DIM_RUNT ─────┼────── TABLA_CUMPLIMIENTO       │
                                             │                                │
                                      ALERTAS_FRAUDE                   DIM_FECHA
```

**¿Por qué Estrella y no Copo de Nieve?**
- 4 dimensiones conformadas (ciudadano, fecha, instructor, RUNT).
- `DIM_FECHA` permite análisis temporal; `DIM_INSTRUCTOR` permite control de carga.
- JOINs de 1 solo nivel → rendimiento óptimo.
- Power BI opera nativamente con star schema.

### Reglas de Negocio Implementadas
- ✅ R1: Sin ciudadanos sin ID (filtro en ingesta)
- ✅ R2: Fechas válidas (coerce + drop NaT)
- ✅ R3: Mismo día → conservar más reciente
- ✅ R5: Errores en estructura separada (parquet + logs)

---

## Diapositiva 4: Hallazgos y Detección de Fraude

### KPIs del Pipeline (datos simulados)

| KPI | Valor |
|-----|-------|
| Ciudadanos totales | 548 |
| Proceso completo (CRC + CEA) | ~0.2% |
| Inconsistencias RUNT | ~40.5% (222 ciudadanos) |
| Alertas de fraude | 304 (89 críticas) |

### Patrones de Fraude Detectados

| Código | Patrón | Severidad |
|--------|--------|-----------|
| F2 | CEA completado en < 3 días | ALTA |
| F3 | Licencia activa sin exámenes CRC | **CRITICA** |
| F4 | Resultados contradictorios mismo día | **CRITICA** |
| F5 | Instructor con >10 clases/día | MEDIA |

### Sistema de Alertas Propuesto
- **CRITICA** → Notificación Teams/Email en < 5 minutos
- **ALTA** → Resumen diario consolidado
- **MEDIA** → Reporte semanal

---

## Diapositiva 5: Propuesta de Valor y Próximos Pasos

### Propuesta de Valor para Olimpia

1. **Visibilidad**: Dashboard de cumplimiento para la Superintendencia de Transporte.
2. **Detección proactiva**: Sistema de alertas que identifica fraude antes de que escale.
3. **Auditoría regulatoria**: Trazabilidad completa con hashes SHA-256 por ingesta.
4. **Escalabilidad**: Migración directa a Microsoft Fabric (Delta Lake + PySpark).

### Roadmap de Producción

| Fase | Acción | Timeframe |
|------|--------|-----------|
| 1 | Delta Lake + Great Expectations | 1–3 meses |
| 2 | Fabric + Power BI + Data Activator | 3–6 meses |
| 3 | ML de fraude (Isolation Forest) + más fuentes | 6–12 meses |

### Entregables del Assessment
- ✅ Repositorio Git con código completo
- ✅ Carpeta `/docs` con 5 documentos técnicos
- ✅ Dataset final CSV con resultados
- ✅ README con instrucciones end-to-end
- ✅ Presentación con hallazgos y propuesta de valor
- ✅ **Dashboard Power BI** (`dashboard/Superintendencia de Transporte.pbix`) con 8 tablas, 8 relaciones, 20 medidas DAX
