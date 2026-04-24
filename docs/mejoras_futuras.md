# Mejoras Futuras – Olimpia Data Pipeline

## Corto Plazo (1–3 meses)

| Mejora | Impacto | Esfuerzo |
|--------|---------|----------|
| Migrar Parquet → **Delta Lake** | ACID, time travel, schema evolution | Medio |
| Integrar **Great Expectations** | Contratos de datos formales, validación declarativa | Bajo |
| Agregar regla **F1** (colusión instructor-CRC) | Detectar instructor con >50% aprobación en misma fecha | Bajo |
| Parametrizar umbrales de fraude | Configuración externa (YAML/JSON) sin tocar código | Bajo |
| CI/CD con **GitHub Actions** | Tests automáticos en cada push | Bajo |

## Mediano Plazo (3–6 meses)

| Mejora | Impacto | Esfuerzo |
|--------|---------|----------|
| Despliegue en **Microsoft Fabric** | Escalabilidad, PySpark, Delta Tables, OneLake | Alto |
| Dashboard **Power BI** con DirectLake | Visualización en tiempo real sin ETL adicional | Medio |
| **Data Activator** para alertas | Notificaciones push Teams/Email automáticas | Medio |
| API REST con **FastAPI** | Exposición programática de tabla_cumplimiento | Medio |
| Ingesta incremental (CDC) | Procesar solo registros nuevos/modificados | Alto |

## Largo Plazo (6–12 meses)

| Mejora | Impacto | Esfuerzo |
|--------|---------|----------|
| ML de detección de fraude | Isolation Forest / Autoencoder para patrones complejos | Alto |
| Incorporar más fuentes (SIMIT, Ministerio) | Enriquecer perfil del ciudadano | Alto |
| Data Catalog / Purview | Gobierno de datos, linaje visual, clasificación PII | Alto |
| Orquestación con **Apache Airflow** / Fabric Pipelines | Scheduling, reintentos, dependencias | Medio |
| SCD Tipo 2 para RUNT | Historial completo de cambios de licencia | Medio |

---

## Detalle de Mejoras Prioritarias

### 1. Delta Lake (reemplazo de Parquet)
- **Beneficio**: Transacciones ACID, time travel (volver a cualquier versión), schema enforcement.
- **Impacto en código**: Cambiar `.to_parquet()` por `.write.format("delta")` en PySpark.

### 2. Great Expectations
- **Beneficio**: Contratos de datos declarativos, reportes HTML de validación.
- **Ejemplo**: "La columna ID_ciudadano nunca debe tener nulos" se declara en YAML.

### 3. Modelo ML de Fraude
- **Algoritmos candidatos**: Isolation Forest (detección de outliers), Autoencoder (anomalías).
- **Features**: velocidad de proceso, instructor con alta aprobación, patrones temporales.
- **Infraestructura**: MLflow en Fabric para tracking de experimentos.

### 4. API REST con FastAPI
```python
@app.get("/cumplimiento/{id_ciudadano}")
def get_cumplimiento(id_ciudadano: int):
    return tabla_cumplimiento[tabla_cumplimiento.ID_ciudadano == id_ciudadano].to_dict()
```
