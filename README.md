# Taller Databricks – Ingeniería de Datos

**Universidad EIA | 2026-2**  
**Estudiante:** Sebastián Giraldo Franco

## Descripción del proyecto

Este repositorio contiene el desarrollo del taller práctico de Databricks, enfocado en almacenamiento, procesamiento, transformación, automatización y validación de datos mediante PySpark, Delta Lake, Unity Catalog, Lakeflow Jobs y Lakeflow Spark Declarative Pipelines.

El proyecto utiliza un conjunto de datos de ventas de una tienda para implementar procesos de ingeniería de datos y verificar su funcionamiento mediante diferentes pruebas.

## Objetivos

- Crear y consultar tablas Delta mediante Unity Catalog.
- Aplicar reglas de limpieza y transformación de datos utilizando PySpark.
- Automatizar procesos mediante Jobs con dependencias entre tareas.
- Implementar un pipeline declarativo utilizando vistas materializadas.
- Verificar la actualización de resultados al incorporar nuevos registros.
- Documentar ejecuciones exitosas, errores y resultados obtenidos.

## Estructura del repositorio

```text
Taller_Databricks/
│
├── notebooks/
│   ├── 01_cargar_ventas.py
│   ├── 01_consultar_ventas.py
│   ├── 02_transformar_ventas.py
│   ├── 04_agregar_venta.py
│   └── 04_verificar_pipeline.py
│
├── pipelines/
│   └── my_transformation.py
│
├── Actividad Databricks.pdf
├── Taller Databricks Entrega Final.pdf
└── README.md
```

## 1. Almacenamiento y consulta de datos

Se creó el esquema `ingenieria_datos_sebastian` dentro del catálogo `workspace` de Databricks.

Se construyó un DataFrame de Spark con 18 registros de ventas, almacenados posteriormente en la tabla Delta `ventas_bronze`.

Mediante un segundo notebook se realizaron consultas para verificar los registros almacenados y consultar las ventas correspondientes a la categoría Tecnología.

**Archivos:**
- `01_cargar_ventas.py`
- `01_consultar_ventas.py`

## 2. Transformación y calidad de datos

Se implementaron reglas de calidad para depurar los registros originales:

- Eliminación de registros duplicados.
- Exclusión de productos nulos o vacíos.
- Exclusión de cantidades menores o iguales a cero.
- Exclusión de precios unitarios menores o iguales a cero.
- Cálculo de la columna `total_venta`.

Después de aplicar las reglas, se obtuvieron 15 registros válidos.

Los resultados se almacenaron en las tablas Delta `ventas_silver` y `ventas_por_categoria`.

**Resultado inicial:** $11.243.600 en ventas válidas.

Se verificó la idempotencia del proceso mediante ejecuciones repetidas, comprobando que los resultados no acumularan registros duplicados.

**Archivo:** `02_transformar_ventas.py`

## 3. Automatización mediante Lakeflow Jobs

Se configuró el Job `job_ventas_sebastian` con dos tareas:

1. `cargar_ventas`: ejecuta la carga de registros en Bronze.
2. `transformar_ventas`: aplica las transformaciones y genera los resultados.

Se estableció una dependencia para ejecutar la transformación únicamente después de finalizar correctamente la carga.

También se realizó una prueba de fallo controlado mediante una excepción de Python.

Databricks registró el error y evitó la ejecución de la tarea dependiente. Posteriormente, se corrigió el fallo y se obtuvo una ejecución exitosa.

## 4. Pipeline declarativo

Se creó el pipeline ETL `pipeline_ventas_sebastian` utilizando Lakeflow Spark Declarative Pipelines y cómputo Serverless.

El pipeline contiene dos vistas materializadas:

**`pipeline_ventas_limpias`**

Lee la tabla `ventas_bronze`, aplica las reglas de calidad y calcula el valor total de cada venta.

**`pipeline_ventas_por_categoria`**

Depende de la vista anterior y agrupa los resultados para calcular el valor total vendido por categoría.

El pipeline administra automáticamente las dependencias entre los conjuntos de datos.

**Archivo:** `pipelines/my_transformation.py`

## 5. Validación de la actualización del pipeline

Para comprobar el funcionamiento del pipeline, se agregó una venta válida directamente a `ventas_bronze`, sin ejecutar nuevamente el notebook de carga inicial.

La nueva venta correspondió a:

- Producto: Tablet.
- Categoría: Tecnología.
- Cantidad: 2 unidades.
- Precio unitario: $900.000.
- Valor total: $1.800.000.

Posteriormente, se ejecutó nuevamente el pipeline declarativo y se verificó la actualización de las vistas materializadas.

### Comparación de resultados

| Categoría | Antes | Después | Diferencia |
|---|---:|---:|---:|
| Tecnología | $9.330.000 | $11.130.000 | +$1.800.000 |
| Ropa | $1.625.000 | $1.625.000 | $0 |
| Alimentos | $288.600 | $288.600 | $0 |
| **Total general** | **$11.243.600** | **$13.043.600** | **+$1.800.000** |

Los registros válidos aumentaron de 15 a 16 y el resumen conservó sus tres categorías.

El incremento de $1.800.000 coincide exactamente con el valor de la venta agregada.

**Archivos:**
- `04_agregar_venta.py`
- `04_verificar_pipeline.py`

## 6. Tecnologías utilizadas

- Databricks Free Edition
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- Lakeflow Jobs
- Lakeflow Spark Declarative Pipelines
- Python
- Git y GitHub

## 7. Evidencias y documentación

El archivo **`Taller Databricks Entrega Final.pdf`** contiene el informe académico completo, incluyendo:

- Respuestas a las preguntas de investigación.
- Descripción de los procedimientos realizados.
- Capturas de pantalla de Databricks.
- Configuración de tareas y dependencias.
- Ejecuciones exitosas y fallidas.
- Diagnóstico y corrección del error controlado.
- Grafo de dependencias del pipeline.
- Actualización de las vistas materializadas.
- Comparación de resultados antes y después de incorporar una nueva venta.

En total, se documentaron **24 evidencias** correspondientes a los cuatro ejercicios del taller.

El archivo `Actividad Databricks.pdf` contiene las instrucciones originales de la actividad.

## 8. Consideraciones de ejecución

Los scripts fueron desarrollados para ejecutarse en el entorno de Databricks.

Algunos archivos utilizan objetos propios de este entorno, como `spark` y `display`, que no están disponibles automáticamente al ejecutar Python de manera local.

Para reproducir el proyecto es necesario disponer de un entorno Databricks, configurar el catálogo y esquema correspondientes y crear las tablas y el pipeline siguiendo el orden de los ejercicios.

## Conclusión

El taller permitió implementar y validar un flujo de ingeniería de datos utilizando Databricks, desde la creación de tablas Delta hasta la automatización y actualización de resultados mediante un pipeline declarativo.

Las pruebas realizadas permitieron comprobar la calidad de los datos, las dependencias entre procesos, el manejo de errores y la consistencia de los resultados obtenidos.