# Consultar resumen actualizado del pipeline
tabla_resumen = (
    "workspace.ingenieria_datos_sebastian."
    "pipeline_ventas_por_categoria"
)

df_resumen = spark.table(tabla_resumen)

display(df_resumen)

# Calcular el total general
from pyspark.sql import functions as F

df_resumen.agg(
    F.sum("total_vendido").alias("total_general")
).show()


### Celda 2


from pyspark.sql import functions as F

# Resultados antes de agregar la nueva venta
valores_antes = {
    "Tecnologia": 9330000,
    "Ropa": 1625000,
    "Alimentos": 288600
}

# Consultar los resultados actuales del pipeline
df_actual = spark.table(
    "workspace.ingenieria_datos_sebastian.pipeline_ventas_por_categoria"
)

# Construir la comparación
df_comparacion = (
    df_actual
    .withColumn(
        "antes",
        F.when(F.col("categoria") == "Tecnologia", 9330000)
         .when(F.col("categoria") == "Ropa", 1625000)
         .when(F.col("categoria") == "Alimentos", 288600)
    )
    .withColumnRenamed("total_vendido", "despues")
    .withColumn("diferencia", F.col("despues") - F.col("antes"))
    .select("categoria", "antes", "despues", "diferencia")
)

display(df_comparacion)
