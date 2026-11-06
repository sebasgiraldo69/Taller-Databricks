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
