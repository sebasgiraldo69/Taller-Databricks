from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="pipeline_ventas_limpias",
    comment="Ventas limpias y validadas con total de venta calculado"
)
def pipeline_ventas_limpias():

    df = spark.read.table(
        "workspace.ingenieria_datos_sebastian.ventas_bronze"
    )

    df_limpio = (
        df.dropDuplicates()
          .filter(F.col("producto").isNotNull())
          .filter(F.col("cantidad") > 0)
          .filter(F.col("precio_unitario") > 0)
          .withColumn(
              "total_venta",
              F.col("cantidad") * F.col("precio_unitario")
          )
    )

    return df_limpio


@dp.materialized_view(
    name="pipeline_ventas_por_categoria",
    comment="Resumen del valor total vendido por categoria"
)
def pipeline_ventas_por_categoria():

    df_limpio = spark.read.table(
        "pipeline_ventas_limpias"
    )

    df_resumen = (
        df_limpio.groupBy("categoria")
        .agg(
            F.sum("total_venta").alias("total_vendido")
        )
    )

    return df_resumen