from decimal import Decimal
from datetime import date
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    DateType, StringType, DecimalType
)

# Tabla Bronze
tabla_bronze = "workspace.ingenieria_datos_sebastian.ventas_bronze"

# Definir el esquema
schema = StructType([
    StructField("id_venta", IntegerType(), False),
    StructField("fecha", DateType(), True),
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DecimalType(10, 2), True)
])

# Nueva venta valida
nueva_venta = [
    (19, date(2026, 11, 5), "Tablet",
     "Tecnologia", 2, Decimal("900000.00"))
]

df_nueva_venta = spark.createDataFrame(
    nueva_venta, schema
)

# Agregar sin reemplazar los registros existentes
df_nueva_venta.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(tabla_bronze)

print("Nueva venta agregada correctamente.")
print("Total de registros en Bronze:",
      spark.table(tabla_bronze).count())

display(df_nueva_venta)