from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, DateType, DecimalType
)
from datetime import date
from decimal import Decimal

# Definir la estructura de los datos
schema = StructType([
    StructField("id_venta", IntegerType(), False),
    StructField("fecha", DateType(), True),
    StructField("producto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", DecimalType(10, 2), True)
])

# Crear registros de ventas
datos = [
    (1, date(2026, 10, 1), "Laptop", "Tecnologia", 2, Decimal("2500000")),
    (2, date(2026, 10, 2), "Mouse", "Tecnologia", 5, Decimal("45000")),
    (3, date(2026, 10, 3), "Teclado", "Tecnologia", 3, Decimal("85000")),
    (4, date(2026, 10, 4), "Camisa", "Ropa", 4, Decimal("75000")),
    (5, date(2026, 10, 5), "Pantalon", "Ropa", 2, Decimal("120000")),
    (6, date(2026, 10, 6), "Zapatos", "Ropa", 3, Decimal("180000")),
    (7, date(2026, 10, 7), "Arroz", "Alimentos", 10, Decimal("4500")),
    (8, date(2026, 10, 8), "Leche", "Alimentos", 8, Decimal("5200")),
    (9, date(2026, 10, 9), "Cafe", "Alimentos", 6, Decimal("18000")),
    (10, date(2026, 10, 10), "Celular", "Tecnologia", 2, Decimal("1500000")),
    (11, date(2026, 10, 11), "Chaqueta", "Ropa", 2, Decimal("220000")),
    (12, date(2026, 10, 12), "Pan", "Alimentos", 12, Decimal("2500")),
    (13, date(2026, 10, 13), "Monitor", "Tecnologia", 1, Decimal("850000")),
    (14, date(2026, 10, 14), "Gorra", "Ropa", 3, Decimal("35000")),
    (15, date(2026, 10, 15), "Huevos", "Alimentos", 4, Decimal("16000")),

    # Errores intencionales
    (1, date(2026, 10, 1), "Laptop", "Tecnologia", 2, Decimal("2500000")),  # Duplicado
    (16, date(2026, 10, 16), None, "Tecnologia", 2, Decimal("95000")),       # Sin producto
    (17, date(2026, 10, 17), "Audifonos", "Tecnologia", -3, Decimal("80000")) # Cantidad negativa
]

# Crear DataFrame
df_ventas = spark.createDataFrame(datos, schema=schema)

# Mostrar registros
display(df_ventas)

# Contar registros
print("Total de registros:", df_ventas.count())


"----------------------------------------------------------------------------------"

# Nombre completo de la tabla en Unity Catalog
tabla_bronze = "workspace.ingenieria_datos_sebastian.ventas_bronze"

# Guardar el DataFrame como tabla Delta
df_ventas.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabla_bronze)

print("Tabla ventas_bronze creada correctamente.")
print("Total de registros guardados:", spark.table(tabla_bronze).count())