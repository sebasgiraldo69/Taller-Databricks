### Celda 1

# Ejercicio 2 - Transformacion y calidad de datos
# Leer la tabla Bronze desde Unity Catalog

tabla_bronze = "workspace.ingenieria_datos_sebastian.ventas_bronze"

df_bronze = spark.table(tabla_bronze)

# Mostrar los datos originales
display(df_bronze)

# Contar registros antes de la limpieza
print("Registros antes de la limpieza:", df_bronze.count())


### Celda 2

from pyspark.sql.functions import col, trim

# 1. Eliminar registros completamente duplicados
df_sin_duplicados = df_bronze.dropDuplicates()

# 2. Aplicar las reglas de calidad
df_limpio = df_sin_duplicados.filter(
    col("producto").isNotNull() &
    (trim(col("producto")) != "") &
    (col("cantidad") > 0) &
    (col("precio_unitario") > 0)
)

# 3. Comparar registros antes y despues
print("Registros originales:", df_bronze.count())
print("Registros sin duplicados:", df_sin_duplicados.count())
print("Registros despues de la limpieza:", df_limpio.count())

# 4. Mostrar los datos limpios
display(df_limpio.orderBy("id_venta"))


### Celda 3

from pyspark.sql.functions import col

# Calcular el valor total de cada venta
df_silver = df_limpio.withColumn(
    "total_venta",
    col("cantidad") * col("precio_unitario")
)

# Mostrar las ventas con la nueva columna
display(df_silver.orderBy("id_venta"))

# Verificar cantidad de registros
print("Registros transformados:", df_silver.count())

# Calcular el valor total de todas las ventas
df_silver.selectExpr(
    "SUM(total_venta) AS total_general"
).show()


### Celda 4

# Nombre completo de la tabla Silver
tabla_silver = "workspace.ingenieria_datos_sebastian.ventas_silver"

# Guardar los datos limpios como tabla Delta
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabla_silver)

# Verificar los registros almacenados
print("Tabla ventas_silver creada correctamente.")
print("Registros almacenados:", spark.table(tabla_silver).count())

# Verificar el total de ventas
spark.table(tabla_silver).selectExpr(
    "SUM(total_venta) AS total_general"
).show()


### Celda 5

from pyspark.sql.functions import sum as spark_sum

# Agrupar las ventas por categoria
df_por_categoria = df_silver.groupBy("categoria").agg(
    spark_sum("total_venta").alias("total_vendido")
)

# Ordenar el resumen por valor vendido
df_por_categoria = df_por_categoria.orderBy(
    col("total_vendido").desc()
)

# Guardar el resumen como tabla Delta
tabla_resumen = "workspace.ingenieria_datos_sebastian.ventas_por_categoria"

df_por_categoria.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabla_resumen)

# Mostrar los resultados almacenados
display(spark.table(tabla_resumen).orderBy(col("total_vendido").desc()))

print("Categorias registradas:", spark.table(tabla_resumen).count())


### Celda 6

from pyspark.sql.functions import sum as spark_sum

# Consultar las tablas despues de repetir el proceso
df_verificar_silver = spark.table(
    "workspace.ingenieria_datos_sebastian.ventas_silver"
)

df_verificar_resumen = spark.table(
    "workspace.ingenieria_datos_sebastian.ventas_por_categoria"
)

print("Registros en ventas_silver:", df_verificar_silver.count())
print("Categorias en ventas_por_categoria:", df_verificar_resumen.count())

# Comprobar duplicados
duplicados = df_verificar_silver.count() - df_verificar_silver.dropDuplicates().count()
print("Registros duplicados en Silver:", duplicados)

# Comprobar total de ventas
df_verificar_resumen.agg(
    spark_sum("total_vendido").alias("total_general")
).show()