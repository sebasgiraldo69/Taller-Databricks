# Leer la tabla Delta desde Unity Catalog
df_consulta = spark.table(
    "workspace.ingenieria_datos_sebastian.ventas_bronze"
)

# Consultar el total de registros
print("Total de registros:", df_consulta.count())

# Mostrar las ventas de la categoria Tecnologia
display(
    df_consulta.filter(df_consulta.categoria == "Tecnologia")
)
