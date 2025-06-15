# SILVER - Transformação dos dados
df_bronze = spark.read.format("delta").load("abfss://bronze@datalake/cliente_a/")

df_silver = df_bronze.filter("campo_obrigatorio IS NOT NULL")

df_silver.write.format("delta").mode("overwrite").save("abfss://silver@datalake/cliente_a/")
