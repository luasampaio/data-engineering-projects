# GOLD - Agregações de negócio
df_silver = spark.read.format("delta").load("abfss://silver@datalake/cliente_a/")

df_gold = df_silver.groupBy("categoria").count()

df_gold.write.format("delta").mode("overwrite").save("abfss://gold@datalake/cliente_a/")
