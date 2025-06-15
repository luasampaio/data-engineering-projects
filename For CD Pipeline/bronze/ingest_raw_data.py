# BRONZE - Ingestão de dados brutos
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .load("abfss://raw@datalake/cliente_a/")
)

df.writeStream.format("delta") \
    .option("checkpointLocation", "/chk/bronze/cliente_a/") \
    .trigger(once=True) \
    .start("abfss://bronze@datalake/cliente_a/")
