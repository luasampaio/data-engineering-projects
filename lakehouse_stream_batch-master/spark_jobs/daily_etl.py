from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum as _sum, count
import logging
import time
from dotenv import load_dotenv
import os

load_dotenv()

BATCH_DIR = os.getenv("BATCH_DIR", "/data/batch")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "demo")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
start = time.time()

logging.info("Iniciando Spark job daily_etl...")

spark = SparkSession.builder.appName("daily_etl").getOrCreate()

try:
    # Carrega lotes Parquet (últimas 24 h)
    df = spark.read.parquet(f"file://{BATCH_DIR}/*.parquet")      # host path mount
    logging.info(f"Lidos {df.count()} registros de arquivos Parquet.")

    daily = (
        df.withColumn("day", to_date("ts"))
          .groupBy("day")
          .agg(_sum("amount").alias("gmv"), count("*").alias("orders"))
    )
    logging.info(f"Resultado do agrupamento: {daily.count()} dias agregados.")

    # salva em Postgres (append ou merge)
    (daily.write
      .format("jdbc")
      .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
      .option("dbtable", "orders_daily")
      .option("user", POSTGRES_USER)
      .option("password", POSTGRES_PASSWORD)
      .mode("append")
      .save())
    logging.info("Dados salvos no Postgres com sucesso.")
except Exception as e:
    logging.error(f"Erro no job Spark: {e}")
    raise
finally:
    elapsed = time.time() - start
    logging.info(f"Job Spark finalizado em {elapsed:.1f} segundos.")
